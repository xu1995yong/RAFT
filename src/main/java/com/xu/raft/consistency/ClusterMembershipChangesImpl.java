package com.xu.raft.consistency;

import com.alipay.remoting.exception.RemotingException;
import com.xu.raft.entity.*;
import com.xu.raft.entity.Result;
import com.xu.raft.node.NodeInfo;
import com.xu.raft.rpc.RaftRpcClient;
import com.xu.raft.rpc.Request;
import com.xu.raft.rpc.Response;
import com.xu.raft.task.RaftThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * 集群配置变更接口默认实现.
 */
@Component
public class ClusterMembershipChangesImpl implements ClusterMembershipChanges {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterMembershipChangesImpl.class);

    @Autowired
    private NodeInfo nodeInfo;


    @Autowired
    private LogModule logModule;
    @Autowired
    private RaftRpcClient rpcClient;

    // 必须是同步的,一次只能添加一个节点
    @Override
    public synchronized Result addPeer(Peer newPeer) throws RemotingException, InterruptedException {
        // 已经存在
        if (nodeInfo.getPeerSet().contains(newPeer)) {
            return new Result();
        }

        nodeInfo.getPeerSet().add(newPeer);

        if (nodeInfo.getNodeStatus() == NodeInfo.NodeStatus.LEADER) {
            nodeInfo.getNextIndexs().put(newPeer, 0L);
            nodeInfo.getMatchIndexs().put(newPeer, 0L);

            for (long i = 0; i < logModule.getLastIndex(); i++) {
                LogEntry e = logModule.read(i);
                if (e != null) {
                    replication(newPeer, e);
                }
            }

            for (Peer item : nodeInfo.getPeerSet()) {
                // TODO 同步到其他节点.
                Request request = Request.builder()
                        .cmd(Request.CHANGE_CONFIG_ADD)
                        .url(newPeer.getAddr())
                        .obj(newPeer)
                        .build();

                Response response = null;
                try {
                    response = rpcClient.send(request);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
                Result result = (Result) response.getResult();
                if (result != null && result.getStatus() == Result.Status.SUCCESS.getCode()) {
                    LOGGER.info("replication config success, peer : {}, newServer : {}", newPeer, newPeer);
                } else {
                    LOGGER.warn("replication config fail, peer : {}, newServer : {}", newPeer, newPeer);
                }
            }

        }

        return new Result();
    }

    //复制到其他机器
    public Future<Boolean> replication(Peer peer, LogEntry entry) {

        return RaftThreadPool.submit(new Callable() {
            @Override
            public Boolean call() throws Exception {

                long start = System.currentTimeMillis(), end = start;

                // 20 秒重试时间
                while (end - start < 20 * 1000L) {

                    AppendEntryParam aentryParam = AppendEntryParam.newBuilder()
                            .setTerm(nodeInfo.getCurrentTerm())
                            .setServerAddr(peer.getAddr())
                            .setLeaderAddr(nodeInfo.getSelf().getAddr())
                            .setLeaderCommit(nodeInfo.getCommitIndex()).build();

                    // 以我这边为准, 这个行为通常是成为 leader 后,首次进行 RPC 才有意义.
                    Long nextIndex = nodeInfo.getNextIndexs().get(peer);
                    LinkedList<LogEntry> logEntries = new LinkedList<>();
                    if (entry.getIndex() >= nextIndex) {
                        for (long i = nextIndex; i <= entry.getIndex(); i++) {
                            LogEntry l = logModule.read(i);
                            if (l != null) {
                                logEntries.add(l);
                            }
                        }
                    } else {
                        logEntries.add(entry);
                    }
                    // 最小的那个日志.
                    LogEntry preLog = getPreLog(logEntries.getFirst());
                    aentryParam.setPreLogTerm(preLog.getTerm());
                    aentryParam.setPrevLogIndex(preLog.getIndex());

                    aentryParam.setEntries(logEntries.toArray(new LogEntry[0]));

                    Request request = Request.builder()
                            .cmd(Request.A_ENTRIES)
                            .obj(aentryParam)
                            .url(peer.getAddr())
                            .build();

                    try {
                        Response response = rpcClient.send(request);
                        if (response == null) {
                            return false;
                        }
                        AppendEntryResult result = (AppendEntryResult) response.getResult();
                        if (result != null && result.isSuccess()) {
                            LOGGER.info("append follower entry success , follower=[{}], entry=[{}]", peer, aentryParam.getEntries());
                            // update 这两个追踪值
                            nodeInfo.getNextIndexs().put(peer, entry.getIndex() + 1);
                            nodeInfo.getMatchIndexs().put(peer, entry.getIndex());
                            return true;
                        } else if (result != null) {
                            // 对方比我大
                            if (result.getTerm() > nodeInfo.getCurrentTerm()) {
                                LOGGER.warn("follower [{}] term [{}] than more self, and my term = [{}], so, I will become follower",
                                        peer, result.getTerm(), nodeInfo.getCurrentTerm());
                                nodeInfo.setCurrentTerm(result.getTerm());
                                // 认怂, 变成跟随者
                                nodeInfo.setNodeStatus(NodeInfo.NodeStatus.FOLLOWER);
                                return false;
                            } // 没我大, 却失败了,说明 index 不对.或者 term 不对.
                            else {
                                // 递减
                                if (nextIndex == 0) {
                                    nextIndex = 1L;
                                }
                                nodeInfo.getNextIndexs().put(peer, nextIndex - 1);
                                LOGGER.warn("follower {} nextIndex not match, will reduce nextIndex and retry RPC append, nextIndex : [{}]", peer.getAddr(),
                                        nextIndex);
                                // 重来, 直到成功.
                            }
                        }

                        end = System.currentTimeMillis();

                    } catch (Exception e) {
                        LOGGER.warn(e.getMessage(), e);
                        // TODO 到底要不要放队列重试?
//                        ReplicationFailModel model =  ReplicationFailModel.newBuilder()
//                            .callable(this)
//                            .logEntry(entry)
//                            .peer(peer)
//                            .offerTime(System.currentTimeMillis())
//                            .build();
//                        replicationFailQueue.offer(model);
                        return false;
                    }
                }
                // 超时了,没办法了
                return false;
            }
        });

    }

    private LogEntry getPreLog(LogEntry logEntry) {
        LogEntry entry = logModule.read(logEntry.getIndex() - 1);

        if (entry == null) {
            LOGGER.warn("get perLog is null , parameter logEntry : {}", logEntry);
            entry = LogEntry.newBuilder().index(0L).term(0).command(null).build();
        }
        return entry;
    }

    /**
     * 必须是同步的,一次只能删除一个节点
     *
     * @param oldPeer
     */
    @Override
    public synchronized Result removePeer(Peer oldPeer) {
        nodeInfo.getPeerSet().remove(oldPeer);
        nodeInfo.getNextIndexs().remove(oldPeer);
        nodeInfo.getMatchIndexs().remove(oldPeer);

        return new Result();
    }
}
