package com.xu.raft.task;

import com.alipay.remoting.exception.RemotingException;
import com.xu.raft.entity.LogEntry;
import com.xu.raft.entity.VoteParam;
import com.xu.raft.entity.VoteResult;
import com.xu.raft.node.NodeInfo;
import com.xu.raft.entity.Peer;
import com.xu.raft.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * 1. 在转变成候选人后就立即开始选举过程
 * 自增当前的任期号（currentTerm）
 * 给自己投票
 * 重置选举超时计时器
 * 发送请求投票的 RPC 给其他所有服务器
 * 2. 如果接收到大多数服务器的选票，那么就变成领导人
 * 3. 如果接收到来自新的领导人的附加日志 RPC，转变成跟随者
 * 4. 如果选举过程超时，再次发起一轮选举
 */
@Component
public class ElectionTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElectionTask.class);

    // 上一次选举时间
    private volatile long preElectionTime = 0;

    // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号
    @Autowired
    private LogModule logModule;

    @Autowired
    NodeInfo nodeInfo;

    @Autowired
    private RaftRpcClient rpcClient;

    //TODO : 如果两个NODE同时开始投票会怎么办？
    public void startElection() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("node {} will become CANDIDATE and start election leader, task term : [{}], LastEntry : [{}]",
                    nodeInfo.getSelf(), nodeInfo.getCurrentTerm(), logModule.getLast());
        }

        nodeInfo.setNodeStatus(NodeInfo.NodeStatus.CANDIDATE);

        nodeInfo.setCurrentTerm(nodeInfo.getCurrentTerm() + 1);
        // 推荐自己.
        nodeInfo.setVotedFor(nodeInfo.getSelf().getAddr());


        List<Peer> peers = nodeInfo.getPeerSet();

        ArrayList<Future<Response<VoteResult>>> futureArrayList = new ArrayList<>();

        LOGGER.info("peerList size : {}, peer list content : {}", peers.size(), peers);

        // 在线程池中发送请求，防止发送请求失败造成线程关闭
        for (Peer peer : peers) {
            Callable sendVoteTask = new Callable() {
                @Override
                public Object call() throws Exception {
                    long lastTerm = 0L;
                    LogEntry last = logModule.getLast();
                    if (last != null) {
                        lastTerm = last.getTerm();
                    }

                    VoteParam param = VoteParam.builder().
                            term(nodeInfo.getCurrentTerm()).
//                            candidateAddr(nodeInfo.getSelf().getAddr()).
        lastLogIndex(logModule.getLastIndex()).
                                    lastLogTerm(lastTerm).
                                    build();

                    Request request = Request.builder()
                            .cmd(Request.R_VOTE)
                            .obj(param)
                            .url(peer.getAddr())
                            .build();

                    try {
                        return rpcClient.send(request);

                    } catch (RemotingException e) {
                        LOGGER.error(e.getMessage());
                        LOGGER.error("ElectionTask RPC Fail , URL : " + request.getUrl());
                        return null;
                    }
                }
            };
            Future<Response<VoteResult>> future = RaftThreadPool.submit(sendVoteTask);

            futureArrayList.add(future);
        }

        // 等待结果.
        AtomicInteger voteCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(futureArrayList.size());

        LOGGER.info("futureArrayList.size() : {}", futureArrayList.size());

        for (Future<Response<VoteResult>> future : futureArrayList) {
            Runnable waitResultTask = new Runnable() {
                @Override
                public void run() {
                    try {

                        Response<VoteResult> response = future.get(3000, MILLISECONDS);
                        if (response == null) {
                            return;
                        }
                        boolean isVoteGranted = response.getResult().isVoteGranted();

                        if (isVoteGranted) {
                            voteCount.incrementAndGet();
                        } else {
                            // 更新自己的任期.
                            long resTerm = response.getResult().getTerm();
                            if (resTerm > nodeInfo.getCurrentTerm()) {
                                nodeInfo.setCurrentTerm(resTerm);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            };
            RaftThreadPool.submit(waitResultTask);
        }

        try {
            // 稍等片刻
            latch.await(3500, MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("InterruptedException By Master election Task");
        }


        LOGGER.info("node {} maybe become leader , success count = {} , status : {}", nodeInfo.getSelf(), voteCount.get(), nodeInfo.getNodeStatus());
        // 如果投票期间,有其他服务器发送 appendEntry , 就可能变成 follower ,这时,应该停止.
        if (nodeInfo.getNodeStatus() == NodeInfo.NodeStatus.FOLLOWER) {
            return;
        }
        // 加上自身.
        if (voteCount.get() >= peers.size() / 2) {
            LOGGER.warn("node {} become leader ", nodeInfo.getSelf());
            nodeInfo.setNodeStatus(NodeInfo.NodeStatus.LEADER);
            nodeInfo.setLeader(nodeInfo.getSelf());
            nodeInfo.setVotedFor("");
            becomeLeaderToDoThing();
        } else {
            // else 重新选举
            nodeInfo.setVotedFor("");
        }
    }

    /**
     * 初始化所有的 nextIndex 值为自己的最后一条日志的 index + 1. 如果下次 RPC 时, 跟随者和leader 不一致,就会失败.
     * 那么 leader 尝试递减 nextIndex 并进行重试.最终将达成一致.
     */
    public void becomeLeaderToDoThing() {
        nodeInfo.getNextIndexs().clear();
        nodeInfo.getMatchIndexs().clear();
        for (Peer peer : nodeInfo.getPeerSet()) {
            nodeInfo.getNextIndexs().put(peer, logModule.getLastIndex() + 1);
            nodeInfo.getMatchIndexs().put(peer, 0L);
        }
    }

    @Override
    public void run() {
        if (nodeInfo.getNodeStatus() == NodeInfo.NodeStatus.LEADER) {
            return;
        }

        long current = System.currentTimeMillis();
        // 基于 RAFT 的随机时间,解决冲突.
        long electionTime = nodeInfo.getDefaultElectionTime() + ThreadLocalRandom.current().nextInt(50);

        if (current - preElectionTime < electionTime) {
            return;
        }
        preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(200) + 150;

        startElection();
    }


}

