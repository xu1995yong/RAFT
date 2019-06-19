package com.xu.raft.rpc.handler;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.protocol.SyncUserProcessor;
import com.xu.raft.consistency.Consensus;
import com.xu.raft.entity.*;
import com.xu.raft.consistency.ClusterMembershipChanges;
import com.xu.raft.rpc.*;
import com.xu.raft.task.RaftThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Component
public class DefaultRaftProcessor<T> extends SyncUserProcessor<Request> implements RaftProcessor<Request> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftProcessor.class);

    @Autowired
    private Consensus consensus;

    @Autowired
    private ClusterMembershipChanges delegate;

    @Override
    public Object handleRequest(BizContext bizCtx, Request request) throws Exception {
        Object obj = request.getObj();
        switch (request.getCmd()) {

            case Request.R_VOTE: {
                VoteResult result = handlerRequestVote((VoteParam) obj);
                return new Response<>(result);
            }
            case Request.A_ENTRIES: {
                AppendEntryResult result = handlerAppendEntries((AppendEntryParam) obj);
                return new Response<>(result);
            }
            case Request.CLIENT_REQ: {
                return new Response<>(handlerClientRequest((ClientKVReq) obj));
            }
            case Request.CHANGE_CONFIG_REMOVE: {
                return new Response<>((removePeer((Peer) obj)));
            }
            case Request.CHANGE_CONFIG_ADD: {
                return new Response<>((addPeer((Peer) obj)));
            }
            default:
                return null;
        }
    }


    @Override
    public String interest() {
        return Request.class.getName();
    }


    public ClientKVAck redirect(Request request) {
//        Request<ClientKVReq> r = Request.newBuilder().
//                obj(request).url(peerSet.getLeader().getAddr()).cmd(Request.CLIENT_REQ).build();
//        Response response = rpcClient.send(r);
//        return (ClientKVAck) response.getResult();
        return null;
    }

    /**
     * 客户端的每一个请求都包含一条被复制状态机执行的指令。
     * 领导人把这条指令作为一条新的日志条目附加到日志中去，然后并行的发起附加条目 RPCs 给其他的服务器，让他们复制这条日志条目。
     * 当这条日志条目被安全的复制（下面会介绍），领导人会应用这条日志条目到它的状态机中然后把执行的结果返回给客户端。
     * 如果跟随者崩溃或者运行缓慢，再或者网络丢包，
     * 领导人会不断的重复尝试附加日志条目 RPCs （尽管已经回复了客户端）直到所有的跟随者都最终存储了所有的日志条目。
     *
     * @param request
     * @return
     */
    //TODO FIX
    public synchronized ClientKVAck handlerClientRequest(ClientKVReq request) {
//        LOGGER.warn("handlerClientRequest handler {} operation,  and key : [{}], value : [{}]",
//                ClientKVReq.Type.value(request.getType()), request.getKey(), request.getValue());
//
//        if (status != NodeStatus.LEADER) {
//            LOGGER.warn("I not am leader , only invoke redirect method, leader addr : {}, my addr : {}",
//                    peerSet.getLeader(), peerSet.getSelf().getAddr());
//            return redirect(request);
//        }
//
//        if (request.getType() == ClientKVReq.GET) {
//            LogEntry logEntry = stateMachine.get(request.getKey());
//            if (logEntry != null) {
//                return new ClientKVAck(logEntry.getCommand());
//            }
//            return new ClientKVAck(null);
//        }
//
//        LogEntry logEntry = LogEntry.newBuilder()
//                .command(Command.newBuilder().
//                        key(request.getKey()).
//                        value(request.getValue()).
//                        build())
//                .term(currentTerm)
//                .build();
//
//        // 预提交到本地日志, TODO 预提交
//        logModule.write(logEntry);
//        LOGGER.info("write logModule success, logEntry info : {}, log index : {}", logEntry, logEntry.getIndex());
//
//        final AtomicInteger success = new AtomicInteger(0);
//
//        List<Future<Boolean>> futureList = new CopyOnWriteArrayList<>();
//
//        int count = 0;
//        //  复制到其他机器
//        for (Peer peer : peerSet.getPeersWithOutSelf()) {
//            // TODO check self and RaftThreadPool
//            count++;
//            // 并行发起 RPC 复制.
//            futureList.add(replication(peer, logEntry));
//        }
//
//        CountDownLatch latch = new CountDownLatch(futureList.size());
//        List<Boolean> resultList = new CopyOnWriteArrayList<>();
//
//        getRPCAppendResult(futureList, latch, resultList);
//
//        try {
//            latch.await(4000, MILLISECONDS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        for (Boolean aBoolean : resultList) {
//            if (aBoolean) {
//                success.incrementAndGet();
//            }
//        }
//
//        // 如果存在一个满足N > commitIndex的 N，并且大多数的matchIndex[i] ≥ N成立，
//        // 并且log[N].term == currentTerm成立，那么令 commitIndex 等于这个 N （5.3 和 5.4 节）
//        List<Long> matchIndexList = new ArrayList<>(matchIndexs.values());
//        // 小于 2, 没有意义
//        int median = 0;
//        if (matchIndexList.size() >= 2) {
//            Collections.sort(matchIndexList);
//            median = matchIndexList.size() / 2;
//        }
//        Long N = matchIndexList.get(median);
//        if (N > commitIndex) {
//            LogEntry entry = logModule.read(N);
//            if (entry != null && entry.getTerm() == currentTerm) {
//                commitIndex = N;
//            }
//        }
//
//        //  响应客户端(成功一半)
//        if (success.get() >= (count / 2)) {
//            // 更新
//            commitIndex = logEntry.getIndex();
//            //  应用到状态机
//            getStateMachine().apply(logEntry);
//            lastApplied = commitIndex;
//
//            LOGGER.info("success apply local state machine,  logEntry info : {}", logEntry);
//            // 返回成功.
//            return ClientKVAck.ok();
//        } else {
//            // 回滚已经提交的日志.
//            logModule.removeOnStartIndex(logEntry.getIndex());
//            LOGGER.warn("fail apply local state  machine,  logEntry info : {}", logEntry);
//            // TODO 不应用到状态机,但已经记录到日志中.由定时任务从重试队列取出,然后重复尝试,当达到条件时,应用到状态机.
//            // 这里应该返回错误, 因为没有成功复制过半机器.
//            return ClientKVAck.fail();
//        }
        return null;
    }


    private void getRPCAppendResult(List<Future<Boolean>> futureList, CountDownLatch latch, List<Boolean> resultList) {
        for (Future<Boolean> future : futureList) {
            RaftThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        resultList.add(future.get(3000, MILLISECONDS));
                    } catch (CancellationException | TimeoutException | ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                        resultList.add(false);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
    }

    public VoteResult handlerRequestVote(VoteParam param) {
        LOGGER.info("handlerRequestVote will be invoke, param info : {}", param);
        VoteResult result = consensus.requestVote(param);
        LOGGER.info("Vote Result is {}", result);
        return result;
    }

    public AppendEntryResult handlerAppendEntries(AppendEntryParam param) {
        if (param.getEntries() != null) {
            LOGGER.warn("node receive node {} append entry, entry content = {}", param.getLeaderAddr(), param.getEntries());
        }

        return consensus.appendEntries(param);
    }

    public Result addPeer(Peer newPeer) throws RemotingException, InterruptedException {
        return delegate.addPeer(newPeer);
    }

    public Result removePeer(Peer oldPeer) {
        return delegate.removePeer(oldPeer);
    }

}
