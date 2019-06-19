package com.xu.raft.task;

import com.xu.raft.entity.AppendEntryParam;
import com.xu.raft.entity.AppendEntryResult;
import com.xu.raft.node.NodeInfo;
import com.xu.raft.entity.Peer;
import com.xu.raft.rpc.RaftRpcClient;
import com.xu.raft.rpc.Request;
import com.xu.raft.rpc.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class HeartBeatTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartBeatTask.class);
    //上次一心跳时间戳
    private volatile long preHeartBeatTime = 0;

    // 心跳间隔基数
    @Value("${raft.heartBeatTick}")
    private long heartBeatTick;

    @Autowired
    NodeInfo nodeInfo;
    @Autowired
    private RaftRpcClient rpcClient;

    @Override
    public void run() {
        //只有Leader才能发送心跳包
        if (nodeInfo.getNodeStatus() != NodeInfo.NodeStatus.LEADER) {
            return;
        }

        if (System.currentTimeMillis() - preHeartBeatTime < heartBeatTick) {
            return;
        }
        LOGGER.info("=========== NextIndex =============");
        for (Peer peer : nodeInfo.getPeerSet()) {
            LOGGER.info("Peer {} nextIndex={}", peer.getAddr(), nodeInfo.getNextIndexs().get(peer));
        }

        preHeartBeatTime = System.currentTimeMillis();

        // 心跳只关心 term 和 leaderID

        //TODO:修改为
        for (Peer peer : nodeInfo.getPeerSet()) {

            AppendEntryParam param = AppendEntryParam.builder()
                    .entries(null)// 心跳,空日志.
                    // .leaderAddr(nodeInfo.getSelf().getAddr())
                    //  .serverId(peer.getAddr())
                    .term(nodeInfo.getCurrentTerm())
                    .build();

            Request<AppendEntryParam> request = Request.<AppendEntryParam>builder().
                    cmd(Request.A_ENTRIES).obj(param).url(peer.getAddr()).build();


            RaftThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Response<AppendEntryResult> response = rpcClient.send(request);
                        AppendEntryResult aentryResult = response.getResult();
                        long term = aentryResult.getTerm();

                        if (term > nodeInfo.getCurrentTerm()) {
                            LOGGER.error("self will become follower, he's term : {}, my term : {}", term, nodeInfo.getCurrentTerm());
                            nodeInfo.setCurrentTerm(term);
                            nodeInfo.setVotedFor("");

                            nodeInfo.setNodeStatus(NodeInfo.NodeStatus.FOLLOWER);
                        }
                    } catch (Exception e) {
                        LOGGER.error("HeartBeatTask RPC Fail, request URL : {} ", request.getUrl());
                    }
                }
            }, false);
        }
    }
}
