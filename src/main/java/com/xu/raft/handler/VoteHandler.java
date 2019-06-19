package com.xu.raft.handler;

import com.xu.raft.entity.Peer;
import com.xu.raft.entity.VoteParam;
import com.xu.raft.entity.VoteResult;
import com.xu.raft.node.NodeInfo;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.locks.ReentrantLock;

public final class VoteHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(VoteHandler.class);

    @Autowired
    LogEntryHandler logEntryHandler;

    private final ReentrantLock voteLock = new ReentrantLock();
    //TODO ：将投票状态从节点状态中剥离，

    @Autowired
    private NodeInfo nodeInfo;

    /**
     * 请求投票 RPC
     * <p>
     * 接收者实现：
     * 如果term < currentTerm返回 false （5.2 节）
     * 如果 votedFor 为空或者就是 candidateAddr，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
     */
    public VoteResult requestVote(VoteParam param) {

        try {
            VoteResult.VoteResultBuilder builder = VoteResult.builder();
            //当前有另外一个投票正在进行
            if (!voteLock.tryLock()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("A vote is taking place at the current node {},and receive a vote request from {}", nodeInfo.getSelf(), param.getCandidateAddr());
                }
                return builder.term(nodeInfo.getCurrentTerm()).voteGranted(false).build();
            }

            // 对方任期没有自己新
            if (param.getTerm() < nodeInfo.getCurrentTerm()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("");
                }
                return builder.term(nodeInfo.getCurrentTerm()).voteGranted(false).build();
            }
//            if (LOGGER.isInfoEnabled()) {
//                LOGGER.info("nodeInfo {} task vote for [{}], param candidateAddr : {}", nodeInfo.getSelf(), nodeInfo.getVotedFor(), param.getCandidateAddr());
//                LOGGER.info("nodeInfo {} task term {}, peer term : {}", nodeInfo.getSelf(), nodeInfo.getCurrentTerm(), param.getTerm());
//            }

            //  (当前节点并没有投票 或者 已经投票过了且是对方节点) && 对方日志和自己一样新
            if ((StringUtil.isNullOrEmpty(nodeInfo.getVotedFor()) || nodeInfo.getVotedFor().equals(param.getCandidateAddr()))) {
                if (LOGGER.isInfoEnabled()) {

                }
                if (logEntryHandler.getLast() != null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("The latest log for this node is {}", logEntryHandler.getLast());
                    }
                    // 对方没有自己新
                    if (logEntryHandler.getLast().getTerm() > param.getLastLogTerm()) {
                        return VoteResult.builder().term(nodeInfo.getCurrentTerm()).voteGranted(false).build();
                    }
                    // 对方没有自己新
                    if (logEntryHandler.getLastIndex() > param.getLastLogIndex()) {
                        return VoteResult.builder().term(nodeInfo.getCurrentTerm()).voteGranted(false).build();
                    }
                }

                // 切换状态
                nodeInfo.setNodeStatus(NodeInfo.NodeStatus.FOLLOWER);
                // 更新
                nodeInfo.setLeader(new Peer(param.getCandidateAddr()));
                nodeInfo.setCurrentTerm(param.getTerm());
                nodeInfo.setVotedFor(param.getServerAddr());
                // 返回成功
                return builder.term(nodeInfo.getCurrentTerm()).voteGranted(true).build();
            }

            return builder.term(nodeInfo.getCurrentTerm()).voteGranted(false).build();

        } finally {
            voteLock.unlock();
        }
    }
}