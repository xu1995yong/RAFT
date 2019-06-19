package com.xu.raft.consistency;

import com.xu.raft.node.NodeInfo;
import com.xu.raft.entity.Peer;
import com.xu.raft.entity.*;
import com.xu.raft.entity.VoteParam;
import com.xu.raft.entity.VoteResult;
import com.xu.raft.task.ElectionTask;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 默认的一致性模块实现.
 */

@Component
public class DefaultConsensus implements Consensus {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsensus.class);
    @Autowired
    private LogModule logModule;

    @Autowired
    private NodeInfo nodeInfo;
    @Autowired
    ElectionTask electionTask;

    public final ReentrantLock voteLock = new ReentrantLock();
    public final ReentrantLock appendLock = new ReentrantLock();


    @Override
    public VoteResult requestVote(VoteParam param) {

//        try {
//            VoteResult.VoteResultBuilder builder = VoteResult.builder();
//            if (!voteLock.tryLock()) {
//                return builder.term(nodeInfo.getCurrentTerm()).voteGranted(false).build();
//            }
//
//            // 对方任期没有自己新
//            if (param.getTerm() < nodeInfo.getCurrentTerm()) {
//                return builder.term(nodeInfo.getCurrentTerm()).voteGranted(false).build();
//            }
//
//            // (当前节点并没有投票 或者 已经投票过了且是对方节点) && 对方日志和自己一样新
//            LOGGER.info("nodeInfo {} task vote for [{}], param candidateAddr : {}", nodeInfo.getSelf(), nodeInfo.getVotedFor(), param.getCandidateAddr());
//            LOGGER.info("nodeInfo {} task term {}, peer term : {}", nodeInfo.getSelf(), nodeInfo.getCurrentTerm(), param.getTerm());
//
//            if ((StringUtil.isNullOrEmpty(nodeInfo.getVotedFor()) || nodeInfo.getVotedFor().equals(param.getCandidateAddr()))) {
//
//                if (logModule.getLast() != null) {
//                    // 对方没有自己新
//                    if (logModule.getLast().getTerm() > param.getLastLogTerm()) {
//                        return VoteResult.FAIL;
//                    }
//                    // 对方没有自己新
//                    if (logModule.getLastIndex() > param.getLastLogIndex()) {
//                        return VoteResult.FAIL;
//                    }
//                }
//
//                // 切换状态
//                nodeInfo.setNodeStatus(NodeInfo.NodeStatus.FOLLOWER);
//                // 更新
//                nodeInfo.setLeader(new Peer(param.getCandidateAddr()));
//                nodeInfo.setCurrentTerm(param.getTerm());
//                nodeInfo.setVotedFor(param.getServerAddr());
//                // 返回成功
//                return builder.term(nodeInfo.getCurrentTerm()).voteGranted(true).build();
//            }
//
//            return builder.term(nodeInfo.getCurrentTerm()).voteGranted(false).build();
//
//        } finally {
//            voteLock.unlock();
//        }
    }


    /**
     * 附加日志(多个日志,为了提高效率) RPC
     * <p>
     * 接收者实现：
     * 如果 term < currentTerm 就返回 false （5.1 节）
     * 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
     * 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
     * 附加任何在已有的日志中不存在的条目
     * 如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
     */
    @Override
    public AppendEntryResult appendEntries(AppendEntryParam param) {
        AppendEntryResult result = AppendEntryResult.fail();
        try {
            if (!appendLock.tryLock()) {
                return result;
            }

            result.setTerm(nodeInfo.getCurrentTerm());
            // 不够格
            if (param.getTerm() < nodeInfo.getCurrentTerm()) {
                return result;
            }


            .setPreElectionTime(System.currentTimeMillis());
            nodeInfo.setLeader(new Peer(param.getLeaderAddr()));

            // 够格
            if (param.getTerm() >= nodeInfo.getCurrentTerm()) {
                LOGGER.debug("nodeInfo {} become FOLLOWER, currentTerm : {}, param Term : {}, param serverAddr",
                        nodeInfo.getSelf(), nodeInfo.getCurrentTerm(), param.getTerm(), param.getServerAddr());
                // 认怂
                nodeInfo.setNodeStatus(NodeInfo.NodeStatus.FOLLOWER);
            }
            // 使用对方的 term.
            nodeInfo.setCurrentTerm(param.getTerm());

            //心跳
            if (param.getEntries() == null || param.getEntries().length == 0) {
                LOGGER.info("nodeInfo {} append heartbeat success , he's term : {}, my term : {}",
                        param.getLeaderAddr(), param.getTerm(), nodeInfo.getCurrentTerm());
                return AppendEntryResult.builder().term(nodeInfo.getCurrentTerm()).success(true).build();
            }

            // 真实日志
            // 第一次
            if (logModule.getLastIndex() != 0 && param.getPrevLogIndex() != 0) {
                LogEntry logEntry;
                if ((logEntry = logModule.read(param.getPrevLogIndex())) != null) {
                    // 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false
                    // 需要减小 nextIndex 重试.
                    if (logEntry.getTerm() != param.getPreLogTerm()) {
                        return result;
                    }
                } else {
                    // index 不对, 需要递减 nextIndex 重试.
                    return result;
                }

            }

            // 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的
            LogEntry existLog = logModule.read(((param.getPrevLogIndex() + 1)));
            if (existLog != null && existLog.getTerm() != param.getEntries()[0].getTerm()) {
                // 删除这一条和之后所有的, 然后写入日志和状态机.
                logModule.removeOnStartIndex(param.getPrevLogIndex() + 1);
            } else if (existLog != null) {
                // 已经有日志了, 不能重复写入.
                result.setSuccess(true);
                return result;
            }

            // 写进日志并且应用到状态机
            for (LogEntry entry : param.getEntries()) {
                logModule.write(entry);
                //  logModule.apply(entry);
                result.setSuccess(true);
            }

            //如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
            if (param.getLeaderCommit() > nodeInfo.getCommitIndex()) {
                int commitIndex = (int) Math.min(param.getLeaderCommit(), logModule.getLastIndex());
                nodeInfo.setCommitIndex(commitIndex);
                nodeInfo.setLastApplied(commitIndex);
            }

            result.setTerm(nodeInfo.getCurrentTerm());

            nodeInfo.setNodeStatus(NodeInfo.NodeStatus.FOLLOWER);
            // TODO, 是否应当在成功回复之后, 才正式提交? 防止 leader "等待回复"过程中 挂掉.
            return result;
        } finally {
            appendLock.unlock();
        }
    }


}
