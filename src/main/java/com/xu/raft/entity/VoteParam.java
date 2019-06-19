package com.xu.raft.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 请求投票 RPC 参数.
 */
@Getter
@ToString
@Builder
public class VoteParam {
    // 候选人的任期号
    private long term;

    //  被请求者 Addr
    private String serverAddr;

    // 请求选票的候选人的 (ip:selfPort)
    private String candidateAddr;

    // 候选人的最后日志条目的索引值
    private long lastLogIndex;
    // 候选人最后日志条目的任期号
    private long lastLogTerm;
}
