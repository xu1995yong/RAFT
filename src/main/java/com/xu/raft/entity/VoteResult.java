package com.xu.raft.entity;

import java.io.Serializable;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
@Builder
public class VoteResult implements Serializable {

    //当前任期号，以便于候选人去更新自己的任期
    long term;

    //候选人赢得了此张选票时为真
    boolean voteGranted;
}
