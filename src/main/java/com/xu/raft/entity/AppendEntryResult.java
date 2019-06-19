package com.xu.raft.entity;

import java.io.Serializable;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Getter
@ToString
@Builder
public class AppendEntryResult implements Serializable {

    /**
     * 当前的任期号，用于领导人去更新自己
     */
    private long term;

    /**
     * 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
     */
    private boolean success;


//    public AppendEntryResult(boolean success) {
//        this.success = success;
//    }
//
//    public static AppendEntryResult fail() {
//        return new AppendEntryResult(false);
//    }
//
//    public static AppendEntryResult ok() {
//        return new AppendEntryResult(true);
//    }

}
