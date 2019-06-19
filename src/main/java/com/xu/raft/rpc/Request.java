package com.xu.raft.rpc;

import java.io.Serializable;

import com.xu.raft.entity.AppendEntryParam;
import com.xu.raft.entity.VoteParam;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
@Builder
public class Request<T> implements Serializable {

    //请求投票
    public static final int R_VOTE = 0;

    //附加日志
    public static final int A_ENTRIES = 1;

    // 客户端
    public static final int CLIENT_REQ = 2;

    // 配置变更. add
    public static final int CHANGE_CONFIG_ADD = 3;

    // 配置变更. remove
    public static final int CHANGE_CONFIG_REMOVE = 4;


    //请求类型
    private int cmd = -1;

    private T obj;

    private String sourceAddr;

    private String url;

}
