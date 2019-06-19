package com.xu.raft.rpc.handler;

import com.xu.raft.entity.AppendEntryParam;
import com.xu.raft.entity.AppendEntryResult;
import com.xu.raft.entity.VoteParam;
import com.xu.raft.entity.VoteResult;
import com.xu.raft.rpc.ClientKVAck;
import com.xu.raft.rpc.ClientKVReq;

public interface RaftProcessor<T> {


    /**
     * 处理请求投票 RPC.
     *
     * @param param
     * @return
     */
    VoteResult handlerRequestVote(VoteParam param);

    /**
     * 处理附加日志请求.
     *
     * @param param
     * @return
     */
    AppendEntryResult handlerAppendEntries(AppendEntryParam param);

    /**
     * 处理客户端请求.
     *
     * @param request
     * @return
     */
    ClientKVAck handlerClientRequest(ClientKVReq request);

    /**
     * 转发给 leader 节点.
     *
     * @param request
     * @return
     */
//    ClientKVAck redirect(ClientKVReq request);

}
