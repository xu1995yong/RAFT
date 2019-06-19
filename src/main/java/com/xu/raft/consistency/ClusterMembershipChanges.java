package com.xu.raft.consistency;

import com.alipay.remoting.exception.RemotingException;
import com.xu.raft.entity.Peer;
import com.xu.raft.entity.Result;

/**
 * 集群配置变更接口.
 */
public interface ClusterMembershipChanges {

    /**
     * 添加节点.
     *
     * @param newPeer
     * @return
     */
    Result addPeer(Peer newPeer) throws RemotingException, InterruptedException;

    /**
     * 删除节点.
     *
     * @param oldPeer
     * @return
     */
    Result removePeer(Peer oldPeer);
}

