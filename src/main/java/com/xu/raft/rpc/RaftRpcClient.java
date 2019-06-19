package com.xu.raft.rpc;

import com.alipay.remoting.exception.RemotingException;

import com.xu.raft.exception.RaftRemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class RaftRpcClient {
    public static Logger logger = LoggerFactory.getLogger(RaftRpcClient.class.getName());
    private com.alipay.remoting.rpc.RpcClient rpcClient = new com.alipay.remoting.rpc.RpcClient();

    @Value("${raft.rpc.client.timeout}")
    private int timeout;

    @PostConstruct
    public void init() {
        rpcClient.startup();
    }


    public Response send(Request request) throws RemotingException, InterruptedException {
        if (logger.isDebugEnabled()) {
            logger.debug("send {} {} for {}", request.getCmd(), request.getObj(), request.getUrl());
        }
        return send(request, timeout);
    }


    public Response send(Request request, int timeout) throws RemotingException, InterruptedException {
        return (Response) rpcClient.invokeSync(request.getUrl(), request, timeout);
    }
}
