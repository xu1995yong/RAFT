package com.xu.raft.rpc;

import com.xu.raft.rpc.handler.DefaultRaftProcessor;
import com.xu.raft.rpc.handler.RaftProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component
public class RaftRpcServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRpcServer.class);
    private int port;

    @Autowired
    DefaultRaftProcessor defaultRaftProcessor;

    private com.alipay.remoting.rpc.RpcServer rpcServer;

    public void initAndStartUp(int port) {
        this.port = port;
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("{} started in port :{}", RaftRpcServer.class, port);
        }
        rpcServer = new com.alipay.remoting.rpc.RpcServer(this.port, false, false);
//        rpcServer.addConnectionEventProcessor(ConnectionEventType.CONNECT, new ConnectEventProcessor());
//        rpcServer.addConnectionEventProcessor(ConnectionEventType.CLOSE, new DisconnectEventProcessor());
        rpcServer.registerUserProcessor(defaultRaftProcessor);
        rpcServer.startup();
    }


    @PreDestroy
    public void stop() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("DefaultRpcServer shutdown in port :{}", this.port);
        }
        rpcServer.shutdown();
    }
}
