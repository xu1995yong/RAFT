package com.xu.raft.node;

import com.xu.raft.entity.LogEntry;
import com.xu.raft.rpc.RaftRpcServer;
import com.xu.raft.task.ElectionTask;
import com.xu.raft.task.HeartBeatTask;
import com.xu.raft.task.RaftThreadPool;
import com.xu.raft.task.ReplicationFailQueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class DefaultNode<T> implements ApplicationListener<ContextRefreshedEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNode.class);

    @Autowired
    private NodeInfo nodeInfo;

    @Autowired
    private RaftRpcServer raftRpcServer;


    @Autowired
    private HeartBeatTask heartBeatTask;
    @Autowired
    private ElectionTask electionTask;
    @Autowired
    private ReplicationFailQueueConsumer replicationFailQueueConsumer;


    @Autowired
    private LogModule logModule;

    @PostConstruct
    private void init() {
        raftRpcServer.initAndStartUp(nodeInfo.getSelf().getPort());
    }

    private void start() {
        RaftThreadPool.scheduleWithFixedDelay(heartBeatTask, 500);
        RaftThreadPool.scheduleAtFixedRate(electionTask, 6000, 500);
//        RaftThreadPool.execute(replicationFailQueueConsumer);

        LogEntry logEntry = logModule.getLast();
        if (logEntry != null) {
            long term = logEntry.getTerm();
            nodeInfo.setCurrentTerm(term);
        }

        LOGGER.info("start success,   {} ", nodeInfo);
    }


    @PreDestroy
    public void destroy() {
    }


    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        start();
    }
}
