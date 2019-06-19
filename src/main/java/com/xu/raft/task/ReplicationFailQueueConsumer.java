package com.xu.raft.task;

import com.xu.raft.consistency.StateMachine;
import com.xu.raft.entity.ReplicationFailModel;
import com.xu.raft.node.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Component
public class ReplicationFailQueueConsumer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationFailQueueConsumer.class);

    private LinkedBlockingQueue<ReplicationFailModel> replicationFailQueue = new LinkedBlockingQueue<>(2048);
    @Autowired
    private StateMachine defaultStateMachine;
    @Autowired
    private NodeInfo nodeInfo;

    private long intervalTime = 1000 * 60;

    @Override
    public void run() {
        for (; ; ) {

            try {
                ReplicationFailModel model = replicationFailQueue.take();
                if (nodeInfo.getNodeStatus() != NodeInfo.NodeStatus.LEADER) {
                    // 应该清空?
                    replicationFailQueue.clear();
                    continue;
                }
                LOGGER.warn("replication Fail Queue Consumer take a task, will be retry replication, content detail : [{}]", model.logEntry);
                long offerTime = model.offerTime;
                if (System.currentTimeMillis() - offerTime > intervalTime) {
                    LOGGER.warn("replication Fail event Queue maybe full or handler slow");
                }

                Callable callable = model.callable;
                Future<Boolean> future = RaftThreadPool.submit(callable);
                Boolean r = future.get(3000, MILLISECONDS);
                // 重试成功.
                if (r) {
                    // 可能有资格应用到状态机.
                    tryApplyStateMachine(model);
                }

            } catch (InterruptedException e) {
                // ignore
            } catch (ExecutionException | TimeoutException e) {
                LOGGER.warn(e.getMessage());
            }
        }
    }

    private void tryApplyStateMachine(ReplicationFailModel model) {

        String success = defaultStateMachine.getString(model.successKey);
        defaultStateMachine.setString(model.successKey, String.valueOf(Integer.valueOf(success) + 1));

        String count = defaultStateMachine.getString(model.countKey);

        if (Integer.valueOf(success) >= Integer.valueOf(count) / 2) {
            defaultStateMachine.apply(model.logEntry);
            defaultStateMachine.delString(model.countKey, model.successKey);
        }
    }
}
