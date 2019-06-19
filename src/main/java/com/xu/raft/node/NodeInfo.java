package com.xu.raft.node;

import com.xu.raft.entity.Peer;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
@Getter
@Setter
@ToString
public class NodeInfo {
    @Value("${raft.port}")
    private int selfPort;
    private Peer self;
    private volatile Peer leader;
    private volatile NodeStatus nodeStatus = NodeStatus.FOLLOWER;

    // 选举时间间隔基数
    @Value("${raft.defaultElectionTime}")
    private volatile long defaultElectionTime;
    // 已知的最大的已经被提交的日志条目的索引值
    private volatile long commitIndex;

    // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增)
    private volatile long lastApplied = 0;

    /* ========== 在领导人里经常改变的(选举后重新初始化) ================== */

    // 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
    private Map<Peer, Long> nextIndexs;

    // 对于每一个服务器，已经复制给他的日志的最高索引值
    private Map<Peer, Long> matchIndexs;

    /* ============ 所有服务器上持久存在的 ============= */
    // 服务器最后一次知道的任期号（初始化为 0，持续递增）
    private volatile long currentTerm = 0;
    // 在当前获得选票的候选人的 Id
    private volatile String votedFor;
    // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号


    private final List<Peer> peerSet = new ArrayList<>();

    @PostConstruct
    public void init() {
        setSelf(new Peer("localhost:" + selfPort));
        List<String> peerAddrs = Arrays.asList("localhost:8775", "localhost:8776", "localhost:8777");
        for (String s : peerAddrs) {
            Peer peer = new Peer(s);
            if (!peer.equals(self)) {
                peerSet.add(peer);
            }
        }
    }


    public enum NodeStatus {
        FOLLOWER(0), CANDIDATE(1), LEADER(2);

        NodeStatus(int code) {
            this.code = code;
        }

        int code;
    }

}
