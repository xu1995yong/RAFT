package com.xu.raft;

import com.xu.raft.rpc.RaftRpcClient;
import com.xu.raft.rpc.RaftRpcServer;
import com.xu.raft.rpc.Request;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = RaftApplication.class)
public class RaftApplicationTests {

    @Autowired
    RaftRpcClient client;

    @Autowired
    RaftRpcServer raftRpcServer;

    @Test
    public void testClient() {
        String url = "127.0.0.1:8777";
        Request request = Request.newBuilder().url(url).cmd(Request.R_VOTE).build();
        // client.send(request);
    }

    @Test
    public void testServer() {
    }

}
