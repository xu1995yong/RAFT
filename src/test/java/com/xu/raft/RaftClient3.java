package com.xu.raft;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.alipay.remoting.exception.RemotingException;
import com.google.common.collect.Lists;

import com.xu.raft.rpc.RaftRpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xu.raft.entity.LogEntry;
import com.xu.raft.rpc.Request;
import com.xu.raft.rpc.Response;
import com.xu.raft.rpc.ClientKVReq;

public class RaftClient3 {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient3.class);


    private final static RaftRpcClient client = new RaftRpcClient();

    static String addr = "localhost:8777";
    static List<String> list = Lists.newArrayList("localhost:8777", "localhost:8778", "localhost:8779");

    public static void main(String[] args) throws RemotingException, InterruptedException {

        AtomicLong count = new AtomicLong(3);

        int keyNum = 4;
        try {
            int index = (int) (count.incrementAndGet() % list.size());
            index = 1;
            addr = list.get(index);

            ClientKVReq obj = ClientKVReq.newBuilder().key("hello:" + keyNum).value("world:" + keyNum).type(ClientKVReq.PUT).build();

            Request<ClientKVReq> r = new Request<>();
            r.setObj(obj);
            r.setUrl(addr);
            r.setCmd(Request.CLIENT_REQ);
            Response<String> response = null;
            try {
                response = client.send(r);
            } catch (Exception e) {
            }

            // LOGGER.info("request content : {}, url : {}, put response : {}", obj.key + "=" + obj.getValue(), r.getUrl(), response.getResult());

            //SleepHelper.sleep(1000);

            obj = ClientKVReq.newBuilder().key("hello:" + keyNum).type(ClientKVReq.GET).build();

            addr = list.get(index);
            addr = list.get(index);
            r.setUrl(addr);
            r.setObj(obj);

            Response<LogEntry> response2;
            try {
                response2 = client.send(r);
            } catch (Exception e) {
                r.setUrl(list.get((int) ((count.incrementAndGet()) % list.size())));
                response2 = client.send(r);
            }

            if (response.getResult() == null) {
                //   LOGGER.error("request content : {}, url : {}, get response : {}", obj.key + "=" + obj.getValue(), r.getUrl(), response2.getResult());
                System.exit(1);
                return;
            }
            // LOGGER.info("request content : {}, url : {}, get response : {}", obj.key + "=" + obj.getValue(), r.getUrl(), response2.getResult());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(1);

    }

}
