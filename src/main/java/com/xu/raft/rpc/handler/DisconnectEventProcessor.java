package com.xu.raft.rpc.handler;

import com.alipay.remoting.Connection;
import com.alipay.remoting.ConnectionEventProcessor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ConnectionEventProcessor for ConnectionEventType.CLOSE
 *
 * @author xiaomin.cxm
 * @version $Id: DISCONNECTEventProcessor.java, v 0.1 Apr 8, 2016 10:58:48 AM xiaomin.cxm Exp $
 */
public class DisconnectEventProcessor implements ConnectionEventProcessor {

    private AtomicBoolean dicConnected = new AtomicBoolean();
    private AtomicInteger disConnectTimes = new AtomicInteger();

    @Override
    public void onEvent(String remoteAddr, Connection conn) {
        dicConnected.set(true);
        disConnectTimes.incrementAndGet();
    }

    public boolean isDisConnected() {
        return this.dicConnected.get();
    }

    public int getDisConnectTimes() {
        return this.disConnectTimes.get();
    }

    public void reset() {
        this.disConnectTimes.set(0);
        this.dicConnected.set(false);
    }
}