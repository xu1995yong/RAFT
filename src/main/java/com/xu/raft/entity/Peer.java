package com.xu.raft.entity;

import java.util.Objects;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Peer {
    private final String addr;

    private final String ip;
    private final int port;


    public Peer(String addr) {
        this.addr = addr;
        this.ip = addr.split(":")[0];
        this.port = Integer.valueOf(addr.split(":")[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Peer peer = (Peer) o;
        return Objects.equals(addr, peer.addr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(addr);
    }

}
