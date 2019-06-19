package com.xu.raft.rpc;

import java.io.Serializable;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Getter
@ToString
@Builder
public class Response<T> implements Serializable {

    private T result;

    public Response(T result) {
        this.result = result;
    }


    public static final Response OK = new Response<>("ok");
    public static final Response FAIL = new Response<>("fail");
}
