package com.xu.raft.exception;

/**
 *
 */
public class RaftRemotingException extends RuntimeException {

    public RaftRemotingException() {
        super();
    }

    public RaftRemotingException(String message) {
        super(message);
    }
}
