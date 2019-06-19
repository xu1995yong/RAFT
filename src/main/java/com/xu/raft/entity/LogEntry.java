package com.xu.raft.entity;

import java.io.Serializable;
import java.util.Objects;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Getter
@ToString
@Builder
public class LogEntry implements Serializable, Comparable {

    private long term;
    @Setter
    private long index;

    private Command command;

    public LogEntry() {
    }

    public LogEntry(long term, Command command) {
        this.term = term;
        this.command = command;
    }

    public LogEntry(long index, long term, Command command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }


    @Override
    public int compareTo(Object o) {
        if (o == null) {
            return -1;
        }
        if (this.getIndex() > ((LogEntry) o).getIndex()) {
            return 1;
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term &&
                Objects.equals(index, logEntry.index) &&
                Objects.equals(command, logEntry.command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, term, command);
    }


}
