package peregrine.io.sstable;

import org.jboss.netty.channel.Channel;
import peregrine.config.Partition;
import peregrine.io.SequenceWriter;

/**
 * Represents information about a client performing a request including the
 * netty Channel they are using, whether they are suspended, etc.
 */
public class ClientRequest {

    private Channel channel;

    private Partition partition;

    private String source;

    private boolean cancelled = false;

    // the output channel for this client so we can write key/value pairs
    private SequenceWriter sequenceWriter = null;

    // the time we received the request
    private long received = System.currentTimeMillis();

    /**
     * Create a client with no channel.  Results are only available within
     * the API and not sent anywhere.
     */
    public ClientRequest(Partition partition, String source) {
        this.partition = partition;
        this.source = source;
    }

    public ClientRequest(Channel channel, Partition partition, String source) {
        this.channel = channel;
        this.partition = partition;
        this.source = source;
    }

    public Channel getChannel() {
        return channel;
    }

    public Partition getPartition() {
        return partition;
    }

    public String getSource() {
        return source;
    }

    public SequenceWriter getSequenceWriter() {
        return sequenceWriter;
    }

    public void setSequenceWriter(SequenceWriter sequenceWriter) {
        this.sequenceWriter = sequenceWriter;
    }

    /**
     * Return true if the underlying Channel is suspended and can not handle writes
     * without blocking.  This is an indication that the channel needs to suspend
     * so that the client can catch up on reads.  This might be a client lagging
     * or it might just have very little bandwidth.
     */
    public boolean isSuspended() {
        return (channel.getInterestOps() & Channel.OP_WRITE) == Channel.OP_WRITE;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public long getReceived() {
        return received;
    }
}
