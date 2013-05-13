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

    private State state = State.READY;

    private Partition partition;

    private String source;

    // the output channel for this client so we can write key/value pairs
    private SequenceWriter sequenceWriter = null;

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

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
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
     * The state of this request depending on the client.  The state will be
     * READY when a client is ready to read over the network, and SUSPENDED if
     * it is
     * idle and not reading results over the network.  If the client dropped
     * off (timeout, etc) then the request is in state CANCELLED.
     */
    public enum State {
        SUSPENDED, READY, CANCELLED;
    }

}
