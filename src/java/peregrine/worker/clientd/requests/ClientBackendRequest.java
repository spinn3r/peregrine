/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package peregrine.worker.clientd.requests;

import org.jboss.netty.channel.Channel;
import peregrine.config.Partition;
import peregrine.io.SequenceWriter;

/**
 * Represents information about a client performing a request including the
 * netty Channel they are using, whether they are suspended, etc.
 */
public class ClientBackendRequest {

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
    public ClientBackendRequest(Partition partition, String source) {
        this.partition = partition;
        this.source = source;
    }

    public ClientBackendRequest(Channel channel, Partition partition, String source) {
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
