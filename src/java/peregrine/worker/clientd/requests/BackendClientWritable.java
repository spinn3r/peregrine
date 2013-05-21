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

import com.spinn3r.log5j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunk;
import peregrine.util.netty.ChannelBufferWritable;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * ChannelBufferWritable which writes to a Netty channel and uses HTTP chunks.
 * We pay attention to the state of the channel and if writes aren't completing
 * immediately, because they're exceeding the TCP send buffer, then we back off
 * and suspend the client.
 */
public class BackendClientWritable implements ChannelBufferWritable {

    protected static final Logger log = Logger.getLogger();

    private static final int CHUNK_OVERHEAD = 12;

    private Channel channel;

    private ArrayBlockingQueue<HttpChunk> queue = new ArrayBlockingQueue<HttpChunk>(100);

    public BackendClientWritable(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void write( ChannelBuffer buff ) throws IOException {
        write( new DefaultHttpChunk( buff ) );
    }

    private ChannelFuture write( final HttpChunk chunk ) throws IOException {
        return channel.write(chunk);
    }

    /**
     * Return true when the client can't keep up with our writes.
     */
    public boolean isSuspended() {
        return channel.isWritable() == false;
    }

    @Override
    public void shutdown() throws IOException {
    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void flush() throws IOException {
        // there's nothing to buffer so we are done.
    }

    @Override
    public void close() throws IOException {
        write( HttpChunk.LAST_CHUNK ).addListener(ChannelFutureListener.CLOSE);
    }

}
