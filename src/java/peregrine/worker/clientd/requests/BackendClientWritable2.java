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

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;

import org.jboss.netty.channel.socket.SocketChannelConfig;
import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunk;

import com.spinn3r.log5j.*;
import peregrine.util.netty.ChannelBufferWritable;

/**
 * ChannelBufferWritable which writes to a Netty channel and uses HTTP chunks.
 * We pay attention to the state of the channel and if writes aren't completing
 * immediately, because they're exceeding the TCP send buffer, then we back off
 * and suspend the client.
 */
public class BackendClientWritable2 implements ChannelBufferWritable {

    protected static final Logger log = Logger.getLogger();

    private static final int CHUNK_OVERHEAD = 12;

    private ClientBackendRequest clientBackendRequest;

    // the socket config for this channel so that we can reference sendBufferSize
    private NioSocketChannelConfig config;

    private ChannelFuture future = null;

    private Channel channel;

    public BackendClientWritable2(ClientBackendRequest clientBackendRequest) {
        this.clientBackendRequest = clientBackendRequest;
        this.channel = clientBackendRequest.getChannel();

        config = (NioSocketChannelConfig)clientBackendRequest.getChannel().getConfig();

    }

    @Override
    public void write( ChannelBuffer buff ) throws IOException {
        write( new DefaultHttpChunk( buff ) );
    }

    private void write( final HttpChunk chunk ) throws IOException {

        if ( future != null ) {

            future.addListener( new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {

                    if ( future.isSuccess() )
                        future = channel.write( chunk );
                }

            } );

        }

        //
        future = channel.write(chunk);

    }

    /**
     * Return true when the client can't keep up with our writes.
     */
    public boolean isSuspended() {

        //return pendingWriteSize.get() > config.getSendBufferSize();

        return (channel.getInterestOps() & Channel.OP_WRITE) == Channel.OP_WRITE;

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
        write( HttpChunk.LAST_CHUNK );
    }

}
