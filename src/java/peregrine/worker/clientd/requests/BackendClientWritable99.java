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
public class BackendClientWritable99 implements ChannelBufferWritable {

    protected static final Logger log = Logger.getLogger();

    private static final int CHUNK_OVERHEAD = 12;

    private ClientBackendRequest clientBackendRequest;

    // the socket config for this channel so that we can reference sendBufferSize
    private NioSocketChannelConfig config;

    // the number of bytes pending to be written.  When we attempt to write data
    // we increment this value by the number of bytes we would write on the wire.
    // when the write is completed we decrement this value.  This uses a
    // AtomicInteger which would be slow for large numbers of messages but if
    // we're using a buffer of 64k we should be fine.
    private AtomicInteger pendingWriteSize = new AtomicInteger();

    // the listener from our last write or null if we haven't written yet.
    private WriteFutureListener writeFutureListener = null;

    private ChannelFuture future = null;

    private Channel channel;

    public BackendClientWritable99(ClientBackendRequest clientBackendRequest) {
        this.clientBackendRequest = clientBackendRequest;
        this.channel = clientBackendRequest.getChannel();

        config = (NioSocketChannelConfig)clientBackendRequest.getChannel().getConfig();

        log.info( "FIXME: sendBufferSize: %s", config.getSendBufferSize() );
        log.info( "FIXME: writeBufferHighWaterMark: %s", config.getWriteBufferHighWaterMark() );
        log.info( "FIXME: writeBufferLowWaterMark: %s", config.getWriteBufferLowWaterMark() );

    }

    @Override
    public void write( ChannelBuffer buff ) throws IOException {
        write( new DefaultHttpChunk( buff ) );
    }

    private void write( final HttpChunk chunk ) throws IOException {

        channel.write( chunk );

        log.info( "FIXME: interestops: %s", clientBackendRequest.getChannel().getInterestOps() );
        log.info( "FIXME: isSuspended: %s", isSuspended() );

//        Channel channel = clientBackendRequest.getChannel();
//
//        // increment the pending write size until the future decrements it.
//
//        pendingWriteSize.getAndAdd( getWriteLength(chunk) );
//
//        if ( writeFutureListener != null ) {
//
//            writeFutureListener = new WriteFutureListener( chunk, writeFutureListener.current );
//            future.addListener( );
//
//        }
//
//        //
//        future = channel.write(chunk);
//        writeFutureListener = new WriteFutureListener( future, chunk );
//        future.addListener(writeFutureListener);

    }

    private int getWriteLength(HttpChunk chunk) {
        return chunk.getContent().writerIndex() + CHUNK_OVERHEAD;
    }

    class WriteFutureListener implements ChannelFutureListener {

        // the previous HTTP chunk and since it's complete we can decrement
        // pendingWriteSize
        HttpChunk previous;

        // the current chunk we would like to write
        HttpChunk current = null;

        WriteFutureListener(HttpChunk current) {
            this.current = current;
        }

        WriteFutureListener(HttpChunk current, HttpChunk previous) {
            this.current = current;
            this.previous = previous;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {

            pendingWriteSize.getAndAdd( -getWriteLength( previous ) );

            if ( current != null ) {
                future.getChannel().write( current );
            }

        }

    }

    private void write_with_listeners( final HttpChunk chunk ) throws IOException {

        if ( future != null ) {

            future.addListener( new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {

                    // only write if we have successfully written this last.
                    if ( future.isSuccess() ) {
                        future.getChannel().write( chunk );
                    } else {
                        clientBackendRequest.setCancelled(true);
                    }
                }
            } );

        }

        future = clientBackendRequest.getChannel().write( chunk );

    }

    private void write_with_suspension( final HttpChunk chunk ) throws IOException {

        //FIXME: this code isn't working correctly I think because it suspends almost
        //immediately...

        if ( isSuspended() ) {

            log.info( "FIXME: here0");

            //write this via the channel future.  In practice we should only do this
            //once and then suspend the channel from further writing.  The only
            //exception here is if we call close() and THEN we we need to add that
            //to the queue for future writes.
            future.addListener( new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {

                    // only write if we have successfully written this last.
                    if ( future.isSuccess() ) {
                        future.getChannel().write( chunk );
                    } else {
                        clientBackendRequest.setCancelled(true);
                    }
                }
            } );

            return;
        }

        future = clientBackendRequest.getChannel().write(chunk);

        // any write request that fails needs to cancel the client.
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {

                if (future.isSuccess() == false) {
                    clientBackendRequest.setCancelled(true);
                }

            }

        });

        // TODO: attempt to JUST use futures to see if the protocol is corect.
        // then try to back out by



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
        log.info( "FIXME: writing last last. " );
        write( HttpChunk.LAST_CHUNK );
    }

}
