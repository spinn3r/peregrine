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
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;

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
public class BackendClientChannelBufferWritable implements ChannelBufferWritable {

    protected static final Logger log = Logger.getLogger();

    private Channel channel;

    private ChannelFuture future = null;

    public BackendClientChannelBufferWritable(Channel channel) {
        this.channel = channel;
    }
    
    @Override
    public void write( ChannelBuffer buff ) throws IOException {

        write( new DefaultHttpChunk( buff ) );

    }

    private void write( final HttpChunk chunk ) throws IOException {

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
                    future.getChannel().write( chunk );
                }
            } );

            return;
        }

        future = channel.write( chunk );

    }

    public boolean isSuspended() {

        if ( future == null )
            return false;

        return future.isDone() == false;

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
        log.info( "FIXME: writing last chunk. " );
        write( HttpChunk.LAST_CHUNK );
    }
    
}
