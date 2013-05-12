/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.worker;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.spinn3r.log5j.*;

/**
 * Base handler for dealing with logging throughput and other issues with PUT.
 */
public class FSPutBaseHandler extends ErrorLoggingChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    /**
     * NR of bytes written.
     */
    protected long written = 0;

    /**
     * Time we started the request.
     */
    protected long started;

    /**
     * Number of chunks written.
     */
    protected long chunks = 0;

    protected WorkerHandler handler;

    protected String path = null;
    
    public FSPutBaseHandler( WorkerHandler handler ) {

        this.handler = handler;
        this.path = handler.path;
        
        started = System.currentTimeMillis();
        
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Object message = e.getMessage();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();
                written += content.writerIndex();
                chunks = chunks + 1;

            } else {

                // log that this was written successfully including the number
                // of bytes.

                long duration = System.currentTimeMillis() - started;

                int mean_chunk_size = 0;

                if ( chunks > 0 )
                    mean_chunk_size = (int)(written / chunks);

                log.debug( "Wrote %,d bytes in %,d chunks (mean chunk size = %,d bytes) in %,d ms to %s",
                           written, chunks, mean_chunk_size, duration, path );

            }

        }
            
    }

}

