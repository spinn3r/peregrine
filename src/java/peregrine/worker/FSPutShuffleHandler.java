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

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.util.regex.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.io.chunk.*;
import peregrine.shuffle.receiver.*;

/**
 * Handles HTTP PUT shuffle messages.
 */
public class FSPutShuffleHandler extends FSPutBaseHandler {

    private static Pattern PATH_REGEX =
        Pattern.compile( "/([0-9]+)/shuffle/([a-zA-Z0-9_-]+)/from-partition/([-0-9]+)/from-chunk/([0-9]+)" );

    private int to_partition;
    private String name;
    private int from_partition;
    private int from_chunk;
    
    private ShuffleReceiver shuffleReceiver = null;
    
    public FSPutShuffleHandler( WorkerDaemon daemon, WorkerHandler handler ) throws Exception {
        super( handler );
        
        String path = handler.request.getUri();
        
        Matcher m = PATH_REGEX.matcher( path );

        if ( ! m.find() )
            throw new IOException( "The path specified is not a shuffle URL: " + path );

        this.to_partition   = Integer.parseInt( m.group( 1 ) );
        this.name           = m.group( 2 );
        this.from_partition = Integer.parseInt( m.group( 3 ) );
        this.from_chunk     = Integer.parseInt( m.group( 4 ) );

        this.shuffleReceiver = handler.daemon.getShuffleReceiverFactory().getInstance( this.name );

        this.path = shuffleReceiver.toString();
        
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        try {

            super.messageReceived( ctx, e );

            Object message = e.getMessage();

            if ( message instanceof HttpChunk ) {

                HttpChunk chunk = (HttpChunk)message;

                if ( ! chunk.isLast() ) {

                    ChannelBuffer content = chunk.getContent();

                    DefaultChunkReader reader = new DefaultChunkReader( content );

                    ChannelBuffer data = reader.data();
                    int count = reader.count();
                    
                    shuffleReceiver.accept( from_partition, from_chunk, to_partition, count, data );
                    
                } else {

                    HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
                    ChannelFuture cf = ctx.getChannel().write(response);

                    if ( ctx.getChannel().isOpen() )
                        cf.addListener(ChannelFutureListener.CLOSE);
                    
                }

            }

        } catch ( Exception exc ) {
            // catch all exceptions and then bubble them up.
            log.error( "Caught exception: ", exc );
            throw exc;
        }
            
    }

}

