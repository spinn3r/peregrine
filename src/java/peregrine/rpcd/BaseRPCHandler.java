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
package peregrine.rpcd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import peregrine.util.*;
import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;
import com.spinn3r.log5j.*;

/**
 */
public abstract class BaseRPCHandler<T> extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    private Channel channel = null;

    private Message message = null;

    private ExecutorService executorService;

    private String uri;
    
    public BaseRPCHandler( ExecutorService executorService, String uri ) {
        this.executorService = executorService;
        this.uri = uri;
    }

    public abstract void handleMessage( RPCDelegate<T> handler, Channel channel, Message message ) 
    	throws Exception;
    
    public abstract RPCDelegate<T> getRPCDelegate( String path );

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Object message = e.getMessage();

        channel = e.getChannel();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();
                byte[] data = new byte[ content.writerIndex() ];
                content.readBytes( data );
                
                this.message = new Message( new String( data ) );
                
            } else {

                doHandleMessage();

            }
                
        }
        
    }

    private void doHandleMessage() {

        try {
            
            URI uri = new URI( this.uri );

            String path = uri.getPath();

            final RPCDelegate delegate = getRPCDelegate( path );

            if ( delegate != null ) {
            	
            	log.info( "Handling with %,d params for URI: %s with %s: \n%s",
                          message.size(), uri, delegate.getClass().getName(), message.toDebugString() );
            	
                executorService.submit( new AsyncMessageHandler( channel, message ) {

                        public void doAction() throws Exception {
                            handleMessage( delegate, channel, message );
                        }
                        
                    } );

                return;
                
            } else {
                log.warn( "No handler for message %s at URI %s", message, uri );
            }

        } catch ( Exception e ) {
            log.error( "Could not handle RPC call: " , e );
        }

        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
        channel.write(response).addListener(ChannelFutureListener.CLOSE);
        return;

    }

    /**
     * Perform an action in a background thread, and then send a response code.
     */
    abstract class AsyncMessageHandler implements Runnable {

        private Channel channel;
        private Message message;
        
        public AsyncMessageHandler( Channel channel,
                                    Message message ) {
            this.channel = channel;
            this.message = message;
        }

        @Override
        public void run() {

            try {

                doAction();

                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
                channel.write(response).addListener(ChannelFutureListener.CLOSE);

            } catch ( Exception e ) {
                
                log.error( "Unable handle message: " + message, e );

                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
                channel.write(response).addListener(ChannelFutureListener.CLOSE);

            }

        }

        public abstract void doAction() throws Exception;

    }
    
}
