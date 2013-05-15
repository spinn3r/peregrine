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

    /**
     * Handle an RPC message.  Return a ChannelBuffer for a custom response.
     * Use null for the default response.
     */
    public abstract ChannelBuffer handleMessage( RPCDelegate<T> handler, Channel channel, Message message ) 
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

        String rpc_call = this.uri;
        
        try {

            URI uri = new URI( this.uri );

            String path = uri.getPath();

            final RPCDelegate delegate = getRPCDelegate( path );

            if ( delegate != null ) {

                rpc_call = String.format( "%s %s: %s", uri, delegate.getClass().getName(), message.toDebugString() );

                // don't log heartbeat messages as they are overly verbose.
                
                if ( ! "heartbeat".equals( message.getString( "action" ) ) ) {
                    //log.info( "Handling RPC call %s", rpc_call );
                }
            	
                executorService.submit( new AsyncMessageHandler( channel, message ) {

                        public ChannelBuffer doAction() throws Exception {
                            return handleMessage( delegate, channel, message );
                        }
                        
                    } );

                return;
                
            } else {
                log.error( "No handler for message %s at URI %s", message, uri );
                sendError();
                return;
            }

        } catch ( RejectedExecutionException e ) {

            log.error( "Unable to accept request.  The executor service is shutdown: %s", rpc_call );
            sendError();
            return;

        } catch ( Exception e ) {
            log.error( "Could not handle RPC call: " , e );
            sendError();
            return;
        }

    }

    private void sendError() {
        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
        channel.write(response).addListener(ChannelFutureListener.CLOSE);
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

                ChannelBuffer content = doAction();

                if ( content == null ) {
                    content = ChannelBuffers.wrappedBuffer( "".getBytes() );
                }
                
                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
                response.setContent( content );
                
                channel.write(response).addListener(ChannelFutureListener.CLOSE);

            } catch ( Exception e ) {
                
                log.error( "Unable handle message (sending INTERNAL_SERVER_ERROR): " + message, e );

                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
                channel.write(response).addListener(ChannelFutureListener.CLOSE);

            }

        }

        /**
         * 
         */
        public abstract ChannelBuffer doAction() throws Exception;

    }
    
}
