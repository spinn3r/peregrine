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
package peregrine.http;

import java.io.*;
import java.util.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class HttpClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    private HttpClient client = null;

    private HttpResponse response = null;

    private List<ChannelBuffer> chunks = new ArrayList();
    
    public HttpClientHandler( HttpClient client ) {
        this.client = client;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Object message = e.getMessage();

        if ( message instanceof HttpResponse ) {

            this.response = (HttpResponse)message;

        } else if ( message instanceof HttpChunk) {

            HttpChunk chunk = (HttpChunk)message;

            if ( chunk.isLast() ) {

                //log.info( "Received HTTP response: %s for %s", response.getStatus(), client.uri );

                client.channelState  = HttpClient.CLOSED;
                client.response      = response;
                client.content       = getContent();

                if ( response.getStatus().getCode() != OK.getCode() ) {

                    log.warn( "Received HTTP response: %s for %s", response.getStatus(), client.uri );

                    client.failed( new IOException( response.getStatus().toString() ) );
                    return;
                }

                client.success();

            } else {
                chunks.add( chunk.getContent() );
            }
            
        }
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

        client.failed( e.getCause() );

    }

    private ChannelBuffer getContent() {

        ChannelBuffer[] buffers = new ChannelBuffer[ chunks.size() ];
        buffers = chunks.toArray( buffers );

        return ChannelBuffers.wrappedBuffer( buffers );
        
    }
    
}

