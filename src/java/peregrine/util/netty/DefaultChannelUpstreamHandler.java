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
package peregrine.util.netty;

import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;

import java.io.*;
import java.net.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.http.*;

import com.spinn3r.log5j.*;

/**
 */
public class DefaultChannelUpstreamHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    public HttpRequest request;

    protected Config config;

    public DefaultChannelUpstreamHandler( Config config ) {
        this.config = config;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Object message = e.getMessage();

        if ( message instanceof HttpRequest ) {
            this.request = (HttpRequest)message;
        }

    }
            
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

        Channel ch = e.getChannel();
        Throwable cause = e.getCause();

        if ( request == null ) {
            log.error( String.format( "Could not handle initial request: " ), cause );
        } else {
            String tag = request.getHeader( HttpClient.X_TAG );
            log.error( String.format( "Could not handle request (tag=%s): %s %s", tag, request.getMethod(), request.getUri() ) , cause );
        }

        if (cause instanceof TooLongFrameException) {
            log.error( "Sending BAD_REQUEST" );
            sendError(ctx, BAD_REQUEST);
            return;
        }

        if (ch.isConnected()) {
            log.error( "Sending INTERNAL_SERVER_ERROR" );
            sendError(ctx, INTERNAL_SERVER_ERROR);
            return;
        }
        
    }

    public void sendOK( ChannelHandlerContext ctx ) {
        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }
    
    public void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);

        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

        String msg = "Failure: " + status.toString() + "\r\n";
        response.setContent( ChannelBuffers.copiedBuffer( msg, CharsetUtil.UTF_8));

        // Close the connection as soon as the error message is sent.
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

    }

    protected String sanitizeUri(String rootDirectory, String uri) throws java.net.URISyntaxException {

        // TODO: I believe this is actually wrong and that we have to try
        // ISO-8601 first according to the HTTP spec but I need to research this
        // problem.

        try {

            uri = URLDecoder.decode(uri, "UTF-8");

        } catch (UnsupportedEncodingException e) {

            try {
                uri = URLDecoder.decode(uri, "ISO-8859-1");
            } catch (UnsupportedEncodingException e1) {
                throw new Error();
            }

        }

        if ( uri.contains( "../" ) || uri.contains( "/.." ) )
            return null;

        // Convert to absolute path on the filesytem.
        return rootDirectory + new URI( uri ).getPath();
        
    }

}

