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
package peregrine.controller;

import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.worker.*;
import peregrine.controller.rpcd.*;
import peregrine.controller.web.*;
import peregrine.http.*;
import peregrine.util.netty.*;

import com.spinn3r.log5j.*;

/**
 * Netty support for handling controller message.
 */
public class ControllerHandler extends DefaultChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    protected Config config;
    protected ControllerDaemon controllerDaemon;

    protected SimpleChannelUpstreamHandler handler = null;

    public ControllerHandler( Config config,
                              ControllerDaemon controllerDaemon ) {

        super( config );
        this.config = config;
        this.controllerDaemon = controllerDaemon;

    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        super.messageReceived( ctx, e );
        
        // introspect requests and route them to the correct handler.
        
        Object message = e.getMessage();

        if ( message instanceof HttpRequest ) {

            HttpMethod method = request.getMethod();

            if ( method == POST ) {
                handler = new ControllerRPCHandler( config, controllerDaemon, request.getUri() );
            } else if ( method == GET ) {

                String path = sanitizeUri( "web/" , request.getUri() );
                
                if ( path == null ) {
                    sendError(ctx, FORBIDDEN);
                    return;
                }

                // this is kind of a hack so that we can serve local files
                // easily and everything else is passed to velocity.
                if ( path.endsWith( ".png" ) ||
                     path.endsWith( ".jpg" ) ||
                     path.endsWith( ".css" ) ||
                     path.endsWith( ".js" ) ) {

                    handler = new FSGetDirectHandler( controllerDaemon, this, path );
                    
                } else {
                    handler = new ControllerWebHandler( config, controllerDaemon, request.getUri() );
                }

            }

        }

        if ( handler != null ) {
            handler.messageReceived( ctx, e );
        } else {
            sendError(ctx, NOT_FOUND);
        }

    }
    
}

