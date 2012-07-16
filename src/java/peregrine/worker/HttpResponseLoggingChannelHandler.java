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
import java.util.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.spinn3r.log5j.*;

/**
 * Handler to log all HTTP requests.
 */
public class HttpResponseLoggingChannelHandler extends SimpleChannelHandler {

    protected static final Logger log = Logger.getLogger();

    private HttpRequest request = null;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        
        if ( e.getMessage() instanceof HttpRequest ) {
            this.request = (HttpRequest)e.getMessage();
        }

        super.messageReceived( ctx, e );

    }

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        if ( e.getMessage() instanceof HttpResponse ) {
            
            HttpResponse response = (HttpResponse) e.getMessage();

            log.info( "%s %s %s %s", request.getMethod(), request.getUri(), request.getProtocolVersion(), response.getStatus() );

        }

        super.writeRequested( ctx, e );

    }
    
}
