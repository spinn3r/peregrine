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
package peregrine.controller.web;

import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;
import java.util.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.controller.rpcd.delegate.*;
import peregrine.http.*;
import peregrine.rpc.*;
import peregrine.rpcd.*;
import peregrine.rpcd.delegate.*;

import org.apache.velocity.app.*;
import org.apache.velocity.*;

import com.spinn3r.log5j.*;

/**
 */
public class ControllerWebHandler extends SimpleChannelUpstreamHandler  {
    
    private static final Logger log = Logger.getLogger();
    
    private static final ControllerRPCDelegate delegate = new ControllerRPCDelegate();
    
    protected Config config;
    protected ControllerDaemon controllerDaemon;
    protected String uri;
    
    public ControllerWebHandler( Config config,
                                 ControllerDaemon controllerDaemon,
                                 String uri ) {
        
        this.config = config;
        this.controllerDaemon = controllerDaemon;
        this.uri = uri;
        
    }
    
    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Channel channel = e.getChannel();

        VelocityContext context = new VelocityContext();
        context.put( "config", config );
        context.put( "controller", controllerDaemon.getController() );

        QueryStringDecoder decoder = new QueryStringDecoder( uri );

        Map<String,List<String>> params = decoder.getParameters();

        // new map with just key/value mappings.  
        Map<String,String> map = new HashMap();

        for( String key : params.keySet() ) {
            map.put( key, params.get( key ).get(0) );
        }

        context.put( "params", map );
        
        StringWriter sw = new StringWriter();

        Template template = Velocity.getTemplate("web/index.vm");

        template.merge( context, sw );
        
        ChannelBuffer content = ChannelBuffers.wrappedBuffer( sw.toString().getBytes() );

        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
        response.setContent( content );
        
        channel.write(response).addListener(ChannelFutureListener.CLOSE);
        
    }

    static {

        Velocity.setProperty( VelocityEngine.FILE_RESOURCE_LOADER_PATH, ".");
        Velocity.setProperty( VelocityEngine.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.Log4JLogChute" );
        Velocity.setProperty( VelocityEngine.VM_PERM_ALLOW_INLINE_REPLACE_GLOBAL, true);
        Velocity.setProperty( "runtime.log.logsystem.log4j.logger", "velocity" );
        Velocity.setProperty( "file.resource.loader.cache", "false" );
        Velocity.init();
        
    }
    
}

