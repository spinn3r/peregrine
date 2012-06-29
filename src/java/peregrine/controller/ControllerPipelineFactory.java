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

import static org.jboss.netty.channel.Channels.*;
import static peregrine.worker.FSPipelineFactory.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.worker.*;

import com.spinn3r.log5j.Logger;

/**
 * Netty pipeline handler for our controller.
 *
 */
public class ControllerPipelineFactory implements ChannelPipelineFactory {

    private static final Logger log = Logger.getLogger();

    private Config config;
    private ControllerDaemon controllerDaemon;
    
    public ControllerPipelineFactory( ControllerDaemon controllerDaemon, 
    							      Config config ) {

        this.controllerDaemon = controllerDaemon;
    	this.config = config;
    	
    }
    
    public ChannelPipeline getPipeline() throws Exception {

        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        if ( config.getTraceNetworkTraffic() ) {

            log.info( "Adding hex pipeline encoder to Netty pipeline." );
            
            pipeline.addLast( "hex",       new HexPipelineEncoder() );
        }

        pipeline.addLast("decoder",        new HttpRequestDecoder( MAX_INITIAL_LINE_LENGTH ,
                                                                   MAX_HEADER_SIZE,
                                                                   MAX_CHUNK_SIZE ) );

        pipeline.addLast("encoder",        new HttpResponseEncoder() );
        pipeline.addLast("handler",        new ControllerHandler( config, controllerDaemon ) );
        
        return pipeline;

    }

}
