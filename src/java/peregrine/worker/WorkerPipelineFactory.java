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

import static org.jboss.netty.channel.Channels.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import peregrine.config.Config;

import com.spinn3r.log5j.Logger;

/**
 */
public class WorkerPipelineFactory implements ChannelPipelineFactory {

    private static final Logger log = Logger.getLogger();

    public static int MAX_INITIAL_LINE_LENGTH    = 1024;
    public static int MAX_HEADER_SIZE            = 1024;

    private Config config;
    private WorkerDaemon daemon;
    
    public WorkerPipelineFactory(Config config,
                                 WorkerDaemon daemon) {

        this.config = config;
        this.daemon = daemon;
        
    }

    public ChannelPipeline getPipeline() throws Exception {

        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        if ( config.getTraceNetworkTraffic() ) {
            log.info( "Adding hex pipeline encoder to Netty pipeline." );
            pipeline.addLast( "hex",       new HexPipelineEncoder( log ) );
        }
        
        pipeline.addLast("decoder",        new HttpRequestDecoder( MAX_INITIAL_LINE_LENGTH ,
                                                                   MAX_HEADER_SIZE,
                                                                   (int)config.getHttpMaxChunkSize() ) );

        pipeline.addLast("encoder",        new HttpResponseEncoder() );

        pipeline.addLast("logger",         new HttpResponseLoggingChannelHandler() );

        pipeline.addLast("handler",        new WorkerHandler( config, daemon ));
        
        return pipeline;

    }

}
