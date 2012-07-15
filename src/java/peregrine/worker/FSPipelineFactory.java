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
public class FSPipelineFactory implements ChannelPipelineFactory {

    private static final Logger log = Logger.getLogger();

    public static int MAX_INITIAL_LINE_LENGTH    = 1024;
    public static int MAX_HEADER_SIZE            = 1024;

    /**
     * The memory consumption of netty partially depends on these variables.
     * 
     * If we have 100 servers in a cluster.  And each is using 8192 to buffer
     * chunks, and each has 25 partitions this would require 100*25 total
     * connections and 8192*100*25 bytes of memory (20.4MB).  At 1024 bytes this
     * would require 2.5MB.
     *
     * <pre>
     * servers    partitions    buffer    memory_per_server
     * 100        25            1024        2.5 MB
     * 100        25            8192       20.4 MB
     * 1000       25            1024       25.0 MB
     * 1000       25            8192      204.0 MB
     * </pre>
     */
    public static int MAX_CHUNK_SIZE             = 16384;

    private Config config;
    private FSDaemon daemon;
    
    public FSPipelineFactory( Config config,
                              FSDaemon daemon ) {

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
                                                                   MAX_CHUNK_SIZE ) );

        pipeline.addLast("encoder",        new HttpResponseEncoder() );

        pipeline.addLast("logger",         new HttpResponseLoggingChannelHandler() );

        pipeline.addLast("handler",        new FSHandler( config, daemon ));
        
        return pipeline;

    }

}
