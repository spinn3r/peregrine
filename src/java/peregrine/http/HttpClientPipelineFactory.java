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
package peregrine.http;

import static org.jboss.netty.channel.Channels.*;
import static peregrine.worker.WorkerPipelineFactory.*;

import peregrine.config.*;
import peregrine.worker.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.spinn3r.log5j.Logger;

/**
 *
 * @version $Rev: 2226 $, $Date: 2010-03-31 11:26:51 +0900 (Wed, 31 Mar 2010) $
 */
public class HttpClientPipelineFactory implements ChannelPipelineFactory {

    private static final Logger log = Logger.getLogger();

    private HttpClient client;
    private Config config;
    
    public HttpClientPipelineFactory( Config config, HttpClient client ) {
        this.config = config;
        this.client = client;
    }
    
    public ChannelPipeline getPipeline() throws Exception {

        ChannelPipeline pipeline = pipeline();

        if ( config.getTraceNetworkTraffic() ) {
            log.info( "Adding hex pipeline encoder to Netty pipeline." );
            pipeline.addLast( "hex",       new HexPipelineEncoder( log ) );
        }

        pipeline.addLast("codec",   new HttpClientCodec( MAX_INITIAL_LINE_LENGTH ,
                                                         MAX_HEADER_SIZE,
                                                         (int)config.getHttpMaxChunkSize() ));
        
        pipeline.addLast("handler", new HttpClientHandler( client ) );

        return pipeline;

    }
    
}
