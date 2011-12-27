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
import static peregrine.pfsd.FSPipelineFactory.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

/**
 *
 * @version $Rev: 2226 $, $Date: 2010-03-31 11:26:51 +0900 (Wed, 31 Mar 2010) $
 */
public class HttpClientPipelineFactory implements ChannelPipelineFactory {

    private HttpClient client;
    
    public HttpClientPipelineFactory( HttpClient client ) {
        this.client = client;
    }
    
    public ChannelPipeline getPipeline() throws Exception {

        ChannelPipeline pipeline = pipeline();

        pipeline.addLast("codec",   new HttpClientCodec( MAX_INITIAL_LINE_LENGTH ,
                                                         MAX_HEADER_SIZE,
                                                         MAX_CHUNK_SIZE ));
        
        pipeline.addLast("handler", new HttpClientHandler( client ) );

        return pipeline;

    }
    
}
