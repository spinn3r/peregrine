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

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

public class HexPipelineEncoder implements ChannelUpstreamHandler, ChannelDownstreamHandler {

    private Logger log = null;

    /**
     * 
     * 
     *
     */
    public HexPipelineEncoder( Logger log ) {
        this.log = log;
    }

    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent evt) throws Exception {

        try {

            handleEvent( "upstream", evt );
            
        } finally {
            ctx.sendUpstream( evt );
        }

    }

    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent evt) throws Exception {

        try {

            handleEvent( "downstream", evt );
            
        } finally {
            ctx.sendDownstream( evt );
        }
        
    }

    private void handleEvent( String direction, ChannelEvent evt ) throws Exception {

        if ( evt instanceof MessageEvent ) {
            
            MessageEvent e = (MessageEvent) evt;

            if ( e.getMessage() instanceof ChannelBuffer ) {
                ChannelBuffer buff = (ChannelBuffer) e.getMessage();
                log.info( "%s: \n%s", direction, Hex.pretty( buff ) );
            }

        } else {
            //log.warn( "NOT logging: %s", evt.getClass().getName() );
        }

    }
    
}
