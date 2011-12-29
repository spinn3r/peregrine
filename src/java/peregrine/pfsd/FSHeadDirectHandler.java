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
package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import peregrine.io.partition.*;
import peregrine.util.*;

/**
 */
public class FSHeadDirectHandler extends SimpleChannelUpstreamHandler {

    private FSDaemon daemon;
    private FSHandler handler;
    
    public FSHeadDirectHandler( FSDaemon daemon, FSHandler handler ) {
        this.daemon = daemon;
        this.handler = handler;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Channel ch = e.getChannel();

        daemon.getExecutorService( getClass() )
            .submit( new FSHeadDirectCallable( handler.path, ch ) );
        
    }

}

class FSHeadDirectCallable extends FSBaseDirectCallable {

    private String path;
    private Channel channel;
    
    public FSHeadDirectCallable( String path,
                                 Channel channel ) {
        this.path = path;
        this.channel = channel;
    }
    
    public Object call() throws Exception {

        if ( exists( channel, path ) ) {
            
            List<File> files = LocalPartition.getChunkFiles( path );

            int length = 0;
            
            for ( File file : files ) {
                length += file.length();
            }
            
            int nr_chunks = files.size();

            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
            response.setHeader( "X-nr-chunks", "" + nr_chunks );
            response.setHeader( "X-length",    "" + length );
            
            channel.write(response);

        }

        return null;
        
    }
    
}
