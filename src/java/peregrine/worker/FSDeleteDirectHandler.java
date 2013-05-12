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

import peregrine.io.partition.*;
import peregrine.io.util.*;

import com.spinn3r.log5j.Logger;

/**
 * Handles HTTP DELETE of files in PFS.
 */
public class FSDeleteDirectHandler extends ErrorLoggingChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    private WorkerHandler handler;
    private WorkerDaemon daemon;
    private Channel channel;
    
    public FSDeleteDirectHandler( WorkerDaemon daemon, WorkerHandler handler ) {
        this.daemon = daemon;
        this.handler = handler;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        channel = e.getChannel();

        daemon.getExecutorService( getClass() ).submit( new FSDeleteDirectCallable() );
        
    }

    class FSDeleteDirectCallable extends FSBaseDirectCallable {
        
        public Object call() throws Exception {

            int deleted = 0;

            // TODO: if the file does not exist should we return
            //
            // HTTP 404 File Not Found ?
            //
            // I think this would be the right thing to do
            //
            if ( exists( channel, handler.path ) ) {
                
                List<File> files = LocalPartition.getChunkFiles( handler.path );

                log.info( "Found %,d potential files to delete.", files.size() );
                
                for( File file : files ) {

                    try {
                        log.info( "Deleting %s", file.getPath() );
                        Files.remove( file );
                    } catch ( IOException e ) {

                        log.error( "Failed to delete: %s (Sending INTERNAL_SERVER_ERROR)", e, file.getPath() );
                        
                        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
                        channel.write(response).addListener(ChannelFutureListener.CLOSE);
                        return null;

                    }

                    ++deleted;
                    
                }

                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
                response.setHeader( "X-deleted", "" + deleted );
                
                channel.write(response).addListener(ChannelFutureListener.CLOSE);

            }

            return null;
            
        }
        
    }
    
}

