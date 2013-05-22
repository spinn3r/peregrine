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
package peregrine.worker;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.util.netty.*;

import com.spinn3r.log5j.Logger;

/**
 * Handles HTTP GET of files in the filesystem.
 */
public class FSGetDirectHandler extends ErrorLoggingChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    private DefaultChannelUpstreamHandler handler;
    private BaseDaemon daemon;
    private Channel channel;

    private ChannelHandlerContext ctx = null;

    // the request URI / path we are handling
    private String path;
    
    public FSGetDirectHandler( BaseDaemon daemon, DefaultChannelUpstreamHandler handler, String path ) {
        this.daemon = daemon;
        this.handler = handler;
        this.path = path;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        this.channel = e.getChannel();
        this.ctx = ctx;
        
        daemon.getExecutorService( getClass() ).submit( new FSGetDirectCallable() );
        
    }

    class FSGetDirectCallable extends FSBaseDirectCallable {
        
        public Object call() throws Exception {

            if ( path == null ) {
                handler.sendError(ctx, FORBIDDEN);
                return null;
            }

            File file = new File( path );

            RandomAccessFile raf;
            try {
                raf = new RandomAccessFile(file, "r");
            } catch (FileNotFoundException fnfe) {
                log.warn( "File not found: %s", file );
                handler.sendError(ctx, NOT_FOUND);
                return null;
            }
            
            long fileLength = raf.length();

            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
            response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, (int)fileLength );

            channel.write(response);

            final FileRegion region =
                new DefaultFileRegion(raf.getChannel(), 0, fileLength);
            
            ChannelFuture writeFuture = channel.write(region);
            
            writeFuture.addListener(new ChannelFutureProgressListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        region.releaseExternalResources();
                    }
                    
                    @Override
                    public void operationProgressed( ChannelFuture future, long amount, long current, long total) {
                        //System.out.printf("%s: %d / %d (+%d)%n", path, current, total, amount);
                    }
                });
            
            //channel.addListener(ChannelFutureListener.CLOSE);

            return null;
            
        }
        
    }
    
}

