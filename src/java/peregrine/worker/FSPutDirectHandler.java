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

import peregrine.util.netty.*;
import peregrine.os.*;
import peregrine.io.util.*;

import java.io.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

/**
 */
public class FSPutDirectHandler extends FSPutBaseHandler {

    public static byte[] EOF = new byte[0];
    
    private ChannelBufferWritable output = null;

    public FSPutDirectHandler( WorkerDaemon daemon, FSHandler handler ) throws IOException {

        super( handler );

        File file = new File( handler.path );
        
        // FIXME: this mkdir should be async.
        Files.mkdirs( file.getParent() );
        
        output = new MappedFileWriter( daemon.getConfig(), file );
        
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        super.messageReceived( ctx, e );
        
        Object message = e.getMessage();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                output.write( chunk.getContent() );
                
            } else {

                // FIXME this must be async ... 
                output.close();
                
                // FIXME: I don't like how the status sending is decoupled from
                // pipeline requests AND non-pipeline requests.  I need to unify
                // these.

                if ( handler.remote == null ) {
                    HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
                    ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
                }

            }

        }
            
    }

}

