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
import java.net.*;
import java.util.regex.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.io.chunk.*;
import peregrine.util.primitive.IntBytes;
import peregrine.shuffle.receiver.*;

import com.spinn3r.log5j.*;

/**
 *
 * Handles interfacing with the table code and directly handling client requests
 * using the SSTableReader interface. 
 * 
 */
public class FSClientRequestHandler extends ErrorLoggingChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    private static Pattern PATH_REGEX =
        Pattern.compile( "/[0-9]+/client-rpc/(GET|SCAN|MUTATE)" );

    public FSClientRequestHandler( FSDaemon daemon, FSHandler handler ) throws Exception {

    }

    public boolean handles( URI uri ) {
        return PATH_REGEX.matcher( uri.getPath() ).find();
    }
    
    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        try {

        } catch ( Exception exc ) {
            // catch all exceptions and then bubble them up.
            log.error( "Caught exception: ", exc );
            throw exc;
        }
            
    }

}

