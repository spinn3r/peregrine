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
import java.util.*;
import java.util.regex.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.client.*;
import peregrine.config.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.io.sstable.*;
import peregrine.io.util.*;
import peregrine.shuffle.receiver.*;
import peregrine.util.*;
import peregrine.util.primitive.IntBytes;

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
        Pattern.compile( "/([0-9]+)/client-rpc/(GET|SCAN|MUTATE)" );

    private Config config;
    
    public FSClientRequestHandler( Config config ) throws Exception {
        this.config = config;
    }

    /**
     * Return true if we can handle the given URI.
     */
    public boolean handles( URI uri ) {
        return PATH_REGEX.matcher( uri.getPath() ).find();
    }

    /**
     * Execute the given request directly.
     */
    public void exec( String uri ) throws IOException {

        QueryStringDecoder decoder = new QueryStringDecoder( uri );

        Matcher matcher = PATH_REGEX.matcher( uri );

        Partition part = null;
        
        if ( matcher.find() ) {

            part = new Partition( Integer.parseInt( matcher.group( 1 ) ) );
            
        } else {
            throw new IOException( "no match" );
        }

        GetRequest request = GetRequestURLParser.toRequest( uri );
        
        //FIXME: we should support a table cache here... 
        
        LocalPartitionReader reader = null;
        DefaultChunkWriter writer = null;

        //FIXME: we should have group 'commit' of requests or intelligent/smart
        //batching so that if we come BACK to the queue and it's full with
        //seekTo or scan requess that we can elide them so that blocks only need
        //to be decompressed for ALL inbound requests.

        // http://mechanical-sympathy.blogspot.com/2011/10/smart-batching.html

        try {

            reader = new LocalPartitionReader( config, part, request.getSource() );
            //writer = new DefaultChunkWriter( config , path, 
            
            reader.seekTo( request.getKeys(), new RecordListener() {

                    @Override
                    public void onRecord( StructReader key, StructReader value ) {
                        // write this out over the wire for now.
                    }
                    
                } );
            
        } finally {
            new Closer( writer, reader ).close();
        }

    }
    
    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        try {

            HttpRequest request = (HttpRequest)e.getMessage();

            exec( request.getUri() );
            
        } catch ( Exception exc ) {
            // catch all exceptions and then bubble them up.
            log.error( "Caught exception: ", exc );
            throw exc;
        }
            
    }

}

