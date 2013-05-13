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
package peregrine.worker.clientd;

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
import peregrine.util.netty.*;
import peregrine.util.primitive.IntBytes;

import com.spinn3r.log5j.*;
import peregrine.worker.ErrorLoggingChannelUpstreamHandler;
import peregrine.worker.WorkerDaemon;

/**
 *
 * Handles interfacing with the tablet code and directly handling client requests
 * using the SSTableReader interface.
 * 
 */
public class BackendHandler extends ErrorLoggingChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    private static Pattern PATH_REGEX =
        Pattern.compile( "/([0-9]+)/client-rpc/(GET|SCAN|MUTATE)" );

    private Config config;

    private String resource;

    private Partition partition = null;

    private WorkerDaemon daemon;

    public BackendHandler(Config config, WorkerDaemon daemon, String resource) throws Exception {

        this.config = config;
        this.daemon = daemon;
        this.resource = resource;

        Matcher matcher = PATH_REGEX.matcher( resource );

        if ( matcher.find() ) {
            partition = new Partition( Integer.parseInt( matcher.group( 1 ) ) );
        } 

    }

    /**
     * Return true if we can handle the given resource URL we were given.
     */
    public boolean handles() {
        return partition != null;
    }

    /**
     * Execute the given request directly.
     */
    public void exec( final Channel channel ) throws IOException {

        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
        channel.write(response);

        // FIXME: return an Internal Server Error (or some other error) when
        // the queue of requests is full... the database is overloaded and we
        // can't handle your request right now.

        GetRequest request = GetRequestURLParser.toRequest( resource );

        // make a list of GetBackendRequests so that we can add them to the queue
        // to be dispatched.

        List<GetBackendRequest> getBackendRequests
                = new ArrayList<GetBackendRequest>( request.getKeys().size() );

        ClientRequest clientRequest = new ClientRequest(channel, partition, request.getSource() );

        for( StructReader key : request.getKeys() ) {

            GetBackendRequest getBackendRequest
                = new GetBackendRequest( clientRequest, key );

            getBackendRequests.add( getBackendRequest );

        }

        // we are done at this point.  the BackendRequestExecutor should now
        // handle serving all the keys.
        daemon.getBackendRequestQueue().add( getBackendRequests );

    }
    
    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        try {

            Object message = e.getMessage();

            exec( e.getChannel() );

        } catch ( Exception exc ) {
            // catch all exceptions and then bubble them up.
            log.error( "Caught exception: ", exc );
            throw exc;
        }
            
    }

}

