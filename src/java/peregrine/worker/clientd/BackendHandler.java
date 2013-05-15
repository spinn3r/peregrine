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
package peregrine.worker.clientd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.client.*;
import peregrine.config.*;

import com.spinn3r.log5j.*;
import peregrine.worker.ErrorLoggingChannelUpstreamHandler;
import peregrine.worker.WorkerDaemon;
import peregrine.worker.clientd.requests.*;
import peregrine.worker.clientd.requests.GetBackendRequest;
import peregrine.worker.clientd.requests.ClientRequest;

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

    private String resource;

    private Partition partition;

    private WorkerDaemon daemon;

    private String requestType;

    public BackendHandler(WorkerDaemon daemon, String resource) throws Exception {

        this.daemon = daemon;
        this.resource = resource;

        Matcher matcher = PATH_REGEX.matcher( resource );

        if ( matcher.find() ) {
            partition = new Partition( Integer.parseInt( matcher.group( 1 ) ) );
            requestType = matcher.group(2);
        } 

    }

    /**
     * Return true if we can handle the given resource URL we were given.
     */
    public boolean handles() {
        return partition != null;
    }


    private List<BackendRequest> getBackendRequests( Channel channel, GetRequest request ) {

        List<BackendRequest> backendRequests
                = new ArrayList<BackendRequest>( request.getKeys().size() );

        ClientRequest clientRequest = new ClientRequest(channel, partition, request.getSource() );

        for( StructReader key : request.getKeys() ) {

            GetBackendRequest backendRequest
                    = new GetBackendRequest( clientRequest, key );

            backendRequests.add( backendRequest );

        }

        return backendRequests;

    }

    private List<BackendRequest> getBackendRequests( Channel channel, ScanRequest request ) {

        ClientRequest clientRequest = new ClientRequest(channel, partition, request.getSource() );

        ScanBackendRequest scanBackendRequest = new ScanBackendRequest( clientRequest, request );

        List<BackendRequest> backendRequests
                = new ArrayList<BackendRequest>( 1 );

        backendRequests.add( scanBackendRequest );

        return backendRequests;

    }

    /**
     * Execute the given request directly.
     */
    public void exec( final Channel channel ) throws IOException {

        // A client COULD in theory send us 50001 keys which itself would
        // exhaust he queue.  We should first decode the request and see if
        // it PLUS the current capacity would exhaust the queue.  This way we
        // don't accept a request for too many keys from one client.

        BackendRequestQueue queue = daemon.getBackendRequestQueue();

        boolean exhausted = false;
        List<BackendRequest> backendRequests = null;

        // TODO: migrate this to JDK 1.7 string switch.
        if ( "GET".equals( requestType ) ) {

            GetRequest request = GetRequestURLParser.toRequest(resource);
            exhausted = queue.isExhausted( queue.size( request ) );
            backendRequests = getBackendRequests( channel, request );

        } else if ( "SCAN".equals( requestType ) ) {

            //FIXME: we need to handle SCAN here too


        } else {
            throw new RuntimeException( "Unknown request type: " + requestType );
        }

        if ( exhausted ) {
            // our queue is exhausted which probably means we're at a high
            // system load.
            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, SERVICE_UNAVAILABLE );
            channel.write(response);
            return;
        }

        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
        response.setHeader( HttpHeaders.Names.TRANSFER_ENCODING, "chunked" );
        channel.write(response);

        // make a list of GetBackendRequests so that we can add them to the queue
        // to be dispatched.


        // we are done at this point.  the BackendRequestExecutor should now
        // handle serving all the keys.
        queue.add( backendRequests );

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

