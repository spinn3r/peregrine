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

import peregrine.client.*;
import peregrine.config.*;

import com.spinn3r.log5j.*;
import peregrine.worker.ErrorLoggingChannelUpstreamHandler;
import peregrine.worker.WorkerDaemon;
import peregrine.worker.clientd.requests.*;
import peregrine.worker.clientd.requests.ClientBackendRequest;

/**
 *
 * Handles interfacing with the tablet code and directly handling client requests
 * using the SSTableReader interface.
 * 
 */
public class BackendHandler extends ErrorLoggingChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    private String resource;

    private WorkerDaemon daemon;

    private ClientRequestMeta clientRequestMeta = null;

    public BackendHandler(WorkerDaemon daemon, String resource) throws Exception {

        this.daemon = daemon;
        this.resource = resource;

        clientRequestMeta = new ClientRequestMeta();

        clientRequestMeta.parse( resource );

    }

    /**
     * Return true if we can handle the given resource URL we were given.
     */
    public boolean handles() {
        return clientRequestMeta.getPartition() != null;
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

        ClientBackendRequest clientBackendRequest =
                new ClientBackendRequest( channel,
                                          clientRequestMeta.getPartition(),
                                          clientRequestMeta.getSource() );

        BackendRequestFactory backendRequestFactory = null;

        //FIXME: we should put the getBackendRequest method behind an interface
        // and have an object returned which implements that interface. This
        // would cleanup the following lines because we don't have to have this
        // GET switch.  I could also use this to get the size I think and remove
        // the ugly size() code that I hate.

        // TODO: migrate this to JDK 1.7 string switch.
        if ( "GET".equals( clientRequestMeta.getRequestType() ) ) {

            GetRequest request = GetRequestURLParser.toRequest(resource);
            backendRequestFactory = new GetBackendRequestFactory( request );

        } else if ( "SCAN".equals( clientRequestMeta.getRequestType() ) ) {

            //FIXME: we need to handle SCAN here too


        } else {
            throw new RuntimeException( "Unknown request type: " + clientRequestMeta.getRequestType() );
        }

        if ( queue.isExhausted( backendRequestFactory.size() ) ) {
            // our queue is exhausted which probably means we're at a high
            // system load.
            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, SERVICE_UNAVAILABLE );
            channel.write(response);
            return;
        }

        // make a list of GetBackendRequests so that we can add them to the queue
        // to be dispatched.

        List<BackendRequest> backendRequests = backendRequestFactory.getBackendRequests(clientBackendRequest);

        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
        response.setHeader( HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED );
        channel.write(response);

        // we are done at this point.  the BackendRequestExecutor should now
        // handle serving all the keys.
        queue.add( backendRequests );

    }
    
    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        try {

            exec( e.getChannel() );

        } catch ( Exception exc ) {
            // catch all exceptions and then bubble them up.
            log.error( "Caught exception: ", exc );
            throw exc;
        }
            
    }

}

