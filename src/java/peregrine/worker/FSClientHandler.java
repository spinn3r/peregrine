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
import java.util.regex.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.io.chunk.*;
import peregrine.util.primitive.IntBytes;
import peregrine.shuffle.receiver.*;

import com.spinn3r.log5j.*;

/**
 * <p>
 * Handles HTTP requests for client API messages.  This includes GET, PUT, and
 * SCAN requests.
 * 
 * <p>
 * This handler accepts two main request types.  The first is a system that
 * introspects client requests directly via a client endpoint.  Any worker daemon
 * can act as a client endpoint.  Normal production use is to put workers behind a
 * proxy server so clients can connect to any of them.
 *
 * <p>The main API requests come in as:
 *
 * <p>
 * <code>/client-rpc/GET?path=/pr/out/test&key=KEY1&key=KEY2</code>
 *
 * <p> This is a client endpoint query and does not specify a partition to run
 * the requests against.  The worker will then take that request and determine
 * which partition servers host those keys and push this request down to those
 * hosts.  Any extra hosts are NOT involved in this process.
 * 
 * <p>
 * The worker then sends requests to the partition servers as:
 * 
 * <p><code>/0/client-rpc/GET?path=/pr/out/test&key=KEY1&key=KEY2</code>
 * 
 * <p> This request is prefixed with the partition involved in solving this
 * query.  The worker then writes the response as a set of key/value pairs with
 * minimal encoding.  The client endpoint then aggregates these packets and
 * returns them directly to the client.
 * 
 * <p> This way we can have clients easily support workers coming and going in
 * the cluster and we can also handle transparently supporting client fan-out
 * where we involve ALL workers hosting these keys in parallel.
 * 
 */
public class FSClientHandler extends ErrorLoggingChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    private static Pattern PATH_REGEX =
        Pattern.compile( "/([0-9]+)/client-rpc/(GET|SCAN|MUTATE)" );

    public FSClientHandler( FSDaemon daemon, FSHandler handler ) throws Exception {

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

