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
package peregrine.rpc;

import java.io.*;
import java.net.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.config.*;
import peregrine.http.*;

import com.spinn3r.log5j.*;

/**
 * Basic RPC client for sending messages to peregrine workers.
 * 
 */
public class Client {

    private static final Logger log = Logger.getLogger();

    private boolean trace = false;

    private Config config = null;
    
    public Client( Config config ) {
        this.config = config;
    }

    public Client( Config config , boolean trace ) {
        this.config= config;
        this.trace = trace;
    }

    /**
     * Invoke an RPC method and block.  Return a {@link Message} object which
     * represents the result and is parsed.
     */
    public Message invoke( Host host, String service, Message message ) throws IOException {

        HttpClient client = invokeAsync( host, service, message );
        client.close();

        ChannelBuffer content = client.getResult();
        
        int len = content.writerIndex();

        byte[] data = new byte[ len ];
        content.readBytes( data );
        
        return new Message( new String( data ) );
        
    }

    public HttpClient invokeAsync( Host host, String service, Message message ) throws IOException {

        try {

            String data = message.toString();

            if ( host == null )
                throw new NullPointerException( "host" );
            
            URI uri = new URI( String.format( "http://%s:%s/%s/RPC", host.getName(), host.getPort(), service ) );

            if( trace ) {
                log.info( "Sending RPC %s %s ..." , data, uri );
            }
            
            HttpClient client = new HttpClient( config, uri );

            client.setMethod( HttpMethod.POST );
            client.write( data.getBytes() );

            return client;
            
        } catch ( URISyntaxException e ) {
            throw new IOException( e );
        }
            
    }

}
