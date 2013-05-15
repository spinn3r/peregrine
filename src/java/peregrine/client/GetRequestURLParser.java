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
package peregrine.client;

import java.util.*;
import java.io.*;
import java.util.regex.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.http.*;
import peregrine.io.chunk.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * Parse URLs into and from requests for use between the client and server.
 */
public class GetRequestURLParser {

    /**
     * Return the list of keys as a list of hashcodes.
     */
    public static List<StructReader> hashcodes( List<StructReader> keys ) {

        List<StructReader> result = new ArrayList( keys.size() );

        for( StructReader key : keys ) {
            result.add( StructReaders.hashcode( key.toByteArray() ) );
        }

        return result;
        
    }
    
    /**
     * Take a request and make it into a URL string to send to the server.
     */
    public static String toURL( GetClient client, GetRequest request ) {

        if ( request.getSource() == null )
            throw new NullPointerException( "source" );
        
        StringBuilder buff = new StringBuilder( 200 );
        buff.append( String.format( "%s/client-rpc/GET?source=%s", client.getConnection().getEndpoint(), request.getSource() ) );

        List<String> args = new ArrayList();

        // add comma separated base64 encoded keys.
        buff.append( "&k=" );

        List<StructReader> keys = request.getKeys();

        if ( request.getHashcode() ) {
            keys = hashcodes( keys );
        }
        
        for( int i = 0; i < keys.size(); ++i ) {

            StructReader key = keys.get( i );

            if ( i > 0 )
                buff.append( "," );
            
            buff.append( Base64.encode( key.toByteArray() ) );

        }

        return buff.toString();

    }

    /**
     * Take a request URL and return it as a Get request representing all fields
     * necessary to execute the request.
     */
    public static GetRequest toRequest( String url ) {

        GetRequest request = new GetRequest();

        QueryStringDecoder decoder = new QueryStringDecoder( url );

        List<StructReader> keys = new ArrayList();
        
        for( String key : decoder.getParameters().get( "k" ).get( 0 ).split( "," ) ) {

            byte[] data = Base64.decode( key );
            keys.add( StructReaders.wrap( data ) );

        }

        String source = decoder.getParameters().get( "source" ).get( 0 );

        request.setKeys( keys );
        request.setSource( source );
        
        return request;
        
    }
    
}