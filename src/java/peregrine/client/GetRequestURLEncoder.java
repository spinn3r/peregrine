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

import peregrine.StructReader;
import peregrine.StructReaders;
import peregrine.util.Base64;

import java.util.ArrayList;
import java.util.List;

/**
 * Encode GET request URLs.
 */
public class GetRequestURLEncoder extends RequestURLEncoder {

    /**
     * Take a request and make it into a URL string to send to the server.
     */
    public String encode( Connection connection, GetRequest request ) {

        assertClientRequestMeta( request.getClientRequestMeta() );

        StringBuilder buff = new StringBuilder( 200 );
        buff.append( String.format("%s/%s/client-rpc/GET?source=%s",
                connection.getEndpoint(),
                request.getClientRequestMeta().getPartition().getId(),
                request.getClientRequestMeta().getSource()) );

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
     * Return the list of keys as a list of hashcodes.
     */
    public static List<StructReader> hashcodes( List<StructReader> keys ) {

        List<StructReader> result = new ArrayList( keys.size() );

        for( StructReader key : keys ) {
            result.add( StructReaders.hashcode( key.toByteArray() ) );
        }

        return result;

    }

}
