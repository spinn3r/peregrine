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
import peregrine.util.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * Encode GET request URLs.
 */
public class GetRequestURLEncoder extends RequestURLEncoder {

    //FIXME: both the client AND the server need CRC32 checksums.  I thought that
    //originally we were just going to include the SERVER responses but that's
    //silly.  A MUTATE operation to the server may corrupt the results.  Further,
    //fetching a key may result in corrupting the hashcode which means we would
    //end up fetching the WRONG key which would NOT be fun.  We can encode/decode
    //about 600MB/s on one more and the box can only really do 160MB/s and has
    //16 cores so CRC32 really isn't too onerous.

    //FIXME: if I move to POST to encode the request I no longer have to use
    //Base64 to encode the keys.  I can also use a CRC32 checksum.  Further. I
    //can (in the future) tell it which columns and column families I want to read
    //on a per item basis.

    //FIXME: include tracing information in both the client AND the server stream
    //response.  This we can read the keys from the server and then we can get
    //back a dapper-style trace of the request so that we can easily debug it on
    //a per-request basis.  I can build this into the DefaultChunkWriter because
    //it supports blocks at the end of the chunk which are just meta blocks.
    //This would have to be enabled via an X-trace HTTP header and the results
    //are included in the response.  Ideally we would be able to support this
    //without any additional overhead in the response.

    //FIXME: if we included authorization in the HTTP requests I could in theory
    //allow our customers to use the peregrine code directly.  For example I could
    //allow them to connect to the controller (with auth) and then download the
    //cluster configuration and then have the clients directly execute queries
    //in the cloud and fetch the results locally.  this would have the advantage
    //that I can make the new Spinn3r client use SCAN easily and they just keep
    //fetching next() except they're fetching from hundreds of nodes in parallel
    //and getting back the results and aggregating them.  In this situation it
    //would be nice to use something like snappy transfer encoding of the
    //results to save bandwidth.

    /**
     * Take a request and make it into a URL string to send to the server.
     */
    public String encode( Connection connection, GetRequest request ) {

        if ( connection == null )
            throw new NullPointerException( "connection" );

        if ( request == null )
            throw new NullPointerException( "request" );

        assertClientRequestMeta( request );

        StringBuilder buff = new StringBuilder( 200 );

        String partition = null;

        if ( request.getPartition() != null ) {
            partition = Integer.toString( request.getPartition().getId() );
        }

         buff.append( String.format( "%s/client-rpc/GET?source=%s",
                                     Strings.path( connection.getEndpoint(),
                                                   partition ),
                                     request.getSource()) );

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
