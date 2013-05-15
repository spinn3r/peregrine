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

import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import peregrine.StructReader;
import peregrine.StructReaders;
import peregrine.util.Base64;

import java.util.*;

/**
 * Parse out scan requests.
 */
public class ScanRequestURLEncoder {

    /**
     * Take a request and make it into a URL string to send to the server.
     */
    public String encode( Connection connection, ScanRequest scanRequest ) {

        //FIXME: this code is shared with GetRequestURLDecoder and we also need
        //to assert that we have a partition.
        if ( scanRequest.getClientRequestMeta().getSource() == null )
            throw new NullPointerException( "source" );

        //FIXME: support hashcoding the keys.

        StringBuilder buff = new StringBuilder( 200 );
        buff.append( String.format( "%s/%s/client-rpc/SCAN?",
                     connection.getEndpoint(), scanRequest.getClientRequestMeta().getPartition().getId() ) );

        Map<String,Object> params = new TreeMap();

        if ( scanRequest.getStart() != null ) {
            params.put( "start.key", Base64.encode( scanRequest.getStart().key().toByteArray() ) );
            params.put( "start.inclusive", scanRequest.getStart().isInclusive() );
        }

        if ( scanRequest.getEnd() != null ) {
            params.put( "end.key", Base64.encode( scanRequest.getEnd().key().toByteArray() ) );
            params.put( "end.inclusive", scanRequest.getEnd().isInclusive() );
        }

        params.put( "limit",  scanRequest.getLimit() );
        params.put( "source", scanRequest.getClientRequestMeta().getSource() );

        int i = 0;
        for( Map.Entry<String,Object> entry : params.entrySet() ) {

            if ( i > 0 )
                buff.append( "&" );

            buff.append( String.format( "%s=%s", entry.getKey(), entry.getValue() ) );

            ++i;
        }

        return buff.toString();

    }

}
