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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Decode scan request URLs.
 */
public class ScanRequestURLDecoder {

    private Map<String,List<String>> params = null;

    private ScanRequest request = new ScanRequest();

    /**
     * Take a request URL and return it as a Get request representing all fields
     * necessary to execute the request.  Return null if we are unable to parse
     * the URL.
     */
    public ScanRequest decode( String url ) {

        ClientRequestMeta clientRequestMeta = new ClientRequestMeta();
        request.setClientRequestMeta( clientRequestMeta );

        if ( ! clientRequestMeta.parse( url ) ) {
            return null;
        }

        QueryStringDecoder decoder = new QueryStringDecoder( url );

        List<StructReader> keys = new ArrayList();

        params = decoder.getParameters();

        if ( params.containsKey( "start.key" ) )
            request.setStart( getBound( "start" ));

        if ( params.containsKey( "end.key" ) )
            request.setEnd( getBound( "end" ));

        request.setLimit( Integer.parseInt( params.get( "limit" ).get( 0 ) ) );

        return request;

    }

    private ScanRequest.Bound getBound( String prefix ) {

        String key = params.get(prefix + ".key").get(0);
        boolean inclusive = "true".equals(params.get(prefix + ".inclusive").get(0));

        return request.newBound( StructReaders.wrap(Base64.decode(key)), inclusive );

    }

}
