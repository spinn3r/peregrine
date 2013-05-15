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
public class GetRequestURLDecoder {

    /**
     * Take a request URL and return it as a Get request representing all fields
     * necessary to execute the request.  Return null if we are unable to parse
     * the URL.
     */
    public GetRequest decode( String url ) {

        GetRequest request = new GetRequest();

        ClientRequestMeta clientRequestMeta = new ClientRequestMeta();

        if ( ! clientRequestMeta.parse( url ) ) {
            return null;
        }

        QueryStringDecoder decoder = new QueryStringDecoder( url );

        List<StructReader> keys = new ArrayList();

        for( String key : decoder.getParameters().get( "k" ).get( 0 ).split( "," ) ) {

            byte[] data = Base64.decode(key);
            keys.add( StructReaders.wrap(data) );

        }

        request.setKeys(keys);
        request.setClientRequestMeta( clientRequestMeta );

        return request;

    }

}