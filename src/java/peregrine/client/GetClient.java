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
 * Run a get request for the given keys.  Clients specify a connection. Then
 * create a Get object, set the keys they wish to fetch, then call exec() and
 * then waitFor() to get the results back.  
 */
public class GetClient {

    private List<Record> records = new ArrayList();

    private Connection connection;

    private Config config;

    private HttpClient client = null;
    
    public GetClient( Config config, Connection connection ) {
        this.config = config;
        this.connection = connection;
    }

    /**
     * Execute the get request. 
     */
    public void exec( GetRequest request ) throws IOException {

        // create an HTTP client and submit the request.

        String url = GetRequestURLParser.toURL( this, request );

        client = new HttpClient( config, url );
        client.setMethod( HttpMethod.GET );

        client.open();

    }

    /**
     * Wait for the request to complete and for us to read all keys.
     */
    public void waitFor() throws IOException {

        // - stick it in a chunk reader
        // - parse it into key/value pairs.
        //

        // read the data
        client.close();
        
        ChannelBuffer buff = client.getResult();

        DefaultChunkReader reader = new DefaultChunkReader( buff );

        while( reader.hasNext() ) {
            reader.next();
            records.add( new Record( reader.key(), reader.value() ) );
        }
        
        reader.close();

    }

    /**
     * Get all records for this request.
     */
    public List<Record> getRecords() {
        return records;
    }

    public Connection getConnection() {
        return connection;
    }
    
}