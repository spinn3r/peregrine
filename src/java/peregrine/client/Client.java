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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import peregrine.Record;
import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.http.HttpClient;
import peregrine.io.chunk.DefaultChunkReader;
import peregrine.io.util.Closer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Client interface which represents the functionality that each client must
 * implement.
 */
public class Client {

    protected Connection connection;

    protected Config config;

    protected HttpClient client = null;

    protected List<Record> records = new ArrayList();

    protected Client(Config config, Connection connection) {
        this.config = config;
        this.connection = connection;
    }

    protected void exec( String url ) throws IOException {

        client = new HttpClient( config, url );
        client.setMethod( HttpMethod.GET );

        client.open();

    }

    /**
     * Wait for the server to respond.
     */
    public void waitForResponse() throws IOException {

        // read the data
        client.close();

    }

    /**
     * Wait for the request to complete and for us to read all keys.
     */
    public void waitFor() throws IOException {

        waitForResponse();

        ChannelBuffer buff = client.getResult();

        // make sure to get the HTTP status code and make sure that
        // we have HTTP 200 OK.
        if ( ! client.getResponse().getStatus().equals( HttpResponseStatus.OK ) ) {
            throw new IOException( "Client request failed: " + client.getResponse().getStatus() );
        }

        // stick the results in a chunk reader to read the output.
        DefaultChunkReader reader = null;

        try {

            reader = new DefaultChunkReader( buff );

            // parse it into key/value pairs.
            while( reader.hasNext() ) {
                reader.next();
                records.add( new Record( reader.key(), reader.value() ) );
            }

        } finally {
            new Closer( reader ).close();
        }

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
