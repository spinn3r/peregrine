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
package peregrine.io.partition;

import java.io.*;
import java.util.*;
import java.net.*;

import peregrine.config.*;
import peregrine.http.*;
import peregrine.util.netty.*;

import com.spinn3r.log5j.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class RemotePartitionWriterDelegate extends BasePartitionWriterDelegate {

    private static final Logger log = Logger.getLogger();

    public RemotePartitionWriterDelegate( Config config, boolean autoSync ) {

    }

    @Override
    public int append() throws IOException {

        Map stat = stat();
        
        int chunks = readHeader( stat, "X-nr-chunks" );

        return chunks;
        
    }

    public Map<String,List<String>> stat() throws IOException {
        return request( "HEAD" );
    }
    
    @Override
    public void erase() throws IOException {

        try {
            
            Map<String,List<String>> map = request( "DELETE" );
            
            readHeader( map, "X-deleted" );
            
            log.info( "Deleted %,d chunks on host: %s", host );

        } catch ( RemoteRequestException e ) {

            // 404 is ok as this would be a new file.
            if ( e.status != 404 )
                throw e;
            
        }
        
    }

    /**
     * Perform an HTTP request.  Note that it's ok that this is done synchronous
     * as we have to wait for the results ANYWAY before moving forward AND this
     * only happens when writing to a new partition.
     */
    private Map<String,List<String>> request( String method ) throws IOException {

        //FIXME: we should ALWAYS use netty as using TWO HTTP libraries is NOT a
        //good idea and will just lead to problems.  I just need to extend netty
        //so that I can perform synchronous HTTP requests.  
        
        URL url = new URL( String.format( "http://%s:%s/%s%s",
                                          host.getName(),
                                          host.getPort(),
                                          partition.getId(),
                                          path ) );

        log.info( "%s: %s", url, method );

        HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
        httpCon.setRequestMethod( method );
        httpCon.setConnectTimeout( HttpClient.WRITE_TIMEOUT );
        httpCon.setReadTimeout( HttpClient.WRITE_TIMEOUT );
        int response = httpCon.getResponseCode();

        if ( response != 200 ) {
            throw new RemoteRequestException( method, url, response );
        }

        return httpCon.getHeaderFields();

    }

    private int readHeader( Map<String,List<String>> map,
                            String name ) throws IOException {

        List<String> headers = map.get( name );

        String header = null;

        if ( headers != null && headers.size() == 1 ) {
            header = headers.get( 0 );
        }

        if ( header == null ) {
            throw new IOException( "Invalid HTTP response header: " + name + " for " + headers);
        }

        return Integer.parseInt( header );
        
    }

    @Override
    public ChannelBufferWritable newChunkWriter( int chunk_id ) throws IOException {

        try {
            
            String chunk_name = LocalPartition.getFilenameForChunkID( chunk_id );
            String chunk_path = String.format( "/%s%s/%s", partition.getId(), path, chunk_name ) ;

            URI uri = new URI( String.format( "http://%s:%s%s",
                                              host.getName() ,
                                              host.getPort() ,
                                              chunk_path ) );

            log.info( "Creating new chunk writer: %s" , uri );

            return new HttpClient( uri );
            
        } catch ( URISyntaxException e ) {
            throw new IOException( "Unable to create new chunk writer: " , e );
        }
            
    }

}

class RemoteRequestException extends IOException {

    public int status;
    
    public RemoteRequestException( String method, URL url, int status ) {
        super( String.format( "HTTP request failed %s %s (%s)", method, url, status ) );
        this.status = status;
    }
    
}
