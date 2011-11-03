package peregrine.io.partition;

import java.io.*;
import java.util.*;
import java.net.*;

import peregrine.pfs.*;

import com.spinn3r.log5j.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class RemotePartitionWriterDelegate extends BasePartitionWriterDelegate {

    private static final Logger log = Logger.getLogger();

    @Override
    public int append() throws IOException {

        Map map = request( "HEAD" );
        
        int chunks = readHeader( map, "X-nr-chunks" );

        return chunks;
        
    }
    
    @Override
    public void erase() throws IOException {

        try {
            Map map = request( "DELETE" );
            
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
    private Map request( String method ) throws IOException {

        //FIXME: infinite read connect, DNS timeouts, etc.

        //FIXME: we should ALWAYS use netty as using TWO HTTP libraries is NOT a
        //good idea and will just lead to problems.  I just need to extend netty
        //so that I can perform synchronous HTTP requests.  
        
        URL url = new URL( String.format( "http://%s:%s%s", host.getName(), host.getPort(), path ) );

        log.info( "%s: %s", url, method );

        HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
        httpCon.setRequestMethod( method );
        int response = httpCon.getResponseCode();

        if ( response != 200 ) {
            throw new RemoteRequestException( response );
        }

        return httpCon.getHeaderFields();

    }

    private int readHeader( Map headers, String name ) throws IOException {

        String val = headers.get( name ).toString();
        
        if ( val == null ) {
            throw new IOException( "HTTP response header not specified: " + name );
        }

        return Integer.parseInt( val );
        
    }

    @Override
    public OutputStream newChunkWriter( int chunk_id ) throws IOException {

        try {
            
            String chunk_name = LocalPartition.getFilenameForChunkID( chunk_id );
            String chunk_path = String.format( "/%s%s/%s", partition.getId(), path, chunk_name ) ;

            URI uri = new URI( String.format( "http://%s:%s%s", host.getName() , host.getPort() , chunk_path ) );

            log.info( "Creating new chunk writer: %s" , uri );

            return new RemoteChunkWriterClient( uri );
            
        } catch ( URISyntaxException e ) {
            throw new IOException( "Unable to create new chunk writer: " , e );
        }
            
    }

}

class RemoteRequestException extends IOException {

    public int status;
    
    public RemoteRequestException( int status ) {
        super( "HTTP request failed: " + status );
        this.status = status;
    }
    
}