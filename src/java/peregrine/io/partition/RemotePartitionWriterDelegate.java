package peregrine.io.partition;

import java.io.*;
import java.util.*;
import java.net.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.pfs.*;

import com.spinn3r.log5j.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class RemotePartitionWriterDelegate extends BasePartitionWriterDelegate {

    public static int BUFFER_SIZE = 8192;

    private static final Logger log = Logger.getLogger();

    @Override
    public int append() throws IOException {
        throw new IOException( "not implemented yet" );
    }
    
    @Override
    public void erase() throws IOException {
        throw new IOException( "not implemented yet" );
    }

    @Override
    public OutputStream newChunkWriter( int chunk_id ) throws IOException {

        try {
            
            String local = Config.getPFSPath( partition, host, path );

            String chunk_name = LocalPartition.getFilenameForChunkID( chunk_id );
            String chunk_path = String.format( "/%s/%s%s/%s", host.getName(), partition.getId(), path, chunk_name ) ;

            URI uri = new URI( String.format( "http://%s:%s%s", host.getName() , host.getPort() , chunk_path ) );

            log.info( "Creating new chunk writer: %s" , uri );

            OutputStream out;

            out = new RemoteChunkWriterClient( uri );
            out = new BufferedOutputStream( out, BUFFER_SIZE );

            return out;
            
        } catch ( URISyntaxException e ) {
            throw new IOException( "Unable to create new chunk writer: " , e );
        }
            
    }

}