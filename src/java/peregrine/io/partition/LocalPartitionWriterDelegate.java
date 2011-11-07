package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.io.chunk.*;

import com.spinn3r.log5j.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class LocalPartitionWriterDelegate extends BasePartitionWriterDelegate {

    private static final Logger log = Logger.getLogger();

    @Override
    public String toString() {
        return path;
    }

    // PartitionWriterDelegate

    @Override
    public int append() throws IOException {

        List<File> chunks = LocalPartition.getChunkFiles( config, partition, path );

        return chunks.size();

    }
    
    @Override
    public void erase() throws IOException {

        List<File> chunks = LocalPartition.getChunkFiles( config, partition, path );

        for ( File chunk : chunks ) {
            
            if ( ! chunk.delete() )
                throw new IOException( "Unable to remove local chunk: " + chunk );
            
        }

    }

    @Override
    public OutputStream newChunkWriter( int chunk_id ) throws IOException {
    	
    	File file = LocalPartition.getChunkFile( config, partition, path, chunk_id );

        log.info( "Creating new local chunk writer: %s" , file );

        return DefaultChunkWriter.getOutputStream( file );
        
    }

}