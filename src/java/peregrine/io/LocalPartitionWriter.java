package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class LocalPartitionWriter {

    /**
     * Chunk size for rollover files.
     */
    public static long CHUNK_SIZE = 134217728;
    
    private String path = null;

    private int chunk_id = 0;

    private LocalChunkWriter chunkWriter = null;

    public LocalPartitionWriter( Partition partition,
                                 Host host,
                                 String path ) throws IOException {
        this( partition, host, path, false );
    }
        
    public LocalPartitionWriter( Partition partition,
                                 Host host,
                                 String local,
                                 boolean append ) throws IOException {

        this.path = Config.getPFSPath( partition, host, local );

        List<File> chunks = LocalPartition.getChunkFiles( partition, host, local );
        
        if ( append == false ) {
            
            for ( File chunk : chunks ) {
                
                if ( ! chunk.delete() )
                    throw new IOException( "Unable to remove local chunk: " + chunk );
                
            }

        } else {

            // the chunk_id needs to be changed so that the append works.
            chunk_id = chunks.size();

        }
        
        //create the first chunk...
        rollover();
        
    }

    public void write( byte[] key_bytes, byte[] value_bytes )
        throws IOException {

        chunkWriter.write( key_bytes, value_bytes );

        rolloverWhenNecessary();
        
    }

    private void rolloverWhenNecessary() throws IOException {

        if ( chunkWriter.length() > CHUNK_SIZE )
            rollover();
        
    }
    
    private void rollover() throws IOException {

        if ( chunkWriter != null )
            chunkWriter.close();

        String chunk_name = LocalPartition.getFilenameForChunkID( this.chunk_id );
        String chunk_path = new File( this.path, chunk_name ).getPath();

        chunkWriter = new LocalChunkWriter( chunk_path );
        
        ++chunk_id; // change the chunk ID now for the next file.
        
    }

    public void close() throws IOException {
        //close the last opened partition...
        chunkWriter.close();        
    }

    public String toString() {
        return path;
    }
    
}