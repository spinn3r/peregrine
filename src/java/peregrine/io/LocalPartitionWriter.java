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

    private ChunkWriter out = null;

    public LocalPartitionWriter( String path ) throws IOException {
        this( path, false );
    }
        
    public LocalPartitionWriter( String path, boolean append ) throws IOException {

        this.path = path;

        List<File> chunks = LocalPartition.getChunkFiles( path );

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

        out.write( key_bytes, value_bytes );

        rolloverWhenNecessary();
        
    }

    private void rolloverWhenNecessary() throws IOException {

        if ( out.length > CHUNK_SIZE )
            rollover();
        
    }
    
    private void rollover() throws IOException {

        if ( out != null )
            out.close();

        String chunk_name = LocalPartition.getFilenameForChunkID( this.chunk_id );
        String chunk_path = new File( this.path, chunk_name ).getPath();

        out = new ChunkWriter( chunk_path );
        
        ++chunk_id; // change the chunk ID now for the next file.
        
    }

    public void close() throws IOException {
        //close the last opened partition...
        out.close();        
    }

    public String toString() {
        return path;
    }
    
}