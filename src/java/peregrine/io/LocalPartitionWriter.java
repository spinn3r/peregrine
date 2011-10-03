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

    public static long CHUNK_SIZE = 100000000;
    
    private String path = null;

    private int chunk_id = 0;

    private ChunkWriter out = null;

    public LocalPartitionWriter( String path ) throws IOException {

        this.path = path;

        List<File> chunks =
            LocalPartition.getChunkFiles( path );

        for ( File chunk : chunks ) {

            if ( ! chunk.delete() )
                throw new IOException( "Unable to remove local chunk: " + chunk );
            
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
        String chunk_path = String.format( "%s/%s" , this.path, chunk_name );

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