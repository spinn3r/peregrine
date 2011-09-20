package maprunner;

import java.io.*;
import java.util.*;

import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class PartitionWriter {

    public static long CHUNK_SIZE = 1000000;
    //public static long CHUNK_SIZE = 1000000000;
    
    private String path = null;

    private int chunk_id = 0;

    private ChunkWriter out = null;
    
    public PartitionWriter( String path ) throws IOException {

        this.path = path;

        //create the first chunk...
        rollover();
        
    }

    public void write( byte[] key_bytes, byte[] value_bytes )
        throws IOException {

        out.write( key_bytes, value_bytes );

        rolloverWhenNecessary();
        
    }

    private void rolloverWhenNecessary() throws IOException {

        if ( out.size > CHUNK_SIZE )
            rollover();
        
    }
    
    private void rollover() throws IOException {

        if ( out != null )
            out.close();
        
        String chunk_path = String.format( "%s/chunk%06d.dat" , this.path, this.chunk_id );

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