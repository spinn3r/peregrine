package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * Export chunks are used in both the Extract phase of ETL jobs with
 * ExtractWriter to write data to individual partitions and chunks AND with map
 * jobs so that we can spool data to disk in the from the mappers directly to
 * the R partition files BEFORE we sort the output so that we can perform reduce
 * on each (K,V...) pair.
 */
public class LocalChunkWriter implements ChunkWriter {

    public static int BUFFER_SIZE = 16384;

    public static boolean USE_ASYNC = true;

    private DefaultChunkWriter delegate = null;
    
    public LocalChunkWriter( String path ) throws IOException {

        // make sure the parent directories exist.
        new File( new File( path ).getParent() ).mkdirs();

        OutputStream out;
        
        if ( USE_ASYNC )
            out = new AsyncOutputStream( path );
        else 
            out = new FileOutputStream( path );
        
        out = new BufferedOutputStream( out, BUFFER_SIZE );

        this.delegate = new DefaultChunkWriter( out );
        
    }

    public void write( byte[] key, byte[] value ) throws IOException {
        delegate.write( key, value );        
    }

    public int count() throws IOException {
        return delegate.count();
    }
    
    public void close() throws IOException {
        delegate.close();
    }

    public long length() throws IOException {
        return delegate.length();
    }
    
}