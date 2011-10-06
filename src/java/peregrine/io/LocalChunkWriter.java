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

    public static boolean USE_ASYNC = true;
    
    public static int BUFFER_SIZE = 16384;

    private String path = null;

    private VarintWriter varintWriter = new VarintWriter();

    private OutputStream out = null;

    private int count = 0;

    protected long length = 0;

    private boolean closed = false;
    
    public LocalChunkWriter( String path ) throws IOException {

        this.path = path;

        // make sure the parent directories exist.
        new File( new File( this.path ).getParent() ).mkdirs();

        if ( USE_ASYNC )
            this.out = new AsyncOutputStream( this.path );
        else 
            this.out = new FileOutputStream( this.path );
        
        this.out = new BufferedOutputStream( this.out, BUFFER_SIZE );
        
    }

    public LocalChunkWriter( OutputStream out ) throws IOException {
        this.out = out;
    }

    public void write( byte[] key, byte[] value )
        throws IOException {

        if ( closed )
            throw new IOException( "LocalChunkWriter is closed" );
        
        write( varintWriter.write( key.length ) );
        write( key );

        write( varintWriter.write( value.length ) );
        write( value );

        ++count;

    }

    private void write( byte[] data ) throws IOException {

        out.write( data );
        length += data.length;
    }

    public int count() {
        return count;
    }

    public long length() {
        return length;
    }
    
    public void close() throws IOException {

        if ( closed )
            return;

        // last four bytes store the number of items.
        out.write( IntBytes.toByteArray( count ) );
        out.close();

        closed = true;
        
    }
    
}