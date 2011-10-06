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
public class DefaultChunkWriter implements ChunkWriter {

    private VarintWriter varintWriter = new VarintWriter();

    private OutputStream out = null;

    private int count = 0;

    protected long length = 0;

    private boolean closed = false;

    public DefaultChunkWriter( OutputStream out ) throws IOException {
        this.out = out;
    }

    public void write( byte[] key, byte[] value )
        throws IOException {

        if ( closed )
            throw new IOException( "DefaultChunkWriter is closed" );
        
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