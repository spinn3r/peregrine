package peregrine.io.chunk;

import java.io.*;
import java.util.*;
import java.nio.*;

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

    public static int BUFFER_SIZE = 8192;
    
    private VarintWriter varintWriter = new VarintWriter();

    private OutputStream out = null;

    private int count = 0;

    protected long length = 0;

    private boolean closed = false;

    private ByteBuffer buff;
    
    public DefaultChunkWriter( OutputStream out ) throws IOException {
        this.out = out;

        this.buff = ByteBuffer.allocate( BUFFER_SIZE );
        this.buff.mark();

    }

    @Override
    public void write( byte[] key, byte[] value )
        throws IOException {

        if ( closed )
            throw new IOException( "closed" );

        // the max write width ... the key length and value length and the max
        // potential value of both keys.

        int write_width = key.length + value.length + 8;

        if ( buff.position() + write_width >= buff.limit() ) {
            flushByteBuffer();
        }

        varintWriter.write( buff, key.length );
        buff.put( key );

        varintWriter.write( buff, value.length );
        buff.put( value );

        ++count;

    }

    private void flushByteBuffer() throws IOException {

        length += buff.position();
        
        write( buff.array() );
        buff.reset();

    }
    
    private void write( byte[] data ) throws IOException {

        out.write( data );
        length += data.length;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;

        flushByteBuffer();

        // last four bytes store the number of items.
        write( IntBytes.toByteArray( count ) );
        out.close();

        closed = true;
        
    }
    
}