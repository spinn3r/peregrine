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

    public static int BUFFER_SIZE = 16384;
    
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

        byte[] backing = buff.array();
        int len = buff.position();
        
        byte[] result = new byte[ len ];
        System.arraycopy( backing, 0, result, 0, len );
        
        out.write( result );

        buff.reset();

    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public long length() {
        return length + buff.position();
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;

        flushByteBuffer();

        // last four bytes store the number of items.

        byte[] count_bytes = IntBytes.toByteArray( count );
        out.write( count_bytes );
        length += count_bytes.length;
        
        out.close();

        closed = true;
        
    }
    
}