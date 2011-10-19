package peregrine.io.chunk;

import java.io.*;
import java.util.*;
import java.nio.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

import org.jboss.netty.buffer.*;

/**
 * Export chunks are used in both the Extract phase of ETL jobs with
 * ExtractWriter to write data to individual partitions and chunks AND with map
 * jobs so that we can spool data to disk in the from the mappers directly to
 * the R partition files BEFORE we sort the output so that we can perform reduce
 * on each (K,V...) pair.
 */
public class DefaultChunkWriter implements ChunkWriter {

    public static boolean USE_ASYNC = true;

    public static int BUFFER_SIZE = 16384;
    
    private static VarintWriter varintWriter = new VarintWriter();

    protected OutputStream out = null;

    private int count = 0;

    protected long length = 0;

    private boolean closed = false;

    //FIXME: this should be thread local 
    private ChannelBuffer buff = ChannelBuffers.buffer( BUFFER_SIZE );

    public DefaultChunkWriter( OutputStream out ) throws IOException {
        this.out = out;
    }

    public DefaultChunkWriter( File file ) throws IOException {

        this.out = getOutputStream( file.getPath() );
        
    }

    public static OutputStream getOutputStream( String path ) throws IOException {

        OutputStream out;
        
        if ( USE_ASYNC )
            out = new AsyncOutputStream( path );
        else 
            out = new FileOutputStream( path );

        return out;
        
    }
    
    @Override
    public void write( byte[] key, byte[] value )
        throws IOException {

        if ( closed )
            throw new IOException( "closed" );

        // the max write width ... the key length and value length and the max
        // potential value of both keys.

        int write_width = key.length + value.length + 8;

        if ( buff.writerIndex() + write_width >= buff.capacity() ) {
            flushChannelBuffer();
        }

        write( buff, key, value );
        
        ++count;

    }

    /**
     * Perform a DIRECT write on a ChannelBuffer.  
     */
    public static void write( ChannelBuffer buff,
                              byte[] key,
                              byte[] value ) {
        
        varintWriter.write( buff, key.length );
        buff.writeBytes( key );

        varintWriter.write( buff, value.length );
        buff.writeBytes( value );
        
    }
    
    private void flushChannelBuffer() throws IOException {

        length += buff.writerIndex();

        byte[] backing = buff.array();
        int len = buff.writerIndex();
        
        byte[] result = new byte[ len ];
        System.arraycopy( backing, 0, result, 0, len );
        
        out.write( result );

        buff.resetReaderIndex();
        buff.resetWriterIndex();

    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public long length() {
        return length + buff.writerIndex();
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;

        flushChannelBuffer();

        // last four bytes store the number of items.

        byte[] count_bytes = IntBytes.toByteArray( count );
        out.write( count_bytes );
        length += count_bytes.length;
        
        out.close();

        closed = true;
        
    }
    
}