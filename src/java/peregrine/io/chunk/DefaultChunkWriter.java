package peregrine.io.chunk;

import java.io.*;
import java.nio.channels.*;

import peregrine.io.async.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;
import peregrine.http.*;

import org.jboss.netty.buffer.*;

/**
 * Export chunks are used in both the Extract phase of ETL jobs with
 * ExtractWriter to write data to individual partitions and chunks AND with map
 * jobs so that we can spool data to disk in the from the mappers directly to
 * the R partition files BEFORE we sort the output so that we can perform reduce
 * on each (K,V...) pair.
 */
public class DefaultChunkWriter implements ChunkWriter {

    public static int MAX_LENGTH_WIDTH = 2 * IntBytes.LENGTH;

    public static int BUFFER_SIZE = 16384;
    
    protected ChannelBufferWritable writer = null;

    private int count = 0;

    protected long length = 0;

    private boolean closed = false;

    private boolean shutdown = false;

    private ChannelBuffer buff = new SlabDynamicChannelBuffer( BUFFER_SIZE, BUFFER_SIZE );
    
    public DefaultChunkWriter( ChannelBufferWritable writer ) throws IOException {
        this.writer = writer;
    }

    public DefaultChunkWriter( File file ) throws IOException {

        MappedFile mapped = new MappedFile( file, FileChannel.MapMode.READ_WRITE );

        writer = mapped.getChannelBufferWritable();

    }

    public static OutputStream getOutputStream( File file ) throws IOException {

        return new AsyncOutputStream( file );
        
    }
    
    @Override
    public void write( byte[] key, byte[] value )
        throws IOException {

        if ( closed )
            throw new IOException( "closed" );

        // the max write width ... the key length and value length and the max
        // potential value of both keys.

        int write_width = key.length + value.length + MAX_LENGTH_WIDTH;

        if ( buff.writerIndex() + write_width >= (buff.capacity() - 1) ) {
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
        
        VarintWriter.write( buff, key.length );
        buff.writeBytes( key );

        VarintWriter.write( buff, value.length );
        buff.writeBytes( value );
        
    }
    
    private void flushChannelBuffer() throws IOException {

        length += buff.writerIndex();

        writer.write( buff );

        // we must create a new buffer each time for now because the sender is
        // async... pooling these would be good so that we can just return them
        // to the pool when done.
        
        buff = new SlabDynamicChannelBuffer( BUFFER_SIZE, BUFFER_SIZE );

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

        shutdown();

        writer.close();

        closed = true;
        
    }

    public void shutdown() throws IOException {

        if ( shutdown )
            return;
        
        flushChannelBuffer();

        // last four bytes store the number of items.

        ChannelBuffer buff = ChannelBuffers.buffer( IntBytes.LENGTH );
        buff.writeInt( count );
        writer.write( buff );
        
        length += IntBytes.LENGTH;

        if ( writer instanceof MultiOutputStream ) {

            MultiOutputStream multi = (MultiOutputStream)writer;
            multi.shutdown();
            
        }

        shutdown = true;
        
    }
    
}