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

    public static int BUFFER_SIZE = 16384;
    
    protected ChannelBufferWritable writer = null;

    private int count = 0;

    protected long length = 0;

    private boolean closed = false;

    private boolean shutdown = false;
    
    public DefaultChunkWriter( ChannelBufferWritable writer ) throws IOException {
        init( writer );
    }

    public DefaultChunkWriter( File file ) throws IOException {

        MappedFile mapped = new MappedFile( file, FileChannel.MapMode.READ_WRITE );

        init( mapped.getChannelBufferWritable() );

    }

    private void init( ChannelBufferWritable writer ) {
        this.writer = new BufferedChannelBufferWritable( writer, BUFFER_SIZE );
    }
    
    @Override
    public void write( byte[] key, byte[] value )
        throws IOException {

        if ( closed )
            throw new IOException( "closed" );

        writeVarint( key.length );
        write( ChannelBuffers.wrappedBuffer( key ) );
        writeVarint( value.length );
        write( ChannelBuffers.wrappedBuffer( value ) );
        
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

    private void write( ChannelBuffer buff ) throws IOException {

        writer.write( buff );
        length += buff.writerIndex();
        
    }

    private void writeVarint( int value ) throws IOException {

        ChannelBuffer buff = ChannelBuffers.buffer( IntBytes.LENGTH );
        VarintWriter.write( buff, value );
        write( buff );
        
    }
    
    @Override
    public int count() {
        return count;
    }

    @Override
    public long length() {
        return length;
    }

    public void shutdown() throws IOException {

        if ( shutdown )
            return;
        
        // last four bytes store the number of items.

        ChannelBuffer buff = ChannelBuffers.buffer( IntBytes.LENGTH );
        buff.writeInt( count );
        write( buff );

        if ( writer instanceof MultiChannelBufferWritable ) {

            MultiChannelBufferWritable multi = (MultiChannelBufferWritable)writer;
            multi.shutdown();
            
        }

        shutdown = true;
        
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;

        shutdown();

        writer.close();

        closed = true;
        
    }

}