package peregrine.io.chunk;

import java.io.*;
import java.nio.channels.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;
import peregrine.config.*;
import peregrine.http.*;
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

    public static int BUFFER_SIZE = 16384;
    
    protected ChannelBufferWritable writer = null;

    private int count = 0;

    protected long length = 0;

    private boolean closed = false;

    private boolean shutdown = false;
    
    public DefaultChunkWriter( ChannelBufferWritable writer ) throws IOException {
        init( writer );
    }

    public DefaultChunkWriter( Config config, File file ) throws IOException {

        MappedFile mapped = new MappedFile( config, file, "rw" );

        init( mapped.getChannelBufferWritable() );

    }

    private void init( ChannelBufferWritable writer ) {
        this.writer = new BufferedChannelBufferWritable( writer, BUFFER_SIZE );
    }
    
    @Override
    public void write( StructReader key, StructReader value )
        throws IOException {

        if ( closed )
            throw new IOException( "closed" );

        length += write( writer, key, value );
        
        ++count;

    }

    /**
     * Perform a DIRECT write on a ChannelBuffer of a key/value pair.
     */
    public static int write( ChannelBufferWritable writer ,
                             StructReader key,
                             StructReader value ) throws IOException {
        
    	int result = 0;

        // TODO: we have to use an atomic write so that the buffered writer can
        // make sure to get one key/value pair without truncating it
        // incorrectly.
        
        ChannelBuffer wrapped =
            ChannelBuffers.wrappedBuffer( StructReaders.varint( key.length() ).getChannelBuffer(),
                                          key.getChannelBuffer(),
                                          StructReaders.varint( value.length() ).getChannelBuffer(),
                                          value.getChannelBuffer() );

        writer.write( wrapped );
        
        result += wrapped.writerIndex();

        return result;
        
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
        writer.write( buff );
        length += IntBytes.LENGTH;

        if ( writer instanceof MultiChannelBufferWritable ) {

            MultiChannelBufferWritable multi = (MultiChannelBufferWritable)writer;
            multi.shutdown();
            
        }

        shutdown = true;
        
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
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