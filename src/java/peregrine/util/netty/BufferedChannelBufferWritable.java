package peregrine.util.netty;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;


/**
 * Buffer output until it is ready to be sent in one CHUNK.  Not N chunks.  This
 * uses ChannelBuffers which can be more efficient as they are all sliced and
 * then an aggregate is used.
 * 
 */
public class BufferedChannelBufferWritable implements ChannelBufferWritable {

    public List<ChannelBuffer> buffers = new ArrayList();

    /**
     * The length, in bytes, for the pending write.
     */
    private int writeLength = 0;

    private int capacity;

    private ChannelBufferWritable delegate;

    private boolean closed = false;
    
    public BufferedChannelBufferWritable( ChannelBufferWritable delegate,
                                          int capacity ) {
        this.delegate = delegate;
        this.capacity = capacity;
    }

    @Override
    public void write( ChannelBuffer buff ) throws IOException {

        if ( closed )
            throw new IOException( "closed" );
        
        if ( ! hasCapacity( buff ) ) { 
            flush();
        }

        buffers.add( buff );
        writeLength += buff.writerIndex();

    }

    /**
     * Perform an action preFlush.  This could be used for example to add
     * additional metadata to a write before we flush the data.
     */
    public void preFlush() throws IOException { }

    /**
     * Write out all buffers to the delegate.
     */
    @Override
    public void flush() throws IOException {

        if ( writeLength == 0 )
            return;

        if ( closed )
            return;

        if ( buffers.size() == 0 ) {
            return;
        }

        preFlush();

        ChannelBuffer writeBuffer = getChannelBuffer();

        delegate.write( writeBuffer );
        
        buffers = new ArrayList();
        writeLength = 0;

        // flush the underlying delegate too
        delegate.flush();
        
    }

    @Override
    public void shutdown() throws IOException {
        flush();
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;
        
        flush();

        delegate.close();

        closed = true;

    }

    @Override
    public String toString() {
        return delegate.toString();
    }
    
    protected ChannelBuffer getChannelBuffer() {

        ChannelBuffer[] buffs = new ChannelBuffer[ buffers.size() ];
        buffs = buffers.toArray( buffs );

        return ChannelBuffers.wrappedBuffer( buffs );
        
    }

    /**
     * Make sure we can accept this output buffer so that it's < 16384 but as
     * close as possible.
     */
    protected boolean hasCapacity( ChannelBuffer buff ) {

        int newCapacity = writeLength + buff.writerIndex();
        
        return newCapacity < capacity;

    }
    
}