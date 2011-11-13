package peregrine.http;

import java.io.*;
import java.util.*;

import org.jboss.netty.buffer.*;

/**
 * Buffer output until it is ready to be sent in one CHUNK.  Not N chunks.  This
 * uses ChannelBuffers which can be more efficient as they are all sliced and
 * then an aggregate is used.
 * 
 */
public class BufferedChannelBuffer implements ChannelBufferWritable {

    public List<ChannelBuffer> buffers = new ArrayList();

    /**
     * The length, in bytes, for the pending write.
     */
    private int writeLength = 0;

    private int capacity;

    private ChannelBufferWritable delegate;
    
    public BufferedChannelBuffer( ChannelBufferWritable delegate,
                                  int capacity ) {
        this.delegate = delegate;
        this.capacity = capacity;
    }

    @Override
    public void write( ChannelBuffer buff ) throws IOException {

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
    
    public void flush() throws IOException {

        preFlush();
        
        delegate.write( getChannelBuffer() );
        
        buffers = new ArrayList();
        writeLength = 0;

    }
    
    @Override
    public void close() throws IOException {

        if ( writeLength > 0 )
            flush();

        delegate.close();
        
    }
    
    protected ChannelBuffer getChannelBuffer() {

        ChannelBuffer[] buffs = new ChannelBuffer[ buffers.size() ];

        int idx = 0;
        for( ChannelBuffer curr : buffers ) {
            buffs[idx++] = curr;
        }

        return ChannelBuffers.wrappedBuffer( buffs );
        
    }

    /**
     * Make sure we can accept this output buffer so that it's < 16384 but as
     * close as possible.
     */
    protected boolean hasCapacity( ChannelBuffer buff ) {

        int newCapacity = writeLength + buff.writerIndex() + HttpClient.CHUNK_OVERHEAD;
        
        return newCapacity < capacity;

    }
    
}