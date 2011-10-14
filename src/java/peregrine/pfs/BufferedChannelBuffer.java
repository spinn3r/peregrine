package peregrine.pfs;

import java.io.*;
import java.net.*;
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

    private int length = 0;

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
        length += buff.writerIndex();

    }

    public void flush() throws IOException {

        delegate.write( getChannelBuffer() );
        
        buffers = new ArrayList();
        length = 0;

    }
    
    @Override
    public void close() throws IOException {

        flush();
        
        delegate.close();
        
    }
    
    private ChannelBuffer getChannelBuffer() {

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
    private boolean hasCapacity( ChannelBuffer buff ) {

        int newCapacity = length + buff.writerIndex() + RemoteChunkWriterClient.CHUNK_OVERHEAD;
        
        return newCapacity < capacity;
    }
    
}