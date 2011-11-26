package peregrine.util;

import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;

/**
 * A thread local ChannelBuffer which allows us to write on a buffer and reset
 * it every time we use it which is much faster than continually re-allocating.
 */
public class ThreadLocalChannelBuffer extends ThreadLocal<ChannelBuffer> {

    private int capacity;
    
    public ThreadLocalChannelBuffer( int capacity ) {
        this.capacity = capacity;
    }

    @Override
    public ChannelBuffer get() {

        ChannelBuffer result = super.get();

        result.writerIndex( 0 );
        result.readerIndex( 0 );

        return result;
        
    }
    
    @Override
    public ChannelBuffer initialValue() {
        //return ChannelBuffers.buffer( capacity );

        // support extension if we need it...
        return new SlabDynamicChannelBuffer( capacity, capacity );
        
    }
    
}

