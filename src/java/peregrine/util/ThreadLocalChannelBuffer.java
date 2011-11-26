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

        // reset the previous one... 
        result.readerIndex( 0 );
        result.writerIndex( 0 );

        return result;
        
    }
    
    @Override
    public ChannelBuffer initialValue() {

        if ( capacity <= 0 )
            throw new IllegalArgumentException( "capacity" );
        
        //return ChannelBuffers.buffer( capacity );

        // support extension if we need it...
        return new SlabDynamicChannelBuffer( capacity, capacity );
        
    }
    
}

