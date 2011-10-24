package peregrine.util;

import java.io.*;
import java.util.*;

import org.jboss.netty.buffer.*;

/**
 * A thread local ChannelBuffer which allows us to write on a buffer and reset
 * it every time we use it which is much faster than continually re-allocating.
 */
public class ThreadLocalChannelBuffer extends ThreadLocal<ChannelBuffer> {

    SimpleChannelBufferFactory factory;
    
    public ThreadLocalChannelBuffer( int capacity ) {
        this.factory = new DefaultSimpleChannelBufferFactory( capacity );
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
        return factory.newChannelBuffer();
    }
    
}

interface SimpleChannelBufferFactory {

    public ChannelBuffer newChannelBuffer();
    
}

class DefaultSimpleChannelBufferFactory implements SimpleChannelBufferFactory {

    private int capacity;
    
    public DefaultSimpleChannelBufferFactory( int capacity ) {
        this.capacity = capacity;
    }

    public ChannelBuffer newChannelBuffer() {
        return ChannelBuffers.buffer( capacity );
    }
    
}
