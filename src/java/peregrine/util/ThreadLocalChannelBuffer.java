package peregrine.util;

import java.io.*;
import java.util.*;

import org.jboss.netty.buffer.*;

/**
 * A thread local ChannelBuffer which allows us to write on a buffer and reset
 * it every time we use it which is much faster than continually re-allocating.
 */
public class ThreadLocalChannelBuffer extends ThreadLocal<ChannelBuffer> {

    ChannelBufferFactory factory;
    
    public ThreadLocalChannelBuffer( int capacity ) {
        this.factory = new DefaultChannelBufferFactory( capacity );
    }

    public ThreadLocalChannelBuffer( int initialCapacity, int extentLength ) {
        this.factory = new ExtendedChannelBufferFactory( initialCapacity, extentLength );
    }

    @Override
    public ChannelBuffer get() {

        ChannelBuffer result = super.get();

        result.resetWriterIndex();
        result.resetReaderIndex();

        return result;
        
    }
    
    @Override
    public ChannelBuffer initialValue() {
        return factory.newChannelBuffer();
    }
    
}

interface ChannelBufferFactory {

    public ChannelBuffer newChannelBuffer();
    
}

class DefaultChannelBufferFactory implements ChannelBufferFactory {

    private int capacity;
    
    public DefaultChannelBufferFactory( int capacity ) {
        this.capacity = capacity;
    }

    public ChannelBuffer newChannelBuffer() {
        return ChannelBuffers.buffer( capacity );
    }
    
}

class ExtendedChannelBufferFactory implements ChannelBufferFactory {

    private int initialCapacity;
    private int extentLength;
    
    public ExtendedChannelBufferFactory( int initialCapacity, int extentLength ) {
        this.initialCapacity = initialCapacity;
        this.extentLength = extentLength;
    }

    public ChannelBuffer newChannelBuffer() {
        return new ExtendedChannelBuffer( initialCapacity, extentLength );
    }
    
}