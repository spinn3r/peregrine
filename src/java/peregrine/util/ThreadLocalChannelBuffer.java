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

    public ThreadLocalChannelBuffer( int initialCapacity, int extentLength ) {
        this.factory = new ExtendedSimpleChannelBufferFactory( initialCapacity, extentLength );
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

class ExtendedSimpleChannelBufferFactory implements SimpleChannelBufferFactory {

    private int initialCapacity;
    private int extentLength;
    
    public ExtendedSimpleChannelBufferFactory( int initialCapacity, int extentLength ) {
        this.initialCapacity = initialCapacity;
        this.extentLength = extentLength;
    }

    public ChannelBuffer newChannelBuffer() {
        return new ExtendedChannelBuffer( initialCapacity, extentLength );
    }
    
}