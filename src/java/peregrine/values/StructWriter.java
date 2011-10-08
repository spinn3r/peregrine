package peregrine.values;

import java.io.*;
import java.nio.*;

import java.nio.charset.Charset;

import peregrine.*;
import peregrine.util.*;

/**
 * 
 */
public class StructWriter {

    public static int BUFFER_SIZE = 16384;
    
    private static ByteBuffer buff = null;

    private static VarintWriter varintWriter = new VarintWriter();

    private static ThreadLocalByteBuffer tla = new ThreadLocalByteBuffer();

    public StructWriter() {

        buff = tla.get();
        buff.reset();
        
    }

    public StructWriter writeVarint( int value ) {

        varintWriter.write( buff, value );
        return this;
        
    }

    public StructWriter writeDouble( double value ) {

        buff.put( LongBytes.toByteArray( Double.doubleToLongBits( value ) ) );
        return this;

    }
    
    public StructWriter writeHashcode( String key ) {

        buff.put( Hashcode.getHashcode( key ) );
        return this;
        
    }
    
    public byte[] toBytes() {

        byte[] backing = buff.array();
        int len = buff.position();
        
        byte[] result = new byte[ len ];
        System.arraycopy( backing, 0, result, 0, len );
        
        return result;
        
    }
    
}

class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {

    public ByteBuffer initialValue() {

        ByteBuffer buff = ByteBuffer.allocate( StructWriter.BUFFER_SIZE );
        buff.mark();
        return buff;
    }
    
}