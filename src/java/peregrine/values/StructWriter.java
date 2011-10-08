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
    
    //private ByteArrayOutputStream out = null;

    private static ByteBuffer buff = null;

    private static VarintWriter varintWriter = new VarintWriter();

    private static ThreadLocalByteBuffer tla = new ThreadLocalByteBuffer();

    private UnsafeOutputStream unsafe = null;

    public StructWriter() {

        buff = tla.get();
        buff.reset();

        //unsafe = new UnsafeOutputStream( out );
        
    }

    public StructWriter writeVarint( int value ) {

        //unsafe.write( varintWriter.write( value ) );

        varintWriter.write( buff, value );
        //buff.put( (byte) value );
        
        return this;
        
    }

    public StructWriter writeDouble( double value ) {

        unsafe.write( LongBytes.toByteArray( Double.doubleToLongBits( value ) ) );
        return this;

    }
    
    public StructWriter writeHashcode( String key ) {

        unsafe.write( Hashcode.getHashcode( key ) );
        return this;
        
    }
    
    public byte[] toBytes() {

        //return out.toByteArray();

        byte[] backing = buff.array();
        int len = buff.position();
        
        byte[] result = new byte[ len ];
        System.arraycopy( backing, 0, result, 0, len );
        
        //System.out.printf( "%,d\n", result.length );
        
        return result;
        
    }
    
}

class UnsafeOutputStream {

    private OutputStream delegate = null;
    
    public UnsafeOutputStream( OutputStream is ) {
        this.delegate = is;
    }

    public void write( byte[] data ) {

        try {

            this.delegate.write( data );
            
        } catch ( IOException e ) {
            throw new RuntimeException(e);
        }
        
    }

}

class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {

    public ByteBuffer initialValue() {

        ByteBuffer buff = ByteBuffer.allocate( StructWriter.BUFFER_SIZE );
        buff.mark();
        return buff;
    }
    
}