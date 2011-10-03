package peregrine.values;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import peregrine.*;
import peregrine.util.*;

/**
 * 
 */
public class StructWriter {

    private ByteArrayOutputStream out = null;
    private UnsafeOutputStream unsafe = null;

    private static VarintWriter varintWriter = new VarintWriter();

    public StructWriter() {

        out = new ByteArrayOutputStream();
        unsafe = new UnsafeOutputStream( out );
        
    }

    public StructWriter writeVarint( int value ) {

        unsafe.write( varintWriter.write( value ) );
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

        return out.toByteArray();
        
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