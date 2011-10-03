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

    private ByteArrayOutputStream out = new ByteArrayOutputStream();

    private static VarintWriter varintWriter = new VarintWriter();
    
    public StructWriter writeVarint( int value ) {

        try {
            
            out.write( varintWriter.write( value ) );
            return this;
            
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        
    }

    public StructWriter writeDouble( double value ) {

        try {

            out.write( LongBytes.toByteArray( Double.doubleToLongBits( value ) ) );
            return this;
            
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

    }
    
    public StructWriter writeHashcode( String key ) {

        try {
            
            out.write( Hashcode.getHashcode( key ) );
            return this;
            
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        
    }
    
    public byte[] toBytes() {

        return out.toByteArray();
        
    }
    
}