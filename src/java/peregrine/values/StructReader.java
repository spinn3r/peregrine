package peregrine.values;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import peregrine.*;
import peregrine.util.*;

/**
 * 
 */
public class StructReader {

    private ByteArrayInputStream in;

    private UnsafeInputStream unsafe;
    
    private static VarintReader varintReader = new VarintReader();

    public StructReader( byte[] data ) {
        this.in = new ByteArrayInputStream( data );
        this.unsafe = new UnsafeInputStream( this.in );
    }

    public int readVarint() {

        try {

            return varintReader.read( in );
            
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        
    }

    public double readDouble() {

        byte[] data = new byte[8];
        unsafe.read( data );

        return Double.longBitsToDouble( LongBytes.toLong( data ) );

    }

    public byte[] readHashcode() {

        byte[] result = new byte[Hashcode.HASH_WIDTH];
        unsafe.read( result );

        return result;

    }
    
}

class UnsafeInputStream {

    private InputStream delegate = null;
    
    public UnsafeInputStream( InputStream is ) {
        this.delegate = is;
    }

    public void read( byte[] data ) {

        try {

            this.delegate.read( data );
            
        } catch ( IOException e ) {
            throw new RuntimeException(e);
        }
        
    }

}