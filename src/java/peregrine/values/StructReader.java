package peregrine.values;

import java.io.*;
import peregrine.util.*;

/**
 * 
 */
public class StructReader {

    private UnsafeInputStream unsafe;

    private boolean debug = false;

    private InputStream is;
    
    private static VarintReader varintReader;

    public StructReader( byte[] data ) {
        this( new ByteArrayInputStream( data ) );
    }

    public StructReader( InputStream is ) {
        this.is = is;
        this.unsafe = new UnsafeInputStream( is );
        this.varintReader = new VarintReader( is );
    }

    public int readVarint() {
        return varintReader.read();
    }

    public double readDouble() {

        byte[] data = new byte[8];
        unsafe.read( data );

        return Double.longBitsToDouble( LongBytes.toLong( data ) );

    }

    public int readInt() {

        byte[] data = new byte[4];
        unsafe.read( data );

        return IntBytes.toInt( data );

    }

    public byte[] read( byte[] data ) {
        unsafe.read( data );
        return data;
    }
    
    public byte[] readHashcode() {

        byte[] result = new byte[Hashcode.HASH_WIDTH];
        unsafe.read( result );

        return result;

    }

    public void setDebug( boolean debug ) {
        this.debug = debug;
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