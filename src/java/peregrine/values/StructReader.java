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

    private static VarintReader varintReader = new VarintReader();

    public StructReader( byte[] data ) {
        this.in = new ByteArrayInputStream( data );
    }

    public int readVarint() {

        try {

            return varintReader.read( in );
            
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        
    }

    public byte[] readHashcode() {

        try {

            byte[] result = new byte[Hashcode.HASH_WIDTH];
            in.read( result );

            return result;
            
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        
    }
    
}