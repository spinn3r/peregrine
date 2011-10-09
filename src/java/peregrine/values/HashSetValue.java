package peregrine.values;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import peregrine.*;
import peregrine.util.*;

/**
 * A set of hashcodes.
 ** FIXME: change this name from HashSetValue to avoid confusion with java.util.HashSet
 */
public class HashSetValue implements Value {

    public List<byte[]> values = new ArrayList();
    
    public HashSetValue() {}

    public HashSetValue( byte[] data ) {
        fromBytes( data );
    }

    public void add( byte[] value ) {
        values.add( value );
    }

    public Collection<byte[]> getValues() {
        return values;
    }

    public int size() {
        return values.size();
    }
    
    public byte[] toBytes() {

        try {

            //TODO: we could create a fixed width array and then System.arrayCopy into it 
            ByteArrayOutputStream bos = new ByteArrayOutputStream( values.size() * Hashcode.HASH_WIDTH );

            for( byte[] value : values ) {
                bos.write( value);
            }
            
            return bos.toByteArray();
            
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
        
    }

    public void fromBytes( byte[] data ) {

        int nr_entries = data.length / Hashcode.HASH_WIDTH;

        for ( int i = 0; i < nr_entries; ++i ) {

            int offset = i * Hashcode.HASH_WIDTH;
            byte[] entry = new byte[ Hashcode.HASH_WIDTH ];

            System.arraycopy( data, offset, entry, 0, Hashcode.HASH_WIDTH );

            values.add( entry );
            
        }
        
    }

    public String toString() {

        StringBuffer buff = new StringBuffer();
        
        for ( byte[] val : getValues() ) {

            buff.append( String.format( "%s ", Hex.encode( val ) ) );
            
        }

        return buff.toString();
        
    }
    
}