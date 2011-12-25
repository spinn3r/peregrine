package peregrine.values;

import java.io.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.util.*;

/**
 * A set of hashcodes.
 */
public class HashSetValue {

    public List<StructReader> values = new ArrayList();
    
    public HashSetValue() {}

    public HashSetValue( StructReader data ) {
        fromChannelBuffer( data.getChannelBuffer() );
    }

    public void add( StructReader value ) {
        values.add( value );
    }

    public Collection<StructReader> getValues() {
        return values;
    }

    public int size() {
        return values.size();
    }
    
    public ChannelBuffer toChannelBuffer() {

        ChannelBuffer buff = ChannelBuffers.buffer( values.size() * Hashcode.HASH_WIDTH );
        
        for( StructReader value : values ) {
            buff.writeBytes( value.getChannelBuffer() );
        }
        
        return buff;

    }

    public void fromChannelBuffer( ChannelBuffer buff ) {

        int nr_entries = buff.writerIndex() / Hashcode.HASH_WIDTH;

        for ( int i = 0; i < nr_entries; ++i ) {

            values.add( new StructReader( buff.readSlice( Hashcode.HASH_WIDTH ) ) );

        }
        
    }

    public String toString() {

        StringBuffer buff = new StringBuffer();
        
        for ( StructReader val : getValues() ) {

            buff.append( String.format( "%s ", Hex.encode( val.getChannelBuffer() ) ) );
            
        }

        return buff.toString();
        
    }
    
}