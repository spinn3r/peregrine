package peregrine;

import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * Create a StructReader around primitive types, varints, hashcodes, etc.
 */
public class StructReaders {
	
	public static final StructReader TRUE  = wrap( true );
	public static final StructReader FALSE = wrap( false );

    public static StructReader wrap( double value ) {

        return new StructWriter()
            .writeDouble( value )
            .toStructReader()
            ;
        
    }

    public static StructReader wrap( int value ) {

        return new StructWriter()
            .writeInt( value )
            .toStructReader()
            ;
        
    }

    public static StructReader wrap( long value ) {

        return new StructWriter()
            .writeLong( value )
            .toStructReader()
            ;
        
    }

    public static StructReader wrap( boolean value ) {

        return new StructWriter()
            .writeBoolean( value )
            .toStructReader()
            ;
        
    }

    public static StructReader wrap( byte[] value ) {

        return new StructReader( value );

    }

    public static StructReader hashcode( String value ) {

        return new StructWriter()
            .writeHashcode( value )
            .toStructReader()
            ;
        
    }

    public static StructReader hashcode( long value ) {

        return new StructWriter()
            .writeHashcode( value )
            .toStructReader()
            ;
        
    }

    /**
     * Generate a StructReader that writes the given list of primitives to a set
     * of hashcodes.
     */
    public static StructReader hashcode( List<Long> values ) {

        StructWriter writer = new StructWriter( values.size() * Hashcode.HASH_WIDTH );
        
        for( long current : values ) {
            writer.writeHashcode( current );
        }

        return writer.toStructReader();
        
    }

    public static StructReader hashcode( int value ) {
        return hashcode( (long)value );
    }

    public static StructReader varint( int value ) {

        return new StructWriter()
            .writeVarint( value )
            .toStructReader()
            ;
        
    }

}