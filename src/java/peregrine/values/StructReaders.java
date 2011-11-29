package peregrine.values;

import org.jboss.netty.buffer.*;

import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * 
 */
public class StructReaders {
	
	public static final StructReader TRUE  = create( true );
	public static final StructReader FALSE = create( false );

    public static StructReader create( double value ) {

        return new StructWriter()
            .writeDouble( value )
            .toStructReader()
            ;
        
    }

    public static StructReader create( int value ) {

        return new StructWriter()
            .writeInt( value )
            .toStructReader()
            ;
        
    }

    public static StructReader create( long value ) {

        return new StructWriter()
            .writeLong( value )
            .toStructReader()
            ;
        
    }

    public static StructReader create( boolean value ) {

        return new StructWriter()
            .writeBoolean( value )
            .toStructReader()
            ;
        
    }

    public static StructReader create( byte[] value ) {

        return new StructWriter()
            .writeBytes( value )
            .toStructReader()
            ;
        
    }

    public static StructReader hashcode( String value ) {

        return new StructWriter()
            .writeHashcode( value )
            .toStructReader()
            ;
        
    }

    public static StructReader hashcode( int value ) {

        return new StructWriter()
            .writeHashcode( value )
            .toStructReader()
            ;
        
    }

    public static StructReader varint( int value ) {

        return new StructWriter()
            .writeVarint( value )
            .toStructReader()
            ;
        
    }

}