
/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine;

import java.util.*;
import java.nio.*;

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

        return new StructWriter(8)
            .writeDouble( value )
            .toStructReader()
            ;
        
    }

    public static StructReader wrap( int value ) {

        return new StructWriter(4)
            .writeInt( value )
            .toStructReader()
            ;
        
    }

    public static StructReader wrap( long value ) {

        return new StructWriter(8)
            .writeLong( value )
            .toStructReader()
            ;
        
    }

    public static StructReader wrap( boolean value ) {

        return new StructWriter(1)
            .writeBoolean( value )
            .toStructReader()
            ;
        
    }

    public static StructReader wrap( String value ) {

        // NOTE: this is a bit of a layering violation but the StructWriter
        // needs to be created with its capacity ahead of time.
        byte[] data = value.getBytes( StructWriter.UTF8 );
        
        return new StructWriter( data.length + 4 )
            .writeBytes( data )
            .toStructReader()
            ;
        
    }

    public static StructReader wrap( byte[] value ) {
        return new StructReader( value );
    }

    public static StructReader wrap( ByteBuffer buff ) {
        return new StructReader( buff );
    }

    public static StructReader wrap( ChannelBuffer buff ) {
        return new StructReader( buff );
    }

    public static StructReader wrap( List<StructReader> list ) {
        return wrap( list, true );
    }

    /**
     * Wrap a list of StructReader so that we can have a new struct which has
     * each StructReader in this list prefixed with a varint so we can unpack it
     * with {@link StructReader#readSlice()}.
     */
    public static StructReader wrap( StructReader... readers ) {

        // TODO: I'm not sure it just wouldn't be faster to just copy the bytes.
        
        ChannelBuffer[] buffers = new ChannelBuffer[ readers.length * 2 ];

        int idx = 0;
        
        for( StructReader current : readers ) {

            buffers[ idx++ ] = varint( current.length() ).getChannelBuffer();
            buffers[ idx++ ] = current.getChannelBuffer();
            
        }

        ChannelBuffer composite = ChannelBuffers.wrappedBuffer( buffers );
        
        return new StructReader( composite );

    }

    /**
     * Wrap a list of StructReader so that we can have a new struct which has
     * each StructReader in this list prefixed with a varint so we can unpack it
     * with {@link StructReader#readSlice()}.
     */
    public static StructReader wrap( List<StructReader> list, boolean useLengthPrefix ) {

        // TODO: I'm not sure it just wouldn't be faster to just copy the bytes.
        
        ChannelBuffer[] buffers;

        if( useLengthPrefix )
            buffers = new ChannelBuffer[ list.size() * 2 ];
        else
            buffers = new ChannelBuffer[ list.size() ];

        int idx = 0;
        
        for( StructReader current : list ) {

            if ( useLengthPrefix )
                buffers[ idx++ ] = varint( current.length() ).getChannelBuffer();

            buffers[ idx++ ] = current.getChannelBuffer();
            
        }

        ChannelBuffer composite = ChannelBuffers.wrappedBuffer( buffers );
        
        return new StructReader( composite );
        
    }

    public static List<StructReader> unwrap( StructReader reader ) {

        List<StructReader> result = new ArrayList();
        
        while( reader.isReadable() ) {

            int len = reader.readVarint();            
            result.add( reader.readSlice( len ) );
            
        }

        return result;
        
    }
    
    public static StructReader hashcode( long value ) {

        return new StructWriter()
            .writeHashcode( value )
            .toStructReader()
            ;
        
    }

    public static StructReader hashcode( int value ) {
        return hashcode( (long)value );
    }

    public static StructReader hashcode( byte[] value ) {

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

    /**
     * Generate a StructReader for a list of hashcodes.
     */
    // TODO: make all methods use this form so that we can easily make lists of
    // types which are fixed width.  
    public static StructReader hashcode( String... values ) {

        StructWriter writer = new StructWriter( values.length * Hashcode.HASH_WIDTH );

        for( String current : values ) {
            writer.writeHashcode( current );
        }

        return writer.toStructReader();

    }

    public static StructReader varint( int value ) {

        return new StructWriter()
            .writeVarint( value )
            .toStructReader()
            ;
        
    }

    public static StructReader join( StructReader... readers ) {

        ChannelBuffer[] buffers = new ChannelBuffer[ readers.length ];

        for( int i = 0; i < readers.length; ++i ) {
            buffers[i] = readers[i].buff;
        }

        return new StructReader( ChannelBuffers.wrappedBuffer( buffers ) );
        
    }

    public static StructReader join( Collection<StructReader> readers ) {

        ChannelBuffer[] buffers = new ChannelBuffer[ readers.size() ];

        int i = 0;

        for( StructReader current : readers ) {
            buffers[i] = current.buff;
            ++i;
        }

        return new StructReader( ChannelBuffers.wrappedBuffer( buffers ) );
        
    }

    public static List<StructReader> split( StructReader reader, int length ) {

        List<StructReader> result = new ArrayList( reader.length() / length );
        
        while ( reader.isReadable() ) {
            result.add( reader.readSlice( length ) );
        }

        return result;
        
    }
    
}
