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

    public static StructReader wrap( ByteBuffer buff ) {
        return new StructReader( buff );
    }
    
    /**
     * Wrap a list of StructReader so that we can have a new struct which has
     * each StructReader in this list prefixed with a varint so we can unpack it
     * with {@link StructReader#readSlice()}.
     */
    public static StructReader wrap( List<StructReader> list ) {

        // TODO: I'm not sure it just wouldn't be faster to just copy the bytes.
        
        ChannelBuffer[] buffers = new ChannelBuffer[ list.size() * 2 ];
        int idx = 0;
        
        for( StructReader current : list ) {

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
