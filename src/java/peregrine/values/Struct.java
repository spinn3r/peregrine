package peregrine.values;

import java.io.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * Stores a list of byte arrays.  The nice thing is that you can just APPEND to
 * this list when you want to add entries to it...
 */
public class Struct implements Value {

    public List<byte[]> values = new ArrayList();

    private int ptr = 0;
    
    public Struct() {}

    public Struct( byte[] data ) {
        fromBytes( data );
    }

    //WRITERS

    public Struct write( List<byte[]> values ) {

        for( byte[] val : values ) {
            this.values.add( val );
        }

        return this;

    }
    
    public Struct write( String value ) {
        write( new StringValue( value ).toBytes() );
        return this;
    }

    public Struct write( int value ) {
        write( new IntValue( value ).toBytes() );
        return this;
    }

    public Struct write( byte[] value ) {
        values.add( value );
        return this;
    }

    // READERS

    public List<byte[]> read() {
        return values;
    }

    public int readInt() {
        return new IntValue( values.get( ptr++ ) ).value;
    }

    @Override
    public byte[] toBytes() {

        try {

        	int len = 0;
            for( byte[] value : values ) {
                len += IntBytes.LENGTH + value.length;
            }

            ChannelBuffer buff = ChannelBuffers.buffer( len );
            
            for( byte[] value : values ) {
                VarintWriter.write( buff, value.length );
                buff.writeBytes( value );
            }
            
            byte[] result = new byte[ buff.writerIndex() ];
            buff.readBytes( result );
            return result;
            
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
        
    }

    @Override
    public void fromBytes( byte[] data ) {

        try {
            
            TrackedInputStream is = new TrackedInputStream( new ByteArrayInputStream( data ) );
            VarintReader varintReader = new VarintReader( is );
            
            while( is.getPosition() < data.length ) {

                int len = varintReader.read();
                byte[] entry = new byte[len];
                is.read( entry );
                values.add( entry );
            }
            
            is.close();
            
        } catch ( IOException e ) {
            throw new RuntimeException( e ); // can't happen.
        }

    }

    @Override
    public String toString() {

        StringBuffer buff = new StringBuffer();
        
        for ( byte[] val : values ) {

            buff.append( String.format( "%s ", Hex.encode( val ) ) );
            
        }

        return buff.toString();
        
    }
    
}