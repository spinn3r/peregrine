package maprunner.values;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import maprunner.*;
import maprunner.util.*;

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

    public void write( List<byte[]> values ) {
        for( byte[] val : values ) {
            this.values.add( val );
        }
    }
    
    public void write( String value ) {
        write( new StringValue( value ).toBytes() );
    }

    public void write( int value ) {
        write( new IntValue( value ).toBytes() );
    }

    public void write( byte[] value ) {
        values.add( value );
    }

    // READERS

    public List<byte[]> read() {
        return values;
    }

    public int readInt() {
        return new IntValue( values.get( ptr++ ) ).value;
    }

    public byte[] toBytes() {

        try {

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            VarintWriter varintWriter = new VarintWriter();
            
            for( byte[] value : values ) {
                bos.write( varintWriter.write( value.length ) );
                bos.write( value);
            }
            
            return bos.toByteArray();
            
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
        
    }

    public void fromBytes( byte[] data ) {

        try {
            
            TrackedInputStream is = new TrackedInputStream( new ByteArrayInputStream( data ) );
            VarintReader varintReader = new VarintReader();
            
            while( is.getPosition() < data.length ) {

                int len = varintReader.read( is );
                byte[] entry = new byte[len];
                is.read( entry );
                values.add( entry );
            }
            
            is.close();
            
        } catch ( IOException e ) {
            throw new RuntimeException( e ); // can't happen.
        }

    }

    public String toString() {

        StringBuffer buff = new StringBuffer();
        
        for ( byte[] val : values ) {

            buff.append( String.format( "%s ", Hex.encode( val ) ) );
            
        }

        return buff.toString();
        
    }
    
}