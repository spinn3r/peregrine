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
public class ByteArrayListValue implements Value {

    public List<byte[]> values = new ArrayList();
    
    public ByteArrayListValue() {}

    public ByteArrayListValue( byte[] data ) {
        fromBytes( data );
    }

    public void addValue( byte[] value ) {
        values.add( value );
    }

    public void addValues( List<byte[]> _values ) {
        this.values.addAll( _values );
    }

    public List<byte[]> getValues() {
        return values;
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
                addValue( entry );
            }
            
            is.close();
            
        } catch ( IOException e ) {
            throw new RuntimeException( e ); // can't happen.
        }

    }

    public String toString() {

        StringBuffer buff = new StringBuffer();
        
        for ( byte[] val : getValues() ) {

            buff.append( String.format( "%s ", Base64.encode( val ) ) );
            
        }

        return buff.toString();
        
    }
    
}