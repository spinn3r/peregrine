package maprunner.values;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import maprunner.*;

public class SetValue implements Value {

    public List<Value> values = new ArrayList();
    
    public SetValue() {}

    public void add( Value value ) {
        values.add( value );
    }
    
    public byte[] toBytes() {

        try {

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            //FIXME: varint on the number of members...
            for( Value value : values ) {

                bos.write( value.toBytes() );
                
            }

            bos.close();
            
            return bos.toByteArray();
            
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
        
    }
    
}