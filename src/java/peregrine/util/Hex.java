package peregrine.util;

import java.util.*;

public class Hex {

    public static String encode( byte[] input ) {
        return encode( input, 128 );
    }

    public static String encode( byte[] input , int offset ) {

        if ( input == null )
            return "null";
        
        StringBuffer buff = new StringBuffer();
        
        for( byte b : input ) {

            int i = ((int)b) + offset;
            
            buff.append( String.format( "%02x ", i ) );

        }

        return buff.toString().trim();
        
    }
    
}