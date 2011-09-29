package maprunner.util;

import java.util.*;

public class Hex {

    public static String encode( byte[] input ) {

        if ( input == null )
            return "null";
        
        StringBuffer buff = new StringBuffer();
        
        for( byte b : input ) {

            int i = b + 128;

            buff.append( String.format( "%02x ", i ) );

        }

        return buff.toString().trim();
        
    }
    
}