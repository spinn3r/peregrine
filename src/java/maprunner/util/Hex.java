package maprunner.util;

import java.util.*;

public class Hex {

    public static String encode( byte[] input ) {

        if ( input == null )
            return "null";
        
        StringBuffer buff = new StringBuffer();
        
        for( byte b : input ) {

            buff.append( String.format( "%02x ", b ) );

        }

        return buff.toString();
        
    }
    
}