
package peregrine.util;

import java.nio.charset.Charset;

public class Hashcode {

    public static final int HASH_WIDTH = 8;

    private static Charset UTF8 = Charset.forName( "UTF-8" );

    public static byte[] getHashcode( String input ) {
        return getHashcode( input.getBytes( UTF8 ) );
    }

    public static byte[] getHashcode( byte[] input ) {
        return getHashcodeWithMD5( input );
    }

    public static byte[] getHashcodeWithSHA1( byte[] input ) {

        //TODO: it is probably easier to route based on the byte array data and
        //not first converting it to a long.
        
        byte[] hashed = SHA1.encode( input );
        byte[] data   = new byte[ HASH_WIDTH ];
        
        System.arraycopy( hashed, 0, data, 0, HASH_WIDTH );
        
        return data;

    }

    public static byte[] getHashcodeWithMD5( byte[] input ) {

        byte[] hashed = MD5.encode( input );
        byte[] data = new byte[ HASH_WIDTH ];
        
        System.arraycopy( hashed, 0, data, 0, HASH_WIDTH );
        
        return data;

    }

}