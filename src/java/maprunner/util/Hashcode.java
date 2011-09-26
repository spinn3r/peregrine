
package maprunner.util;

import java.nio.charset.Charset;

public class Hashcode {

    public static final int HASH_WIDTH = 8;

    private static Charset UTF8 = Charset.forName( "UTF-8" );

    public static byte[] getHashcode( String input ) {
        return getHashcode( input.getBytes( UTF8 ) );
    }

    public static byte[] getHashcode( byte[] input ) {

        //TODO: it is probably easier to route based on the byte array data and
        //not first converting it to a long.
        
        byte[] sha1 = SHA1.encode( input );
        byte[] data = new byte[ HASH_WIDTH ];
        
        System.arraycopy( sha1, 0, data, 0, HASH_WIDTH );
        
        return data;

    }

}