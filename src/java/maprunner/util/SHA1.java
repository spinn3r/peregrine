
package maprunner.util;

import java.nio.charset.*;
import java.security.*;

/**
 *
 */
public class SHA1 {

    private static ThreadLocal local = new ThreadLocalMessageDigest( "SHA1" );

    private static Charset UTF8 = Charset.forName( "UTF-8" );

    public static byte[] encode( final String content ) {

        return encode( content.getBytes( UTF8 ) );

    }
    
    public static byte[] encode( final byte[] bytes ) {

        MessageDigest md = (MessageDigest)local.get();        
        md.reset();
        return md.digest( bytes );

    }

    public static MessageDigest getMessageDigest() {
        return (MessageDigest)local.get();
    }
        
}

