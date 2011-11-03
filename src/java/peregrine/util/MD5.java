
package peregrine.util;

import java.security.*;

/**
 * MD5 cryptographic hash function:
 *
 * http://en.wikipedia.org/wiki/MD5
 *
 * MD5 should generally be deprecated but for key generation purposes where
 * performance is a factor MD5 is much faster (especially when testing).
 * 
 */
public class MD5 {

    private static GenericMessageDigest delegate = new GenericMessageDigest( "MD5" );

    public static byte[] encode( String content ) {
        return delegate.encode( content );
    }
    
    public static byte[] encode( final byte[] bytes ) {
        return delegate.encode( bytes );
    }

    public static MessageDigest getMessageDigest() {
        return delegate.getMessageDigest();
    }

}

