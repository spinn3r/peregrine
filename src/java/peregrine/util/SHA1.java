
package peregrine.util;

import java.security.*;

/**
 * SHA1 cryptographic hash function:
 *
 * http://en.wikipedia.org/wiki/SHA-1
 * 
 */
public class SHA1 {

    private static GenericMessageDigest delegate = new GenericMessageDigest( "SHA1" );

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

