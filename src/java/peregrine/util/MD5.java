
package peregrine.util;

import java.nio.charset.*;
import java.security.*;

/**
 * MD5 cryptographic hash function:
 *
 * http://en.wikipedia.org/wiki/MD5
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

}

