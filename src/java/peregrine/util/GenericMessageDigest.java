
package peregrine.util;

import java.nio.charset.*;
import java.security.*;

/**
 *
 */
public class GenericMessageDigest {

    private static Charset UTF8 = Charset.forName( "UTF-8" );

    private ThreadLocal local = null;

    public GenericMessageDigest( String name ) {
        this.local = new ThreadLocalMessageDigest( name );
    }
    
    public byte[] encode( final String content ) {

        return encode( content.getBytes( UTF8 ) );

    }
    
    public byte[] encode( final byte[] bytes ) {

        MessageDigest md = (MessageDigest)local.get();        
        md.reset();
        return md.digest( bytes );

    }

    public MessageDigest getMessageDigest() {
        return (MessageDigest)local.get();
    }
        
}

