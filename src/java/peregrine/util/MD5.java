/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

