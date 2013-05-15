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

