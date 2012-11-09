/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.util;

import peregrine.*;

import java.nio.charset.Charset;

public class Hashcode {

    /**
     * Width of a hashcode (8 bytes).
     */
    public static final int HASH_WIDTH = 8;

    /**
     * Required key width.
     */
    public static final int KEY_WIDTH = HASH_WIDTH;

    public static byte[] getHashcode( String input ) {
        return getHashcode( input.getBytes( Charsets.UTF8 ) );
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

    public static void assertKeyLength( StructReader key ) {

        if ( key.length() != KEY_WIDTH ) {

            throw new IllegalArgumentException( String.format( "Key is incorrect length (must be %s bytes): %s",
                                                               KEY_WIDTH, key.length() ) ); 

        }

    }
    
}
