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

/**
 * Base16 - encodes 'Canonical' Base16.  
 */
public class Base16 {

    private static org.apache.commons.codec.binary.Hex codec =
        new org.apache.commons.codec.binary.Hex();
    
    /**
     */
    public static String encode( final byte[] bytes ) {
        return new String( codec.encode( bytes ) );
    }

    /**
     */
    public static byte[] decode( byte[] bytes ) {

        try {
            return codec.decode( bytes );
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }

    }

    /**
     */
    public static byte[] decode( String data ) {
        return decode( data.getBytes() );
    }

}

