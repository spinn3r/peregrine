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
 * Base16 - encodes 'Canonical' Base16
 */
public class Base16 {

    static final String[] hex = new String[] {
            "0", "1", "2", "3", "4", "5", "6",
            "7", "8", "9", "a", "b", "c", "d",
            "e", "f",
    };

    /**
     * @param bytes
     * @return String
     */
    public static String encode(final byte[] bytes) {

        StringBuffer base16 = new StringBuffer( bytes.length );

        for( int i = 0; i < bytes.length; i++) {
            base16.append(hex[(bytes[i] >>4) & 15]);
            base16.append(hex[(bytes[i]) & 15]);
        }
        return base16.toString();
    }

}

