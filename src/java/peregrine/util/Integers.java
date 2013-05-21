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

public class Integers {

    public static final int LENGTH = 4;

    /**
     * Get a 4 byte array from the given int.
     */
    public static byte[] toByteArray( int value ) {

        byte[] b = new byte[LENGTH];
        b[0] = (byte)(( value >> 24 ) & 0xFF);
        b[1] = (byte)(( value >> 16 ) & 0xFF);
        b[2] = (byte)(( value >> 8  ) & 0xFF);
        b[3] = (byte)(( value >> 0  ) & 0xFF);

        return b;
        
    }

    /**
     * Convert a 4 byte array to an int
     */
    public static int toInt( byte[] b ) {

        //This works by taking each of the bit patterns and converting them to
        //ints taking into account 2s complement and then adding them..
        
        return (((((int) b[3]) & 0xFF) << 32) +
                ((((int) b[2]) & 0xFF) << 40) +
                ((((int) b[1]) & 0xFF) << 48) +
                ((((int) b[0]) & 0xFF) << 56));
    }    

}
