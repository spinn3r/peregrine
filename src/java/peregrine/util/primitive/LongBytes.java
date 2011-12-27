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
package peregrine.util.primitive;

public class LongBytes {

    public static final int LENGTH = 8;

    public static byte[] toByteArray( long value ) {

        byte[] b = new byte[LENGTH];

        b[7] = (byte)((value >> 0 ) & 0xFF);
        b[6] = (byte)((value >> 8 ) & 0xFF);
        b[5] = (byte)((value >> 16) & 0xFF);
        b[4] = (byte)((value >> 24) & 0xFF);

        b[3] = (byte)((value >> 32) & 0xFF);
        b[2] = (byte)((value >> 40) & 0xFF);
        b[1] = (byte)((value >> 48) & 0xFF);
        b[0] = (byte)((value >> 56) & 0xFF);

        return b;
        
    }

    public static long toLong( byte[] b ) {
        
        return (((((long) b[7]) & 0xFF)       ) +
                ((((long) b[6]) & 0xFF) << 8  ) +
                ((((long) b[5]) & 0xFF) << 16 ) +
                ((((long) b[4]) & 0xFF) << 24 ) +
                ((((long) b[3]) & 0xFF) << 32 ) +
                ((((long) b[2]) & 0xFF) << 40 ) +
                ((((long) b[1]) & 0xFF) << 48 ) +
                ((((long) b[0]) & 0xFF) << 56 ));

    }    

}
