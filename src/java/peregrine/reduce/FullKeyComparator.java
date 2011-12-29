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
package peregrine.reduce;

import java.util.*;

public class FullKeyComparator implements Comparator<byte[]> {

    /**
     * Compares its two arguments for order. Returns a negative integer, zero,
     * or a positive integer as the first argument is less than, equal to, or
     * greater than the second.
     * 
     */
    public int compare( byte[] k0, byte[] k1 ) {

        int len = k0.length;

        int diff = 0;
        
        for( int offset = 0; offset < len; ++offset ) {

            diff = k0[offset] - k1[offset];

            if ( diff != 0 )
                return diff;
            
        }

        return diff;
        
    }

}

