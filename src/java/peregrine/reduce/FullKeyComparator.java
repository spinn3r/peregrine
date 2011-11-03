
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

