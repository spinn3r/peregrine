
package peregrine.reduce;

import java.util.*;

public class DepthBasedKeyComparator implements Comparator<byte[]> {

    private int offset = 0;

    private int cmp;
    
    public int compare( byte[] k0, byte[] k1 ) {
                
        for( ; offset < k0.length ; ++offset ) {

            cmp = k0[offset] - k1[offset];

            if ( cmp != 0 || offset == k0.length - 1 ) {
                return cmp;
            }

        }
        
        return cmp;

    }

}