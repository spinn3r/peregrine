
package peregrine.reduce;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;

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