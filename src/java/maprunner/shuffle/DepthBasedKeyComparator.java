
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

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