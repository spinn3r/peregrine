
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

public class FullKeyComparator implements Comparator<byte[]> {

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

