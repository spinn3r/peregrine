package maprunner;

import maprunner.util.*;

public class Tuple implements Comparable {

    protected byte[] key = null;
    protected byte[] value = null;

    private long keycmp = -1;

    public Tuple( byte[] key, byte[] value ) {
        keycmp = Hashcode.toLong( key );
    }

    public int compareTo( Object o ) {

        long result = keycmp - ((Tuple)o).keycmp;

        // TODO: is there a faster way to do this?
        if ( result < 0 )
            return -1;
        else if ( result > 0 )
            return 1;

        return 0;
        
    }
    
}
