package maprunner;

import maprunner.util.*;
import maprunner.keys.*;
import maprunner.shuffle.SortRecord;

public class Tuple implements Comparable, SortRecord {

    public byte[] key = null;
    public byte[] value = null;

    private long keycmp = -1;

    public Tuple( byte[] key, byte[] value ) {

        this.key = key;
        this.value = value;
        this.keycmp = Hashcode.toLong( key );
        
    }

    public Tuple( Key key, Value value ) {
        this.key = key.toBytes();
        this.value = value.toBytes();

        if ( key instanceof IntKey )
            this.keycmp = ((IntKey)key).value;
        
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
