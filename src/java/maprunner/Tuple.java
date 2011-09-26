package maprunner;

import maprunner.util.*;
import maprunner.keys.*;
import maprunner.shuffle.SortRecord;

public class Tuple implements SortRecord, Comparable {

    public byte[] key = null;
    public byte[] value = null;

    public long keycmp = -1;

    public Tuple( byte[] key, byte[] value ) {

        this.key = key;
        this.value = value;
        this.keycmp = LongBytes.toLong( key );
        
    }

    public Tuple( Key key, Value value ) {
        this.key = key.toBytes();
        this.value = value.toBytes();

        if ( key instanceof IntKey )
            this.keycmp = ((IntKey)key).value;
        
    }

    public int compareTo( Object obj ) {

        //FIXME: remove this once I stop using Arrays.sort()
        
        long result = longValue() - ((Tuple)obj).longValue();

        if ( result < 0 )
            return -1;
        else if ( result > 0 )
            return 1;

        return 0;

    }

    public long longValue() {
        return keycmp;
    }

    public String toString() {
        return String.format( "%s" , new IntKey( key ).value );
    }

}
