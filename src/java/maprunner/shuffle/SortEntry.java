
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public final class SortEntry {

    public List<byte[]> values = new ArrayList();

    public byte[] key;
    
    private long keycmp;

    public SortEntry( byte[] key ) {

        this.keycmp = LongBytes.toLong( key );
        this.key = key;
        
    }

    public SortEntry() {}

    public long cmp( SortEntry entry ) {
        return keycmp - entry.keycmp;
    }

    public String toString() {

        List<Integer> list = new ArrayList();
        for( byte[] value : values ) {
            list.add( new IntValue( value ).value );
        }
        
        return String.format( "%s=%s", new IntKey( key ).value, list );

    }
    
}

