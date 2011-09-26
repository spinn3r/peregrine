
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

    public byte[] key;
    
    private long keycmp;

    private int len = 0;

    private List<byte[]> values = new ArrayList();

    public SortEntry() {}

    public SortEntry( byte[] key ) {

        this.keycmp = LongBytes.toLong( key );
        this.key = key;
        
    }

    public long cmp( SortEntry entry ) {
        return keycmp - entry.keycmp;
    }

    public void addValue( byte[] value ) {
        this.values.add( value );
    }

    public void addValues( List<byte[]> _values ) {
        this.values.addAll( _values );
    }
    
    public List<byte[]> getValues() {
        return this.values;
    }
    
}

