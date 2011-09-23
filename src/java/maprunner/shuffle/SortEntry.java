
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public final class SortEntry implements SortRecord {

    public List<byte[]> values = new ArrayList();

    public long keycmp;

    public byte[] key;
    
    public SortEntry( SortRecord record ) {

        Tuple t = (Tuple)record;
        this.keycmp = t.keycmp;
        this.key = t.key;
        
    }

    public SortEntry() {}
    
    public long longValue() {
        return keycmp;
    }

    public long compareTo( SortRecord record ) {
        return longValue() - record.longValue();
    }

    public String toString() {

        List<Integer> list = new ArrayList();
        for( byte[] value : values ) {
            list.add( new IntValue( value ).value );
        }
        
        return String.format( "%s=%s", new IntKey( key ).value, list );

    }
    
}

