
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

    private List<byte[]> data = new ArrayList();

    public SortEntry() {}

    public SortEntry( byte[] key ) {

        this.keycmp = LongBytes.toLong( key );
        this.key = key;
        
    }

    public long cmp( SortEntry entry ) {
        return keycmp - entry.keycmp;
    }

    public void write(byte[] data ) {
        len += data.length;
    }
    
    public byte[] getValue() {

        if ( data.size() == 1 )
            return data.get( 0 );
        
        byte[] result = new byte[len];

        int offset = 0;
        for( byte[] d : data ) {
            System.arraycopy( d, 0, result, offset, d.length );
            offset = d.length;
        }
        
        return result;

    }
    
}

