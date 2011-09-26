
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

//     public void write(byte[] d ) {

//         len += d.length;
//         data.add( d );
//     }
    
//     public byte[] getValue() {

//         if ( data.size() == 1 )
//             return data.get( 0 );
        
//         byte[] result = new byte[len];

//         System.out.printf( "FIXME: len: %d\n" , len );
//         int offset = 0;
//         for( byte[] d : data ) {
//             System.out.printf( "FIXME: before: %s\n", Hex.encode( d ) );
            
//             System.arraycopy( d, 0, result, offset, d.length );
//             offset = d.length;
//         }

//         System.out.printf( "FIXME: after: %s\n", Hex.encode( result ) );

//         return result;

//     }
    
}

