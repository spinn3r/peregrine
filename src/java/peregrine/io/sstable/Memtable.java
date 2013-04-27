package peregrine.io.sstable;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.os.*;

/**
 * This is the same essential design used in LevelDB, Cassandra, and
 * HBase. Specificaly a skip list dictionary / map backing keys directly.
 */
public class Memtable {

    // the current key updated via next() or seekTo()
    private StructReader key = null;

    // the current value updated via next() or seekTo()
    private StructReader value = null;

    // the actual backing that stores the data for our memtable.
    private Map<byte[],byte[]> map
        = new ConcurrentSkipListMap( new ByteArrayComparator() );
    
    public boolean seekTo( StructReader key ) {

        // reset the current key and value because they are invalid if this
        // seekTo fails.
        this.key = null;
        this.value = null;
        
        byte[] value = map.get( key.toByteArray() );

        if ( value != null ) {

            this.key = key;
            this.value = new StructReader( value );
            
            return true;
            
        }
        
        return false;
        
    }

    public void write( StructReader key, StructReader value ) throws IOException {
        map.put( key.toByteArray(), value.toByteArray() );
    }

    class ByteArrayComparator implements Comparator<byte[]> {

        public int compare( byte[] v1, byte[] v2 ) {

            int n = Math.min(v1.length, v2.length);

            int i = 0;
            int j = 0;

            int k = i;
            final int lim = n + i;
            while (k < lim) {
                byte c1 = v1[k];
                byte c2 = v2[k];
                if (c1 != c2) {
                    return c1 - c2;
                }
                k++;
            }
            
            return v1.length - v2.length;
        }

    }

    public static void main( String[] args ) throws Exception {

        Memtable memtable = new Memtable();

        int max = 100;
        
        for( long i = 0; i < max; i = i + 2 ) {
            memtable.write( StructReaders.wrap( i ), StructReaders.wrap( i ) );
        }

        int hits = 0;

        for( long i = 0; i < max; i = i + 2 ) {

            StructReader key = StructReaders.wrap( i );

            if ( memtable.seekTo( key ) ) {
                ++hits;
            }
            
        }

        System.out.printf( "hits: %,d for max %,d\n", hits, max );

    }

}