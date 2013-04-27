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
public class Memtable implements SSTableReader, SSTableWriter {

    // There is an 80 byte overhead for objects in the memtable.
    private static final long OVERHEAD_PER_RECORD = 80;

    // the current key updated via next() or seekTo()
    private StructReader key = null;

    // the current value updated via next() or seekTo()
    private StructReader value = null;

    // the actual backing that stores the data for our memtable.
    private Map<byte[],byte[]> map
        = new ConcurrentSkipListMap( new ByteArrayComparator() );

    // true if close() has been called.  We require that we are still open so
    // that writes don't completed after close()
    private boolean closed = false;

    // the number of raw bytes stored in the memtable.  This is the number of
    // bytes written to each key and value for all stored records.
    private long rawByteUsage = 0;
    
    private Iterator<Map.Entry<byte[],byte[]>> iterator = null;

    public Memtable() {
        first();
    }

    /**
     * Go to the first entry in the memtable so that next() starts from the
     * beginning.
     */
    public void first() {
        this.iterator = map.entrySet().iterator();
    }
    
    @Override
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

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }
    
    @Override
    public void next() {

        Map.Entry<byte[],byte[]> entry = iterator.next();

        this.key   = new StructReader( entry.getKey() );
        this.value = new StructReader( entry.getValue() );
        
    }

    @Override
    public StructReader key() {
        return this.key;
    }

    @Override
    public StructReader value() {
        return this.value;
    }

    @Override
    public void write( StructReader key, StructReader value ) throws IOException {
        requireOpen();

        byte[] _key = key.toByteArray();
        byte[] _value = value.toByteArray();

        // TODO: it's not very efficient to do this with EVERY write (and will
        // hurt performance).  Ideally we would update rawByteUsage within put()
        // directly because it will collide with another record.
        if ( map.containsKey( _key ) ) {
            rawByteUsage -= ( _key.length + map.get( _key ).length );
        } 

        map.put( _key, _value );

        rawByteUsage += _key.length + _value.length;

    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    private void requireOpen() throws IOException {
        if ( closed ) throw new IOException( "closed" );
    }

    // compute estimated memory usage from the size of the internal skip list,
    // the number of bytes for each raw key and value, and skip list internal
    // overhead.
    public long memoryUsage() {

        return (size() * OVERHEAD_PER_RECORD) + rawByteUsage;

    }

    public int size() {
        return map.size();
    }
    
    /**
     * Take the current memtable data and drain/write it to the given
     * SequenceWriter.  
     */
    public void drainTo( SequenceWriter writer ) throws IOException {

        first();

        while( hasNext() ) {
            next();
            writer.write( key(), value() );
        }
        
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

        int found = 0;
        
        memtable.first();
        
        while( memtable.hasNext() ) {

            memtable.next();

            ++found;
            
        }

        System.err.printf( "found: %s\n", found );
        
    }

}