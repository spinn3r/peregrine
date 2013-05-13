package peregrine.io.sstable;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.client.ScanRequest;
import peregrine.io.*;

/**
 * This is the same essential design used in LevelDB, Cassandra, and
 * HBase. Specificaly a skip list dictionary / map backing keys directly.
 */
public class Memtable implements SSTableReader, SSTableWriter {

    //FIXME: this structure won't work because we CAN NOT just overwrite the
    //prevous value because we may wish to keep the last N value.  It would work
    //for some tables but not others.  However, the overhead of creating a LIST
    //of objects isn't easy either.
    //
    //FIXME: ok, a solution to this could be to have a LinkedList of internal
    //maps so that we can take the current value and stick it into the 'next'
    //map.  This way we only have a max of N maps do deal with AND the their
    //capacity is deterministic.  I do NOT like the issue where where we have
    //byte[] and List<byte[]> at the same time.
    
    //FIXME: use a MultiRecordReader to store entries.  It requries 1 byte
    //overhead PER entry.  We just store a varint which is the length of the
    //next record.  This way we can store N records per entry without much
    //overhead.

    // There is an 80 byte overhead for objects in the memtable.
    private static final long OVERHEAD_PER_RECORD = 80;

    // the current key updated via next() or seekTo()
    private StructReader key = null;

    // the current value updated via next() or seekTo()
    private StructReader value = null;

    // the actual backing that stores the data for our memtable.
    //private Map<byte[],byte[]> map
    //that     = new ConcurrentSkipListMap( new ByteArrayComparator() );

    private Map<byte[],byte[]> map
        = new TreeMap( new ByteArrayComparator() );
    
    // true if close() has been called.  We require that we are still open so
    // that writes don't completed after close()
    private boolean closed = false;

    // the number of raw bytes stored in the memtable.  This is the number of
    // bytes written to each key and value for all stored records.
    private long rawByteUsage = 0;

    // the iterator for the keys within this memtable.  This walks the keys
    // ascendign which works for drainTo when we write keys to disk.
    private Iterator<Map.Entry<byte[],byte[]>> iterator = null;

    // the number of entries within the map.  This is a near approximation
    // because technically the memtable could be used from multiple threads.
    // When used from a single thread it's completely accurate.  If we called
    // map.size() it wouldn't be a constant time operation and would slow down
    // performance of memoryUsage()
    private int size = 0;

    /**
     * Go to the first entry in the memtable so that next() starts from the
     * beginning.
     */
    public void first() {
        this.iterator = map.entrySet().iterator();
    }

    @Override
    public Record seekTo( StructReader key ) throws IOException {
        throw new RuntimeException( "FIXME: not implemented" );

    }

    @Override
    public boolean seekTo( List<StructReader> key, RecordListener listener ) throws IOException {

        // byte[] value = map.get( key.toByteArray() );

        // if ( value != null ) {
        //     return new Record( key, new StructReader( value ) );
        // }
        
        // return null;

        throw new RuntimeException( "FIXME: not implemented" );
        
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
        // directly because it will collide with another record.  It looked like
        // this was about 15% faster in practice.
        if ( map.containsKey( _key ) ) {
            rawByteUsage -= ( _key.length + map.get( _key ).length );
        } else {
            ++size;
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
        return size;
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

    // NOTE that this maintains strict byte ordering and not java signed byte
    // ordering.  In practice this DOES impose a slight performance penalty but
    // not a significant one.
    class ByteArrayComparator implements Comparator<byte[]> {

        public int compare( byte[] v1, byte[] v2 ) {

            final int lim = Math.min(v1.length, v2.length);

            int k = 0;
            while (k < lim) {
                int c1 = v1[k] & 0xFF;
                int c2 = v2[k] & 0xFF;
                if (c1 != c2) {
                    return c1 - c2;
                }
                k++;
            }
            
            return v1.length - v2.length;
        }

    }

}