package maprunner.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.io.*;
import maprunner.util.*;
import maprunner.shuffle.*;
import maprunner.keys.*;

/**
 * 
 */
public class Joiner {

    private FilePriorityQueue queue;

    private FileReference last = null;

    private FileComparator comparator = new FileComparator();

    private List<LocalPartitionReader> readers;
    
    public Joiner( List<LocalPartitionReader> readers )
        throws IOException {

        this.readers = readers;
        
        this.queue = new FilePriorityQueue();
        
        int id = 0;
        for( LocalPartitionReader reader : readers ) {

            FileReference ref = new FileReference( id++, reader );
            queue.add( ref );
            
        }

    }
    
    public byte[][] next() throws IOException {

        int nr_readers = readers.size();
        
        byte[][] joined = new byte[nr_readers][];
        
        while( true ) {

            FileReference ref = queue.poll();

            boolean changed = ref == null || ( last != null && comparator.compare( last, ref ) != 0 );

            if ( changed ) {

                byte[][] result = joined;
                joined = new byte[nr_readers][];

                return result;
                
            }

            if ( ref == null )
                return null;

            joined[ref.id] = ref.key;

            last = ref;
            
        }
        
    }

    public static void write( PartitionWriter writer,
                              int v ) throws IOException {

        byte[] key = new IntKey( v ).toBytes();
        byte[] value = key;
        
        writer.write( key, value );
    }

}

class FilePriorityQueue {

    private PriorityQueue<FileReference> delegate = new PriorityQueue( 10, new FileComparator() );

    public void add( FileReference ref ) throws IOException {

        Tuple t = ref.reader.read();

        if ( t == null )
            return;

        ref.key = t.key;

        delegate.add( ref );

    }

    public FileReference poll() throws IOException {

        FileReference poll = delegate.poll();

        if ( poll == null )
            return null;
        
        FileReference result = new FileReference( poll.id, poll.key );
        
        add( poll );

        return result;
        
    }
    
}

class FileReference {

    public byte[] key;
    public int id = -1;
    protected LocalPartitionReader reader;
    
    public FileReference( int id, LocalPartitionReader reader ) {
        this.id = id;
        this.reader = reader;
    }

    public FileReference( int id, byte[] key ) {
        this.id = id;
        this.key = key;
    }

}

class FileComparator implements Comparator<FileReference> {

    private int offset = 0;
    public  int cmp;

    public int compare( FileReference r1, FileReference r2 ) {

        int key_length = r1.key.length;
        
        for( ; offset < key_length ; ++offset ) {

            cmp = r1.key[offset] - r2.key[offset];

            if ( cmp != 0 || offset == key_length - 1 ) {
                return cmp;
            }

        }
        
        return cmp;

    }

}