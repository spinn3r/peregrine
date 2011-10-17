package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.util.*;
import peregrine.reduce.*;
import peregrine.keys.*;
import peregrine.io.partition.*;

/**
 *
 * 
 */
public class LocalMerger {

    private FilePriorityQueue queue;

    private FileReference last = null;

    private FileComparator comparator = new FileComparator();

    private List<LocalPartitionReader> readers;
    
    public LocalMerger( List<LocalPartitionReader> readers )
        throws IOException {

        this.readers = readers;
        
        this.queue = new FilePriorityQueue();
        
        int id = 0;
        for( LocalPartitionReader reader : readers ) {

            System.out.printf( "merging with %s as %s\n", reader, id );
            
            FileReference ref = new FileReference( id++, reader );
            queue.add( ref );
            
        }

    }
    
    public JoinedTuple next() throws IOException {

        int nr_readers = readers.size();
        
        byte[][] joined = new byte[nr_readers][];
        
        while( true ) {

            FileReference ref = queue.poll();

            try {

                if ( ref == null && last == null )
                    return null;

                if ( last != null )
                    joined[last.id] = last.value;

                boolean changed = ref == null || ( last != null && comparator.compare( last, ref ) != 0 );
                
                if ( changed ) {
                    
                    JoinedTuple result = new JoinedTuple( last.key, joined );
                    joined = new byte[nr_readers][];
                    
                    return result;
                    
                }

            } finally {
                last = ref;
            }
            
        }
        
    }

}

class FilePriorityQueue {

    private PriorityQueue<FileReference> delegate = new PriorityQueue( 10, new FileComparator() );

    public void add( FileReference ref ) throws IOException {

        if ( ref.reader.hasNext() == false )
            return;
        
        ref.key   = ref.reader.key();
        ref.value = ref.reader.value();

        delegate.add( ref );

    }

    public FileReference poll() throws IOException {

        FileReference poll = delegate.poll();

        if ( poll == null )
            return null;

        FileReference result = new FileReference( poll.id, poll.key, poll.value );

        add( poll );

        return result;
        
    }
    
}

class FileReference {

    public byte[] key;

    public byte[] value;
    
    public int id = -1;
    protected LocalPartitionReader reader;
    
    public FileReference( int id, LocalPartitionReader reader ) {
        this.id = id;
        this.reader = reader;
    }

    public FileReference( int id, byte[] key, byte[] value ) {
        this.id = id;
        this.key = key;
        this.value = value;
    }

}

class FileComparator implements Comparator<FileReference> {

    //DepthBasedKeyComparator delegate = new DepthBasedKeyComparator();
    FullKeyComparator delegate = new FullKeyComparator();
    
    public int compare( FileReference r1, FileReference r2 ) {

        return delegate.compare( r1.key , r2.key );

    }

}