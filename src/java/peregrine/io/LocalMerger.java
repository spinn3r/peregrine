package peregrine.io;

import java.io.*;
import java.util.*;
import peregrine.reduce.*;
import peregrine.values.*;
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

        if ( readers.size() == 0 )
            throw new IllegalArgumentException( "readers" );
        
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
        
        List<StructReader> joined = newEmptyList( nr_readers );
        
        while( true ) {

            FileReference ref = queue.poll();

            try {

                if ( ref == null && last == null )
                    return null;

                if ( last != null ) {
                    joined.set( last.id, last.value );
                }

                boolean changed = ref == null || ( last != null && comparator.compare( last, ref ) != 0 );
                
                if ( changed ) {
                    
                    JoinedTuple result = new JoinedTuple( last.key, joined );
                    joined = newEmptyList( nr_readers );
                    
                    return result;
                    
                }

            } finally {
                last = ref;
            }
            
        }
        
    }

    private List<StructReader> newEmptyList(int size) {

        List<StructReader> result = new ArrayList( size );

        for( int i = 0; i < size; ++i ) {
            result.add( null );
        }
        
        return result;
        
    }
    
}

class FilePriorityQueue {

    private PriorityQueue<FileReference> delegate = new PriorityQueue( 10, new FileComparator() );

    public void add( FileReference ref ) throws IOException {

        if ( ref.reader.hasNext() == false )
            return;
        
        ref.setKey( ref.reader.key() );
        ref.setValue( ref.reader.value() );
        
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

	public byte[] keyAsByteArray = null;;

    protected StructReader key = null;
    protected StructReader value = null;
    
    public int id = -1;
    protected LocalPartitionReader reader;
    
    public FileReference( int id, LocalPartitionReader reader ) {
        this.id = id;
        this.reader = reader;
    }

    public FileReference( int id, StructReader key, StructReader value ) {
        this.id = id;
        this.setKey( key );
        this.setValue( value );
    }

    public void setKey( StructReader key ) {
        this.keyAsByteArray = key.toByteArray();
        this.key = key;
    }

    public void setValue( StructReader value ) {
        this.value = value;
    }
    
}

class FileComparator implements Comparator<FileReference> {

    //DepthBasedKeyComparator delegate = new DepthBasedKeyComparator();
    FullKeyComparator delegate = new FullKeyComparator();
    
    public int compare( FileReference r1, FileReference r2 ) {

        return delegate.compare( r1.keyAsByteArray , r2.keyAsByteArray );

    }

}