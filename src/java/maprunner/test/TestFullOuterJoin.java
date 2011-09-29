package maprunner.test;

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
public class TestFullOuterJoin {

    public static void main( String[] args ) throws Exception {

        //write keys to two files but where there isn't a 100%
        //intersection... then try to join against these files. 

        Config.addPartitionMembership( 0, "cpu0" );

        //now test writing two regions to a file and see if both sides of the
        //join are applied correctly

        Partition part = new Partition( 0 );
        Host host = new Host( "cpu0", 0, 0 );
        
        PartitionWriter writer;

        writer = new PartitionWriter( part, "/tmp/left" );

        write( writer, 1 );
        write( writer, 2 );
        write( writer, 3 );
        write( writer, 4 );
        write( writer, 5 );

        writer.close();

        writer = new PartitionWriter( part, "/tmp/right" );

        write( writer, 4 );
        write( writer, 5 );
        write( writer, 6 );
        write( writer, 7 );
        write( writer, 8 );

        writer.close();

        int nr_files = 2;

        List<String> files = new ArrayList();
        files.add( "/tmp/left" );
        files.add( "/tmp/right" );

        FilePriorityQueue queue = new FilePriorityQueue();
        
        int id = 0;
        for( String file : files ) {

            LocalPartitionReader reader = new LocalPartitionReader( part, host, file );
            
            FileReference ref = new FileReference( id, reader );
            queue.add( ref );
            
            ++id;
            
        }

        FileReference last = null;
        FileComparator comparator = new FileComparator();
        byte[][] joined = new byte[2][];
        
        while( true ) {

            FileReference ref = queue.poll();

            boolean changed = ref == null || ( last != null && comparator.compare( last, ref ) != 0 );

            if ( ref != null )
                System.out.printf( "ref=%s cmp=%d\n", Hex.encode( ref.key ), comparator.cmp );
            
            if ( changed ) {

                // run the map job against this value.
                System.out.printf( "0=%s, 1=%s\n", Hex.encode( joined[0] ), Hex.encode( joined[1] ) );

                joined = new byte[2][];

            }

            if ( ref == null )
                break;

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