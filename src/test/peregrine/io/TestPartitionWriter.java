package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;
import peregrine.perf.*;

public class TestPartitionWriter extends peregrine.BaseTest {

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        String path = "/tmp/test";
        
        PartitionWriter writer = new PartitionWriter( new Partition( 0 ), path );
        writer.close();

        Partition part = new Partition( 0 );
        Host host = new Host( "cpu0", 0, 0 );

        List<ChunkReader> readers = LocalPartition.getChunkReaders( part, host, path );

        assertEquals( readers.size(), 1 ) ;

        System.out.printf( "worked.\n" );
        
    }

    public void test2() throws Exception {

        System.out.printf( "Running test2...\n" );
        
        DiskPerf.remove( Config.DFS_ROOT );

        Partition part = new Partition( 0 );
        Host host = new Host( "cpu0", 0, 0 );

        String path = "/tmp/test";

        // STEP 1... make a new file and write lots of chunks to it.
        
        LocalPartitionWriter.CHUNK_SIZE = 1000;

        PartitionWriter writer = new PartitionWriter( new Partition( 0 ), path );

        for ( int i = 0; i < 10000; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );
            
        }

        writer.close();

        // STEP 2: make sure we have LOTS of chunks.
        
        List<ChunkReader> readers = LocalPartition.getChunkReaders( part, host, path );

        System.out.printf( "We have %,d readers\n", readers.size() );
        
        assertTrue( readers.size() > 1 ) ;

        // now create another PartitionWriter this time try to overwrite the
        // existing file and all chunks should be removed.
        
        writer = new PartitionWriter( new Partition( 0 ), path );
        writer.close();

        readers = LocalPartition.getChunkReaders( part, host, path );

        System.out.printf( "We have %,d readers\n", readers.size() );
        
        assertEquals( readers.size(), 1 ) ;

        // now create a partition writer which should in theory span a few
        // chunks.

        // then try to overwrite it so that it only has one chunk now...

        // make sure we only have one chunk on disk.
        
    }

    public void test3() throws Exception {

        int max_per_round = 10000;
        
        System.out.printf( "Running test3...\n" );
        
        DiskPerf.remove( Config.DFS_ROOT );

        Partition part = new Partition( 0 );
        Host host = new Host( "cpu0", 0, 0 );

        String path = "/tmp/test";

        // **** STEP 1 ... make a new file and write lots of chunks to it.
        
        LocalPartitionWriter.CHUNK_SIZE = 1000;

        PartitionWriter writer = new PartitionWriter( new Partition( 0 ), path );

        for ( int i = 0; i < max_per_round; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );
            
        }

        writer.close();

        // **** STEP 2 ok... do the SAME thing but this time in append mode.

        writer = new PartitionWriter( new Partition( 0 ), path, true );

        for ( int i = 0; i < max_per_round; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );
            
        }

        writer.close();

        // **** STEP 3 ok.... now READ all the values out and make sure we have 2 x 

        LocalPartitionReader reader = new LocalPartitionReader( part, host, path );

        int count = 0;
        while( true ) {

            Tuple t = reader.read();

            if ( t == null )
                break;

            ++count;
            
        }

        System.out.printf( "Read %,d entries from appended file.\n", count );
        
        reader.close();
        
        assertEquals( count, max_per_round * 2 );
        
    }

    public void setUp() {

        super.setUp();

        //PartitionWriter 
        Config.addPartitionMembership( 0, "cpu0" );
        Config.addPartitionMembership( 1, "cpu1" );

    }

    public static void main( String[] args ) throws Exception {

        TestPartitionWriter t = new TestPartitionWriter();
        //System.out.printf( "%s\n", t.run() );

        t.setUp();
        t.test3();
        //t.tearDown();
        
        //t.test1();
        
    }

}
