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
import peregrine.reduce.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;

public class TestPartitionWriter extends peregrine.BaseTest {

    protected Config config;
    
    public void setUp() {

        config = new Config();
        
        config.setHost( new Host( "localhost" ) );

        config.addMembership( 0, "localhost" );
        config.addMembership( 1, "localhost" );

    }

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        String path = "/tmp/test";
        
        PartitionWriter writer = new DefaultPartitionWriter( config, new Partition( 0 ), path );
        writer.close();

        Partition part = new Partition( 0 );
        Host host = config.getHost();

        List<ChunkReader> readers = LocalPartition.getChunkReaders( config, part, path );

        assertEquals( readers.size(), 1 ) ;

        System.out.printf( "worked.\n" );
        
    }

    public void test2() throws Exception {

        System.out.printf( "Running test2...\n" );
        
        remove( config.getRoot() );

        Partition part = new Partition( 0 );
        Host host = config.getHost();

        String path = "/tmp/test";

        // STEP 1... make a new file and write lots of chunks to it.
        
        DefaultPartitionWriter.CHUNK_SIZE = 1000;

        PartitionWriter writer = new DefaultPartitionWriter( config, new Partition( 0 ), path );

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
        
        List<ChunkReader> readers = LocalPartition.getChunkReaders( config, part, path );

        System.out.printf( "We have %,d readers\n", readers.size() );
        
        assertTrue( readers.size() > 1 ) ;

        // now create another PartitionWriter this time try to overwrite the
        // existing file and all chunks should be removed.
        
        writer = new DefaultPartitionWriter( config, new Partition( 0 ), path );
        writer.close();

        readers = LocalPartition.getChunkReaders( config, part, path );

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
        
        remove( config.getRoot() );

        Partition part = new Partition( 0 );
        Host host = config.getHost();

        String path = "/tmp/test";

        // **** STEP 1 ... make a new file and write lots of chunks to it.

        System.out.printf( "step1\n" );
        
        DefaultPartitionWriter.CHUNK_SIZE = 1000;

        PartitionWriter writer = new DefaultPartitionWriter( config, new Partition( 0 ), path );

        for ( int i = 0; i < max_per_round; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );
            
        }

        writer.close();

        System.out.printf( "BEFORE had %,d chunks\n",
                           LocalPartition.getChunkFiles( config, part, path ).size() );
        
        System.out.printf( "step2..\n" );
        
        // **** STEP 2 ok... do the SAME thing but this time in append mode.

        writer = new DefaultPartitionWriter( config, new Partition( 0 ), path, true );

        for ( int i = 0; i < max_per_round; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );
            
        }

        writer.close();

        System.out.printf( "AFTER had %,d chunks\n",
                           LocalPartition.getChunkFiles( config, part, path ).size() );

        // **** STEP 3 ok.... now READ all the values out and make sure we have 2 x 

        System.out.printf( "going to read now.\n" );
        
        LocalPartitionReader reader = new LocalPartitionReader( config, part, path );

        int count = 0;
        while( reader.hasNext() ) {

            try {
            
                byte[] key = reader.key();
                byte[] value = reader.value();
                
                int intval = new StructReader( value )
                    .readVarint();
                
                if ( count < 10000 )
                    assertEquals( intval, count );

            } catch ( Throwable t ) {
                throw new IOException( "Failed after reading N items: " + count, t );
            }
                
            ++count;
            
        }

        System.out.printf( "Read %,d entries from appended file.\n", count );
        
        reader.close();
        
        assertEquals( count, max_per_round * 2 );
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
