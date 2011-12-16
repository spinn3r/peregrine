package peregrine.io.partition;

import java.io.*;
import java.util.*;
import peregrine.*;
import peregrine.values.*;
import peregrine.config.Partition;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;

public class TestDefaultPartitionWriter extends peregrine.BaseTestWithMultipleConfigs {

    public static int[] TESTS = new int[] { 0, 1, 2, 3 , 10, 100, 1000 , 10000 };

    @Override
    public void doTest() throws Exception {

        for( int test : TESTS ) {
            doTest( test );
        }
        
    }

    public void doTest( int max ) throws Exception {

        String path = "/tmp/test";

        // STEP 1... make a new file and write lots of chunks to it.
        
        config.setChunkSize( 1000 );

        Partition part = new Partition( 0 );
        
        PartitionWriter writer = new DefaultPartitionWriter( config, part, path );
        
        for ( int i = 0; i < max; ++i ) {

        	StructReader key = StructReaders.varint( i );
                
        	StructReader value = key;

            writer.write( key, value );
            
        }

        writer.close();

        // STEP 2: make sure we have LOTS of chunks on disk.
        
        List<DefaultChunkReader> readers = LocalPartition.getChunkReaders( configs.get( 0 ), part, path );

        System.out.printf( "We have %,d readers\n", readers.size() );
        
        assertTrue( readers.size() >= 1 ) ;

        // now create another PartitionWriter this time try to overwrite the
        // existing file and all chunks should be removed.
        
        writer = new DefaultPartitionWriter( config, part, path );
        writer.close();

        readers = LocalPartition.getChunkReaders( config, part, path );

        System.out.printf( "We have %,d readers\n", readers.size() );
        
        assertEquals( 0, readers.size() ) ;
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
