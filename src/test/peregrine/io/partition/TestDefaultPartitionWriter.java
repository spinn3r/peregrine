package peregrine.io.partition;

import java.io.*;
import java.util.*;
import peregrine.*;
import peregrine.values.*;
import peregrine.config.Partition;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;

public class TestDefaultPartitionWriter extends peregrine.BaseTestWithMultipleConfigs {

    @Override
    public void doTest() throws Exception {

        String path = "/tmp/test";

        // STEP 1... make a new file and write lots of chunks to it.
        
        DefaultPartitionWriter.CHUNK_SIZE = 1000;

        Partition part = new Partition( 0 );
        
        PartitionWriter writer = new DefaultPartitionWriter( config, part, path );

        int max = 10000;
        
        for ( int i = 0; i < max; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );
            
        }

        writer.close();

        // STEP 2: make sure we have LOTS of chunks on disk.
        
        List<ChunkReader> readers = LocalPartition.getChunkReaders( configs.get( 0 ), part, path );

        System.out.printf( "We have %,d readers\n", readers.size() );
        
        assertTrue( readers.size() > 1 ) ;

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
