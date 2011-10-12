package peregrine.pfsd.shuffler;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;
import peregrine.io.partition.*;
import peregrine.pfsd.shuffler.*;

public class TestShuffleOutputWriter extends peregrine.BaseTest {

    protected Config config;
    public void setUp() {

        config = new Config();
        config.setHost( new Host( "localhost" ) );
        
        // TRY with three partitions... 
        config.addPartitionMembership( 0, "localhost" );
        config.addPartitionMembership( 1, "localhost" );

    }
    
    public void test1() throws IOException {

        String path = "/tmp/shuffle1.test";
        
        ShuffleOutputWriter buff = new ShuffleOutputWriter( config, path );

        int max_writes = 1000;
        int max_partitions = config.getPartitionMembership().size();

        for( int i = 0; i < max_writes; ++i ) {
        
            for( int j = 0 ; j < max_partitions; ++j ) {

                int from_partition = i;
                int from_chunk = i;
                int to_partition = j;
                
                buff.accept( from_partition, from_chunk, to_partition, new byte[2048] );
                
            }

        }

        buff.close();

        ShuffleInputReader reader = new ShuffleInputReader( path, 1 );

        int count = 0;
        while( reader.hasNext() ) {
            ShufflePacket pack = reader.next();
            ++count;
        }

        assertEquals( max_writes, count );

    }

    public static void main( String[] args ) throws Exception {

        TestShuffleOutputWriter test = new TestShuffleOutputWriter();
        test.setUp();
        test.test1();
        test.tearDown();
        
    }

}