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
import peregrine.shuffle.*;

public class TestShuffleOutputWriter extends peregrine.BaseTest {

    protected Config config;
    public void setUp() {

        config = new Config();
        config.setHost( new Host( "localhost" ) );
        
        // TRY with three partitions... 
        config.addMembership( 0, "localhost" );
        config.addMembership( 1, "localhost" );

    }
    
    public void test1() throws IOException {

        String path = "/tmp/shuffle1.test";
        
        ShuffleOutputWriter buff = new ShuffleOutputWriter( config, path );

        int max_writes = 10;
        int max_partitions = config.getMembership().size();

        byte[] value = new byte[] { (byte)6, (byte)7, (byte)8, (byte)9 };

        for( int i = 0; i < max_writes; ++i ) {
        
            for( int j = 0 ; j < max_partitions; ++j ) {

                int from_partition = i;
                int from_chunk = i;
                int to_partition = j;

                buff.accept( from_partition, from_chunk, to_partition, 1, value );
                
            }

        }

        buff.close();

        System.out.printf( "Reading from path: %s\n", path );
        
        ShuffleInputReader reader = new ShuffleInputReader( path, 1 );

        int count = 0;
        while( reader.hasNext() ) {

            ShufflePacket2 pack = reader.next();

            System.out.printf( "from_partition: %s, from_chunk: %s, to_partition: %s, data length: %,d, data: %s \n",
                               pack.from_partition, pack.from_chunk, pack.to_partition, pack.data.capacity(), Hex.encode( pack.data ) );
            
            assertEquals( pack.to_partition, 1 );
            
            ++count;

        }
        
        assertEquals( max_writes, count );

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}