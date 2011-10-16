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

public class TestShuffler extends peregrine.BaseTest {

    protected Config config;
    public void setUp() {

        config = new Config();
        config.setHost( new Host( "localhost" ) );

        // TRY with three partitions... 
        config.addMembership( 0, "localhost" );
        config.addMembership( 1, "localhost" );

    }
    
    public void test1() throws IOException {

        ShufflerFactory factory = new ShufflerFactory( config );
        
        Shuffler shuffler = factory.getInstance( "default" );

        int max_writes = 1000;
        int max_partitions = config.getMembership().size();

        for( int i = 0; i < max_writes; ++i ) {
        
            for( int j = 0 ; j < max_partitions; ++j ) {

                int from_partition = i;
                int from_chunk = i;
                int to_partition = j;
                
                shuffler.accept( from_partition, from_chunk, to_partition, new byte[2048] );

            }

        }

        shuffler.close();

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}