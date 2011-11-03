package peregrine.pfsd.shuffler;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;
import peregrine.io.partition.*;
import peregrine.pfsd.shuffler.*;
import peregrine.shuffle.receiver.*;

import org.jboss.netty.buffer.*;

public class TestShuffler extends peregrine.BaseTest {

    protected Config config;
    public void setUp() {

        super.setUp();
        
        config = new Config();
        config.setHost( new Host( "localhost" ) );

        config.setConcurrency( 2 );
        
        // TRY with three partitions... 
        config.getHosts().add( new Host( "localhost" ) );

        config.init();
        
    }
        
    public void test1() throws IOException {

        ShuffleReceiverFactory factory = new ShuffleReceiverFactory( config );
        
        ShuffleReceiver shuffleReceiver = factory.getInstance( "default" );

        int max_writes = 1000;
        int max_partitions = config.getMembership().size();

        for( int i = 0; i < max_writes; ++i ) {
        
            for( int j = 0 ; j < max_partitions; ++j ) {

                int from_partition = i;
                int from_chunk = i;
                int to_partition = j;
                
                shuffleReceiver.accept( from_partition, from_chunk, to_partition, 1, ChannelBuffers.wrappedBuffer( new byte[2048] ) );

            }

        }

        shuffleReceiver.close();

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}