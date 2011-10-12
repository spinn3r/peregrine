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
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.pfsd.*;

public class TestNewShuffleJobOutput extends peregrine.BaseTest {

    protected Config config;
    
    public void setUp() {

        super.setUp();

        config = getConfig();

        Config config11112 = getConfig();
        Config config11113 = getConfig();

        config11112.getHost().setPort( 11112 );
        config11113.getHost().setPort( 11113 );
        
        new FSDaemon( config11112 );
        new FSDaemon( config11113 );

    }

    private Config getConfig() {

        Config config = new Config();

        config.setHost( new Host( "localhost" ) );
        
        config.addPartitionMembership( 0, new Host( "localhost", 11112 ) );
        config.addPartitionMembership( 1, new Host( "localhost", 11113 ) );

        return config;
        
    }
    
    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        NewShuffleJobOutput output = new NewShuffleJobOutput( config );

        ChunkReference chunkRef = new ChunkReference( new Partition( 0  ) );
        chunkRef.local = 0;

        output.onChunk( chunkRef );

        for ( int i = 0; i < 100; ++i ) {

            byte[] key = new StructWriter()
                .writeHashcode( "" + i )
                .toBytes()
                ;

            byte[] value = key;

            output.emit( key, value );
            
        }

        output.onChunkEnd( chunkRef );

        output.close();

        // now try to read the entries back out once it is shuffled...

    }

    public static void main( String[] args ) throws Exception {

        TestNewShuffleJobOutput t = new TestNewShuffleJobOutput();
        t.setUp();
        t.test1();
        //t.tearDown();
        
    }

}
