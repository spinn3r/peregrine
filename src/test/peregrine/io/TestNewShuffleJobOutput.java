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

    protected FSDaemon daemon0;
    protected FSDaemon daemon1;
    protected FSDaemon daemon2;
    
    public void setUp() {

        super.setUp();

        config = getConfig( 11112 );

        daemon0 = new FSDaemon( config );
        daemon1 = new FSDaemon( getConfig( 11113 ) );
        daemon2 = new FSDaemon( getConfig( 11114 ) );

    }

    private Config getConfig( int port ) {

        Config config = new Config();

        Host host = new Host( "localhost", port );
        
        config.setHost( host );

        config.addPartitionMembership( 0, new Host( "localhost", 11112 ) );
        config.addPartitionMembership( 1, new Host( "localhost", 11113 ) );
        config.addPartitionMembership( 2, new Host( "localhost", 11114 ) );

        config.setRoot( String.format( "%s/%s/%s" , Config.DEFAULT_ROOT, host.getName(), host.getPort() ) );
        
        return config;
        
    }
    
    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        ShuffleJobOutput output = new ShuffleJobOutput( config );

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

        daemon0.shufflerFactory.getInstance( "default" ).close();
        daemon1.shufflerFactory.getInstance( "default" ).close();
        daemon2.shufflerFactory.getInstance( "default" ).close();

        daemon0.shufflerFactory.getInstance( "default" ).close();
        daemon1.shufflerFactory.getInstance( "default" ).close();
        daemon2.shufflerFactory.getInstance( "default" ).close();

    }

    public static void main( String[] args ) throws Exception {

        TestNewShuffleJobOutput t = new TestNewShuffleJobOutput();
        t.setUp();
        t.test1();
        //t.tearDown();

        //Thread.sleep( 5000L );
        
    }

}
