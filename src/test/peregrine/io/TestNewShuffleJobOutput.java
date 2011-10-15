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

    // FIXME: this should extend a common config.

    protected Config config;

    protected Config config0;
    protected Config config1;
    protected Config config2;

    protected FSDaemon daemon0;
    protected FSDaemon daemon1;
    protected FSDaemon daemon2;
    
    public void setUp() {

        super.setUp();

        config0 = getConfig( 11112 );
        config1 = getConfig( 11113 );
        config2 = getConfig( 11114 );

        daemon0 = new FSDaemon( config0 );
        daemon1 = new FSDaemon( config1 );
        daemon2 = new FSDaemon( config2 );

        // run with the first config.
        config = config0;
        
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

    private void doTestIter( int max_emits ) throws Exception {

        ShuffleJobOutput output = new ShuffleJobOutput( config );

        ChunkReference chunkRef = new ChunkReference( new Partition( 0  ) );
        chunkRef.local = 0;

        output.onChunk( chunkRef );

        for ( int i = 0; i < max_emits; ++i ) {

            byte[] key = new StructWriter()
                .writeHashcode( "" + i )
                .toBytes()
                ;

            byte[] value = key;

            output.emit( key, value );
            
        }

        output.onChunkEnd( chunkRef );

        output.close();

        Controller controller = new Controller( config );

        controller.flushAllShufflers();

    }

    public void doTest( int iterations, int max_emits ) throws Exception {

        assertEquals( config.getHosts().size(), 3 );

        System.out.printf( "Running with %,d hosts.\n", config.getHosts().size() );
        
        for( int i = 0; i < iterations; ++i ) {
            doTestIter( max_emits );
        }

    }
    
    public void test1() throws Exception {
        doTest( 20, 1 );
        doTest( 100, 3 );
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
