package peregrine.io.partition;

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

public class TestParallelFileSystemWrites extends peregrine.BaseTest {

    protected FSDaemon daemon = null;

    protected List<Host> hosts = null;
    
    public void test1() throws Exception {
        _test( 100 );
        //_test( 9000000 );
        //_test( 30000000 );
    }

    /**
     * test running with two lists which each have different values.
     */
    public void _test( int max ) throws Exception {

        long before = System.currentTimeMillis();
        
        int port = FSDaemon.PORT;
        int nr_replicas = 3;

        if ( hosts == null ) {

            hosts = new ArrayList();
            
            for( int i = 0; i < nr_replicas; ++i ) {

                Host host = new Host( "localhost", i, port );

                String root = String.format( "%s/%s/%s", Config.DEFAULT_ROOT, host.getName(), host.getPort() );

                Config config = new Config();
                config.setHost( host );
                config.setRoot( root );
                
                FSDaemon daemon = new FSDaemon( config );

                hosts.add( host );

                ++port;

            }

        }

        System.out.printf( "Running with hosts: %s\n", hosts );

        Config config = new Config();
        config.setHost( new Host( "localhost" ) );
        
        config.addPartitionMembership( 0, hosts );

        Partition part = new Partition( 0 );
        
        String path = "/test/parallel-test";
        
        DefaultPartitionWriter writer = new DefaultPartitionWriter( config, part, path );

        int computed_written = 0;

        for( int i = 0; i < max; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );

        }

        System.out.printf( "closing\n" );
        
        writer.close();

        long after = System.currentTimeMillis();

        System.out.printf( "duration: %,d ms\n", (after-before) );
        
        //System.out.printf( "sleeping\n" );
        //Thread.sleep( 300000L );

        // FIXME: ok... now verify the SHA1 of all these files and make sure
        // they are the same. 
        
    }

    public static void main( String[] args ) throws Exception {

        int max = 100000;

        if ( args.length >= 1 ) 
            max = Integer.parseInt( args[ 0 ] );

        TestParallelFileSystemWrites t = new TestParallelFileSystemWrites();

        if ( args.length >= 2 ) {

            t.hosts = new ArrayList();

            String[] hosts = args[1].split( "," );

            for( String host : hosts ) {
                t.hosts.add( new Host( host, 11112 ) );
            }

            
        }

        t.setUp();
        t._test( max );

        //t.tearDown();

        System.exit( 0 );
        
    }

}
