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
        _test( 100000 );
        //_test( 30000000 );
    }

    /**
     * test running with two lists which each have different values.
     */
    public void _test( int max ) throws Exception {

        int port = FSDaemon.PORT;
        int nr_replicas = 3;

        if ( hosts == null ) {

            hosts = new ArrayList();
            
            for( int i = 0; i < nr_replicas; ++i ) {

                String root = String.format( "%s/%s", Config.PFS_ROOT, port );
                
                FSDaemon daemon = new FSDaemon( root, port );

                Host host = new Host( "localhost", i, port );

                hosts.add( host );

                ++port;

            }

        }

        System.out.printf( "Running with hosts: %s\n", hosts );

        Config.addPartitionMembership( 0, hosts );

        Partition part = new Partition( 0 );
        
        String path = "/test/parallel-test";
        
        NewPartitionWriter writer = new NewPartitionWriter( part, path );

        int computed_written = 0;
        
        for( int i = 0; i < max; ++i ) {

            byte[] key = new StructWriter()
                .writeVarint( i )
                .toBytes()
                ;

            byte[] value = key;

            writer.write( key, value );

        }

        System.out.printf( "sleeping\n" );
        Thread.sleep( 300000L );
        
        //writer.close();

        // FIXME: ok... now verify the SHA1 of all these files and make sure
        // they are the same. 
        
    }

    public static void main( String[] args ) throws Exception {

        int max = 100000;

        if ( args.length > 0 ) 
            max = Integer.parseInt( args[ 0 ] );

        TestParallelFileSystemWrites t = new TestParallelFileSystemWrites();

        /*
        t.hosts = new ArrayList();

        t.hosts.add( new Host( "dev3.wdc.sl.spinn3r.com", 11112 ) );
        t.hosts.add( new Host( "util0029.wdc.sl.spinn3r.com", 11112 ) );
        t.hosts.add( new Host( "util0030.wdc.sl.spinn3r.com", 11112 ) );
        */
        
        t.setUp();
        t._test( max );

        //t.tearDown();

        System.exit( 0 );
        
    }

}
