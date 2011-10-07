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

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        int port = FSDaemon.PORT;
        int nr_replicas = 3;

        List<Host> hosts = new ArrayList();
        
        for( int i = 0; i < nr_replicas; ++i ) {

            String root = String.format( "%s/%s", Config.PFS_ROOT, port );
            
            FSDaemon daemon = new FSDaemon( root, port );

            Host host = new Host( "localhost", i, port );

            hosts.add( host );

            ++port;

        }

        System.out.printf( "Running with hosts: %s\n", hosts );

        Config.addPartitionMembership( 0, hosts );

        Partition part = new Partition( 0 );
        
        String path = "/test/parallel-test";
        
        NewPartitionWriter writer = new NewPartitionWriter( part, path );

        writer.close();
        
    }

    public static void main( String[] args ) throws Exception {
        
        TestParallelFileSystemWrites t = new TestParallelFileSystemWrites();
        t.setUp();
        t.test1();

        //t.tearDown();

        System.exit( 0 );
        
    }

}
