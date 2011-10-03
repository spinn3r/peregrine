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
import peregrine.perf.*;

public class TestPartitionWriter extends peregrine.BaseTest {

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        //PartitionWriter 
        Config.addPartitionMembership( 0, "cpu0" );
        Config.addPartitionMembership( 1, "cpu1" );

        String path = "/tmp/test";
        
        PartitionWriter writer = new PartitionWriter( new Partition( 0 ), path );
        writer.close();

        Partition part = new Partition( 0 );
        Host host = new Host( "cpu0", 0, 0 );

        List<ChunkReader> readers = LocalPartition.getChunkReaders( part, host, path );

        assertEquals( readers.size(), 1 ) ;

        System.out.printf( "worked.\n" );
        
    }
    
    public static void main( String[] args ) throws Exception {

        TestPartitionWriter t = new TestPartitionWriter();
        t.test1();
        
    }

}
