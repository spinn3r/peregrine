package peregrine.pfs;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;

public class TestEmptyWrites extends peregrine.TestWithTwoPartitions {

    public void test1() throws Exception {

        RemoteChunkWriterClient client =
            new RemoteChunkWriterClient( new URI( "http://localhost:11112/0/shuffle/nr_nodes/from-partition/0/from-chunk/0" ) );
        
        client.close();
        
    }

    public static void main( String[] args ) throws Exception {
        org.junit.runner.JUnitCore.main( TestEmptyWrites.class.getName() );
    }

}