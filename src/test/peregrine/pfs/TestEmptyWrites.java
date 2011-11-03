package peregrine.pfs;

import java.net.*;

public class TestEmptyWrites extends peregrine.BaseTestWithTwoPartitions {

    public void test1() throws Exception {

        RemoteChunkWriterClient client =
            new RemoteChunkWriterClient( new URI( "http://localhost:11112/0/shuffle/nr_nodes/from-partition/0/from-chunk/0" ) );
        
        client.close();
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}