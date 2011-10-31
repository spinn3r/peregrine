package peregrine;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;
import peregrine.io.partition.*;
import peregrine.pfs.*;
import peregrine.pfsd.*;

public class TestPutRequests extends peregrine.BaseTestWithTwoDaemons {

    public void doTest( int max ) throws Exception {

        for( int i = 0; i < max; ++i ) {

            System.out.printf( "Writing %,d out of %,d\n", i , max );
            
            RemoteChunkWriterClient client = new RemoteChunkWriterClient( new URI( String.format( "http://localhost:11112/foo/%s", i ) ) );

            client.write( "xxxxxxxxxxxxxxxxxx".getBytes() );

            client.close();
            
        }

    }
    
    public void test1() throws Exception {

        doTest( 1000 );
    }

    public void test1WithHexPipeline() throws Exception {

        HexPipelineEncoder.ENABLED = true;
        
        doTest( 1000 );
    }

    public void test2() throws Exception {

        int max = 10;
        
        for( int i = 0; i < max; ++i ) {

            System.out.printf( "Writing %,d out of %,d\n", i , max );
            
            RemoteChunkWriterClient client = new RemoteChunkWriterClient( new URI( String.format( "http://localhost:11112/foo/%s", i ) ) );

            client.write( "xxxxxxxxxxxxxxxxxx".getBytes() );

            Thread.sleep( 1000L );
            
            client.write( "xxxxxxxxxxxxxxxxxx".getBytes() );

            client.close();
            
        }
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}