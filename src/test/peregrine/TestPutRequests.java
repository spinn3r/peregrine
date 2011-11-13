package peregrine;

import java.net.*;

import peregrine.http.*;
import peregrine.pfsd.*;

public class TestPutRequests extends peregrine.BaseTestWithTwoDaemons {

    public void doTest( int max ) throws Exception {

        for( int i = 0; i < max; ++i ) {

            System.out.printf( "Writing %,d out of %,d\n", i , max );
            
            HttpClient client = new HttpClient( new URI( String.format( "http://localhost:11112/foo/%s", i ) ) );

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
            
            HttpClient client = new HttpClient( new URI( String.format( "http://localhost:11112/foo/%s", i ) ) );

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