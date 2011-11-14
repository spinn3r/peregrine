package peregrine.http;

import java.io.*;
import java.net.*;

import peregrine.http.*;
import peregrine.pfsd.*;

public class TestFailedRequests extends peregrine.BaseTest {

    public void test1() throws Exception {

        String link = "http://localhost:8000/foo/bar";

        for ( int i = 0; i < 500; ++i ) {

            try {
                HttpClient client = new HttpClient( link );
                client.write( "hello world".getBytes() );
                client.close();
            } catch ( IOException e ) {
                System.out.printf( "x" );
            }

        }

        System.out.printf( "done\n" );
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}