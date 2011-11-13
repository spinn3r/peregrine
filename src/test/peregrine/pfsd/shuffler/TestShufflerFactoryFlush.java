package peregrine.pfsd.shuffler;

import java.net.*;

import peregrine.http.*;

import org.jboss.netty.handler.codec.http.*;

public class TestShufflerFactoryFlush extends peregrine.BaseTestWithTwoDaemons {

    public void test1() throws Exception {

        // now measure the flush time...

        QueryStringEncoder encoder = new QueryStringEncoder( "" );
        encoder.addParam( "action", "flush" );
        String query = encoder.toString();

        HttpClient client = new HttpClient( new URI( "http://localhost:11112/shuffler/RPC" ) );

        client.setMethod( HttpMethod.POST );
        client.write( query.getBytes() );
        client.close();
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}