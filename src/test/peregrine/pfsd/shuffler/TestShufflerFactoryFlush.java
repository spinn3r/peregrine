package peregrine.pfsd.shuffler;

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
import peregrine.pfsd.shuffler.*;

import org.jboss.netty.handler.codec.http.*;

public class TestShufflerFactoryFlush extends peregrine.BaseTestWithTwoDaemons {

    public void test1() throws Exception {

        // now measure the flush time...

        QueryStringEncoder encoder = new QueryStringEncoder( "" );
        encoder.addParam( "action", "flush" );
        String query = encoder.toString();

        RemoteChunkWriterClient client = new RemoteChunkWriterClient( new URI( "http://localhost:11112/shuffler/RPC" ) );

        client.setMethod( HttpMethod.POST );
        client.write( query.getBytes() );
        client.close();
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}