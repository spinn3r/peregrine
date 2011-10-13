package peregrine.pfs;

import java.io.*;
import java.net.*;
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
import peregrine.pfs.*;
import peregrine.pfsd.*;

public class TestPipelineWrites extends peregrine.BaseTest {

    public void setUp() {

        super.setUp();

        new FSDaemon( newConfig( 11112 ) );
        new FSDaemon( newConfig( 11113 ) );

    }

    public Config newConfig( int port ) {

        Config config = new Config();

        config.setHost( new Host( "localhost", port ) );
        config.setRoot( String.format( "%s/localhost/%s", Config.DEFAULT_ROOT, port ) );
        
        return config;
    }
    
    /**
     * make sure the before and after 
     */
    public void test1() throws Exception {

        URI uri = new URI( "http://localhost:11112/test-write-hash" );

        RemoteChunkWriterClient client = new RemoteChunkWriterClient( uri );

        client.setHeader( "X-pipeline", "localhost:11113" );
        
        int block = 16384;
        
        long max = 2000 ;

        long nr_bytes = (long)max * (long)block;
        
        System.out.printf( "Writing %,d bytes.\n" , nr_bytes );

        MessageDigest digest = SHA1.getMessageDigest();
        
        for( int i = 0; i < max; ++i ) {

            byte[] b = new byte[block];
            
            for( int j = 0; j < block ; ++j ) {
                b[j] = (byte)i;
                
            }

            digest.update( b );
            
            client.write( b );

        }

        System.out.printf( "Wrote all bytes.\n" );
        
        client.close();

        byte[] digest_value = digest.digest();

        File file = new File( String.format( "%s/localhost/11112/test-write-hash", Config.DEFAULT_ROOT ) );

        byte[] data = toByteArray( new FileInputStream( file ) );

        String before = Hex.encode( digest_value );
        String after  = Hex.encode( SHA1.encode( data ) );

        System.out.printf( "before: %s\n", before );
        System.out.printf( "after:  %s\n", after );

        assertEquals( before , after );

    }

    public static void main( String[] args ) throws Exception {

        TestPipelineWrites t = new TestPipelineWrites();
        t.setUp();
        t.test1();
        t.tearDown();
        
    }

}
