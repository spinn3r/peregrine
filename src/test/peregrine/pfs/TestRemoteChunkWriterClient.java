package peregrine.pfs;

import java.io.*;
import java.net.*;
import java.security.*;

import peregrine.util.*;
import peregrine.config.Config;

public class TestRemoteChunkWriterClient extends peregrine.BaseTestWithMultipleConfigs {

    public static int[] TEST = new int[] { 0, 1, 2, 4, 8, 16, 32 };

    @Override
    public void doTest() throws Exception {

        for( int test : TEST ) {
            doTest( test );
        }
        
    }

    public void doTest( int max ) throws Exception {

        URI uri = new URI( "http://localhost:11112/test-write-hash" );

        RemoteChunkWriterClient client = new RemoteChunkWriterClient( uri );

        int block = 16384;
        
        long max = 100;

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
        runTests();
    }

}
