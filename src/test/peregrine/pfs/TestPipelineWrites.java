/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.pfs;

import java.io.*;
import java.net.*;
import java.security.*;

import peregrine.util.*;
import peregrine.worker.*;
import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.http.*;

public class TestPipelineWrites extends peregrine.BaseTest {

    Config config = null;

    public void setUp() {

        super.setUp();

        config = newConfig( 11112 );
        
        new FSDaemon( config );
        new FSDaemon( newConfig( 11113 ) );

    }

    public Config newConfig( int port ) {

        Config config = new Config();

        config.setHost( new Host( "localhost", port ) );
        config.getHosts().add( config.getHost() );
        
        config.init();
        
        return config;
    }
    
    /**
     * make sure the before and after 
     */
    public void test1() throws Exception {

        URI uri = new URI( "http://localhost:11112/test-write-hash" );

        HttpClient client = new HttpClient( uri );

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

        File file = new File( String.format( "%s/localhost/11112/test-write-hash", config.getBasedir() ) );

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
