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
package peregrine.io.partition;

import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.sstable.*;
import peregrine.util.*;
import peregrine.worker.clientd.requests.*;
import peregrine.worker.clientd.requests.GetBackendRequest;
import peregrine.worker.clientd.requests.ClientBackendRequest;

public class TestSSTableSupport extends peregrine.BaseTestWithMultipleProcesses {

    private void writeSSTable( String path, int max ) throws Exception {

        Partition part = new Partition( 0 );
        
        PartitionWriter writer = new DefaultPartitionWriter( config, part, path );
        
        for ( long i = 0; i < max; ++i ) {

        	StructReader key = StructReaders.wrap( i );
                
        	StructReader value = key;

            writer.write( key, value );
            
        }

        writer.close();

    }
    
    public void doTestSeekTo( int max ) throws Exception {

        String path = "/tmp/test";

        // STEP 1... make a new file and write lots of chunks to it.
        
        config.setChunkSize( 1000 );

        writeSSTable( path, max );

        Partition part = new Partition( 0 );

        LocalPartitionReader reader = new LocalPartitionReader( configs.get( 0 ), part, path );

        int found = 0;
        while( reader.hasNext() ) {

            reader.next();
            ++found;
            
            System.out.printf( "   %s\n", Hex.encode( reader.key() ) );
            
        }
        
        reader.close();

        assertEquals( max, found );

        // reopen it to test again.
        reader = new LocalPartitionReader( configs.get( 0 ), part, path );

        ClientBackendRequest clientBackendRequest = new ClientBackendRequest( part, path );

        for( long i = 1; i < max; ++i ) {

            GetBackendRequest getBackendRequest = new GetBackendRequest(clientBackendRequest, StructReaders.wrap( i ) );
            Record record = reader.seekTo( getBackendRequest );

            assertNotNull( record );

        }

        assertNull( reader.findIndexBlockReference(StructReaders.wrap(Long.MAX_VALUE)) );
        assertNull( reader.findIndexBlockReference(StructReaders.wrap((long) max * 2)) );
        assertNull( reader.findIndexBlockReference(StructReaders.wrap((long) max)) );

        if ( max > 0 ) {
            assertNotNull( reader.findIndexBlockReference(StructReaders.wrap((long) max - 1)) );
        }

        final AtomicInteger result = new AtomicInteger();

        List<BackendRequest> backendRequests = new ArrayList<BackendRequest>();

        for( StructReader key : StructReaders.range( 0, max - 1 ) ) {

            backendRequests.add( new GetBackendRequest(clientBackendRequest, key ) );

        }

        reader.seekTo( backendRequests, new RecordListener() {

                @Override
                public void onRecord( BackendRequest client, StructReader key, StructReader value ) {
                    result.getAndIncrement();
                }

            } );

        assertEquals( max, result.get() );
        
        reader.close();

    }

    public void doTest() throws Exception {

        doTestSeekTo( 0 );
        doTestSeekTo( 100 );
        doTestSeekTo( 1000 );
        doTestSeekTo( 10000 );

    }

    public static void main( String[] args ) throws Exception {
        System.setProperty( "peregrine.test.factor", "1" ); // 1m
        System.setProperty( "peregrine.test.config", "1:1:1" ); // takes 3 seconds
        runTests();
    }

}
