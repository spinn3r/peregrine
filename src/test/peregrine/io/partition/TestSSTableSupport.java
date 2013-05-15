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
import peregrine.client.ScanRequest;
import peregrine.config.*;
import peregrine.io.sstable.*;
import peregrine.io.sstable.Scanner;
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

        for( long i = 0; i < max; ++i ) {

            GetBackendRequest getBackendRequest = new GetBackendRequest(clientBackendRequest, StructReaders.wrap( i ) );
            Record record = reader.seekTo( getBackendRequest );

            assertNotNull( record );

        }

        assertNull( reader.findDataBlockReference( StructReaders.wrap( Long.MAX_VALUE ) ) );
        assertNull( reader.findDataBlockReference( StructReaders.wrap( (long)max * 2 ) ) );
        assertNull( reader.findDataBlockReference( StructReaders.wrap( (long)max ) ) );

        if ( max > 0 ) {
            assertNotNull( reader.findDataBlockReference( StructReaders.wrap( (long)max-1 ) ) );
        }

        final AtomicInteger result = new AtomicInteger();

        List<BackendRequest> backendRequests = new ArrayList<BackendRequest>();

        for( StructReader key : range( 0, max - 1 ) ) {

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

    private void doTestScan( ScanRequest scanRequest, int max, List<StructReader> keys ) throws Exception {

        String path = "/tmp/test.sstable";

        writeSSTable( path, max );

        Partition part = new Partition( 0 );

        ClientBackendRequest clientBackendRequest = new ClientBackendRequest( part, path );

        scanRequest.setClient(clientBackendRequest);

        SSTableReader reader = new LocalPartitionReader( configs.get( 0 ), part, path );

        Scanner scanner = new Scanner( reader );

        final List<StructReader> found = new ArrayList();

        scanner.scan(scanRequest, new RecordListener() {

                @Override
                public void onRecord( BackendRequest clientRequest, StructReader key, StructReader value ) {

                    System.out.printf( "  scanRequest.onRecord: key=%s\n", Hex.encode( key ) );

                    found.add( key );

                }

            } );

        assertEquals( found , keys );

        reader.close();

    }

    private void doTestScan() throws Exception {

        // we have to test the powerset of all possible options
        //
        // no start
        // start inclusive
        // start exclusive
        //
        // no end
        // end inclusive
        // end exclusive
        //
        // beginning of chunk reader
        // empty chunk reader
        // at end of chunk reader

        // FIXME: large LIMIT ... 
        
        ScanRequest scanRequest;
        
        // ********* no start / no end.

        scanRequest = new ScanRequest();
        scanRequest.setLimit( 10 );
        doTestScan(scanRequest, 1000, range( 0, 9 ) );

        // ********* no start / end inclusive
        scanRequest = new ScanRequest();
        scanRequest.setEnd( StructReaders.wrap( 1L ), true );
        scanRequest.setLimit( 10 );
        doTestScan(scanRequest, 1000, range( 0, 1 ) );

        // ********* no start / end exclusive

        scanRequest = new ScanRequest();
        scanRequest.setEnd( StructReaders.wrap( 1L ), false );
        scanRequest.setLimit( 10 );
        doTestScan(scanRequest, 1000, range( 0, 0 ) );

        // ********* start inclusive / no end
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 0L ), true );
        scanRequest.setLimit( 10 );
        doTestScan(scanRequest, 1000, range( 0, 9 ) );

        // ********* start inclusive / end inclusive.
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 1L ), true );
        scanRequest.setEnd( StructReaders.wrap( 2L ), true );
        scanRequest.setLimit( 10 );
        doTestScan(scanRequest, 1000, range( 1, 2 ) );

        // ********* start inclusive / end exclusive
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 1L ), true );
        scanRequest.setEnd( StructReaders.wrap( 2L ), false );
        scanRequest.setLimit( 10 );
        doTestScan(scanRequest, 1000, range( 1, 1 ) );

        // ********* start exclusive / no end
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 0L ), false );
        scanRequest.setLimit( 10 );
        doTestScan(scanRequest, 1000, range( 1, 10 ) );

        // ********* start exclusive / end exclusive.
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 1L ), false );
        scanRequest.setEnd( StructReaders.wrap( 2L ), true );
        scanRequest.setLimit( 10 );
        doTestScan(scanRequest, 1000, range( 2, 2 ) );

        // ********* start exclusive / end exclusive
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 1L ), false );
        scanRequest.setEnd( StructReaders.wrap( 3L ), false );
        scanRequest.setLimit( 10 );
        doTestScan(scanRequest, 1000, range( 2, 2 ) );

    }

    public void doTest() throws Exception {

        doTestScan();

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
