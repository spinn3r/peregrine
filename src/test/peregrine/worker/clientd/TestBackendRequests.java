/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package peregrine.worker.clientd;

import peregrine.BaseTest;
import peregrine.Record;
import peregrine.StructReader;
import peregrine.StructReaders;
import peregrine.client.*;
import peregrine.config.Config;
import peregrine.config.ConfigParser;
import peregrine.config.Partition;
import peregrine.io.ExtractWriter;
import peregrine.io.chunk.DefaultChunkWriter;
import peregrine.io.partition.DefaultPartitionWriter;
import peregrine.io.partition.LocalPartitionReader;
import peregrine.io.sstable.RecordListener;
import peregrine.util.Hex;
import peregrine.worker.clientd.requests.BackendRequest;
import peregrine.worker.clientd.requests.ClientBackendRequest;
import peregrine.worker.clientd.requests.GetBackendRequest;
import peregrine.worker.clientd.requests.ScanBackendRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test backend requests against
 */
public class TestBackendRequests extends BaseTest {

    long max = 50;
    List<StructReader> keys = range( 0, max-1 );
    Partition partition = new Partition(0);
    Connection conn = new Connection( "http://localhost:11112" );
    String path = String.format( "/test/%s/test1.sstable", getClass().getName() );

    private Config config;

    private void doSetup(List<StructReader> keys) throws Exception {

        DefaultPartitionWriter.ENABLE_LOCAL_DELEGATE = true;

        ExtractWriter writer = new ExtractWriter( config, path );

        System.out.printf( "Writing keys: \n");

        for( StructReader key : keys ) {
            System.out.printf( "    %s\n", Hex.encode(key) );
            writer.write( key, StructReaders.wrap("xxxx") );
        }

        writer.close();

        System.out.printf( "Wrote %,d keys to %s", keys.size(), path );

    }
    private void doTestGetRequests( List<StructReader> keys, long nrExpectedRecords ) throws IOException {

        List<BackendRequest> requests = new ArrayList<BackendRequest>();

        ClientBackendRequest clientBackendRequest = new ClientBackendRequest(partition,path);

        System.out.printf( "Creating get requests: \n");

        for( StructReader key : keys ) {

            System.out.printf( "    %s\n", Hex.encode(key) );

            GetBackendRequest getBackendRequest = new GetBackendRequest( clientBackendRequest, key );
            requests.add( getBackendRequest );
        }

        doExec( requests, range( 0, 49 ) );

    }

    private ScanBackendRequest doTestScanRequests( ScanRequest scanRequest, long max, List<StructReader> expectedKeys ) throws IOException {
        //now try with a scan.  This should be fun!

        scanRequest.setSource( path );
        scanRequest.setPartition(partition);

        ClientBackendRequest clientBackendRequest = new ClientBackendRequest(partition,path);

        ScanBackendRequest scanBackendRequest = new ScanBackendRequest( clientBackendRequest, scanRequest );

        doExec( scanBackendRequest, expectedKeys );

        return scanBackendRequest;

    }

    private void doTestScanRequests() throws IOException {

        ScanRequest scanRequest;

        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( max - 5 ), true );
        scanRequest.setLimit( 10 );

        ScanBackendRequest scanBackendRequest =
                doTestScanRequests(scanRequest, 1000, range(max-5, max-1));

        assertEquals( true, scanBackendRequest.isComplete() );

        // ********* start exclusive / no end
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 0L ), false );
        scanRequest.setLimit( 10 );
        doTestScanRequests(scanRequest, 1000, range( 1, 10 ) );

        // ********* start inclusive / end inclusive.
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 1L ), true );
        scanRequest.setEnd( StructReaders.wrap( 2L ), true );
        scanRequest.setLimit( 10 );
        doTestScanRequests(scanRequest, 1000, range( 1, 2 ) );


        // ********* no start / end inclusive
        scanRequest = new ScanRequest();
        scanRequest.setEnd( StructReaders.wrap( 1L ), true );
        scanRequest.setLimit( 10 );
        doTestScanRequests(scanRequest, 1000, range( 0, 1 ) );

        // ********* no start / no end.
        scanRequest = new ScanRequest();
        scanRequest.setLimit( 10 );
        doTestScanRequests(scanRequest, 1000, range( 0, 9 ) );

        // ********* no start / end exclusive

        scanRequest = new ScanRequest();
        scanRequest.setEnd( StructReaders.wrap( 1L ), false );
        scanRequest.setLimit( 10 );
        doTestScanRequests(scanRequest, 1000, range(0, 0));

        // ********* start inclusive / no end
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 0L ), true );
        scanRequest.setLimit( 10 );
        doTestScanRequests(scanRequest, 1000, range(0, 9));


        // ********* start inclusive / end exclusive
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 1L ), true );
        scanRequest.setEnd( StructReaders.wrap( 2L ), false );
        scanRequest.setLimit( 10 );
        doTestScanRequests(scanRequest, 1000, range(1, 1));

        // ********* start exclusive / end exclusive.
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 1L ), false );
        scanRequest.setEnd( StructReaders.wrap( 2L ), true );
        scanRequest.setLimit( 10 );
        doTestScanRequests(scanRequest, 1000, range(2, 2));

        // ********* start exclusive / end exclusive
        scanRequest = new ScanRequest();
        scanRequest.setStart( StructReaders.wrap( 1L ), false );
        scanRequest.setEnd( StructReaders.wrap( 3L ), false );
        scanRequest.setLimit( 10 );
        doTestScanRequests(scanRequest, 1000, range(2, 2));

        //test towards the end of the stream and make sure the ScanBackendRequest
        //is marked complete


    }

    private void doExec( BackendRequest request, List<StructReader> expectedKeys ) throws IOException {

        List<BackendRequest> requests = new ArrayList<BackendRequest>();
        requests.add( request );

        doExec( requests, expectedKeys );

    }

    private void doExec( List<BackendRequest> requests, List<StructReader> expectedKeys ) throws IOException {

        LocalPartitionReader reader = new LocalPartitionReader( config, partition, path );

        final List<Record> records = new ArrayList<Record>();

        reader.seekTo( requests, new RecordListener() {

            @Override
            public void onRecord( BackendRequest backendRequest, StructReader key, StructReader value ) {
                records.add( new Record( key, value ) );
            }

        } );

        assertResults( records, expectedKeys );

    }

    private void assertResults( List<Record> records, List<StructReader> expectedKeys ) {

        System.out.printf( "Found records: \n");

        List<StructReader> keys = new ArrayList<StructReader>();

        for( Record current : records ) {
            keys.add( current.getKey() );
            System.out.printf( "    %s= %s\n", Hex.encode(current.getKey()), Hex.encode( current.getValue() ) );
        }

        System.out.printf( "Found %,d records. \n", records.size() );

        assertEquals( keys, expectedKeys );
        assertEquals(expectedKeys.size(), records.size());

    }
    public void test1() throws Exception {

        // TODO:
        //
        // - test requesting two of the same key and getting back the same key.
        //
        // - test GetBackendRequest keys that don't exists
        //
        // - test with an empty index.
        //
        // - test mixing scan and get requests.
        //
        // - create a bunch of List<BackendRequest> objects and compute the powerset
        //   of all possible combinations.  All different types of scans, etc.  Then
        //   execute them and make sure they are all correct.
        //
        // - Add support for metrics so that we can detect when we're comparing
        //   too many keys.

        config = ConfigParser.parse();

        doSetup( keys );
        doTestGetRequests( keys, max );
        doTestScanRequests();

    }

    public static void main( String[] args ) throws Exception {
        runTests(args);
    }

}
