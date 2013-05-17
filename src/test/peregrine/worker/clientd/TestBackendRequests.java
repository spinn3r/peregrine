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

    int max = 50;
    List<StructReader> keys = range( 1, max );
    Partition partition = new Partition(0);
    Connection conn = new Connection( "http://localhost:11112" );
    String path = String.format( "/test/%s/test1.sstable", getClass().getName() );

    private Config config;

    //FIXME: ok... testing the client server protocol is one thing but starting
    //up a daemon and connecting through a port and debugging the client protocol
    //and the correctness of our algorithm is going to be a huge waste of time.
    //instead just write data to disk and take the executor and work with the
    //file directly. This is the next big thing we have to do.  This code isn't
    //testable this way and it's just going to be impossible to work with.

    private void doSetup(List<StructReader> keys) throws Exception {

        ExtractWriter writer = new ExtractWriter( config, path );

        for( StructReader key : keys ) {
            writer.write( key, StructReaders.wrap("xxxx") );
        }

        writer.close();

        System.out.printf( "Wrote %,d keys to %s", keys.size(), path );

    }
    private void doTestGetRequests( List<StructReader> keys, int nrExpectedRecords ) throws IOException {

        List<BackendRequest> requests = new ArrayList<BackendRequest>();

        ClientBackendRequest clientBackendRequest = new ClientBackendRequest(partition,path);

        System.out.printf( "Creating records: \n");

        for( StructReader key : keys ) {
            System.out.printf( "    %s\n", Hex.encode(key) );

            GetBackendRequest getBackendRequest = new GetBackendRequest( clientBackendRequest, key );
            requests.add( getBackendRequest );
        }

        doExec( requests, range( 1, 50 ) );

    }

    private void doTestScanRequests() throws IOException {
        //now try with a scan.  This should be fun!

        ScanRequest scanRequest = new ScanRequest();
        scanRequest.getClientRequestMeta().setSource( path );
        scanRequest.getClientRequestMeta().setPartition(partition);

        ClientBackendRequest clientBackendRequest = new ClientBackendRequest(partition,path);

        ScanBackendRequest scanBackendRequest = new ScanBackendRequest( clientBackendRequest, scanRequest );

        doExec( scanBackendRequest, range( 1, 10 ) );

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
        // - test keys that don't exists
        //
        // - test with an empty index.
        //
        // - test mixing scan and get requests.
        //
        // - test the powerset of all scan operations.

        config = ConfigParser.parse();

        doSetup( range( 1, max ) );
        doTestGetRequests( keys, max );
        doTestScanRequests();

    }

    public static void main( String[] args ) throws Exception {
        runTests(args);
    }

}
