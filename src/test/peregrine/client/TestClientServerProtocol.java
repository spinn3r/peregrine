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
package peregrine.client;

import java.io.IOException;
import java.util.*;

import peregrine.*;
import peregrine.http.HttpClient;
import peregrine.io.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.util.*;

public class TestClientServerProtocol extends peregrine.BaseTestWithMultipleProcesses {

    // FIXME: a scan request needs to first route to the right region server and
    // then if that request isn't complete (because it hit the end of the region)
    // then we need to execute it over the next region.

    int max = 50;
    List<StructReader> keys = StructReaders.range( 1, max );
    Partition partition = new Partition(0);
    Connection conn = new Connection( "http://localhost:11112" );
    String path = String.format( "/test/%s/test1.sstable", getClass().getName() );

    private void doSetup(List<StructReader> keys) throws Exception {

        ExtractWriter writer = new ExtractWriter( config, path );

        for( StructReader key : keys ) {
            writer.write( key, StructReaders.wrap( "xxxx" ) );
        }

        writer.close();

        System.out.printf( "Wrote %,d keys to %s", keys.size(), path );

    }

    public void doTest() throws Exception {

        doSetup( StructReaders.range( 1, max ) );

        //doTestGetRequests( range( 1, max ), max );
        //doTestGetRequests( range( 100, 150 ), 0 );

        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();
        doTestScanRequests();

        doTestGetRequests( StructReaders.range( 1, max ), max );
        doTestGetRequests( StructReaders.range( 100, 150 ), 0 );

        // FIXME: this is going to need to be fixed.  We shouldn't have this as
        // a global static member.  It needs to be passed in and then shutdown
        // externally.
        HttpClient.socketChannelFactory.releaseExternalResources();

    }

    private void doTestScanRequests() throws IOException {
        //now try with a scan.  This should be fun!

        ScanRequest scanRequest = new ScanRequest();
        scanRequest.setSource( path );
        scanRequest.setPartition(partition);

        String url = new ScanRequestURLEncoder().encode(conn, scanRequest);

        System.out.printf( "Scan URL: %s" , url );

        ScanClient scanClient = new ScanClient( config, conn );

        scanClient.exec( scanRequest );
        scanClient.waitFor();

        assertResults( scanClient.getRecords(), scanRequest.getLimit() );

    }

    private void doTestGetRequests( List<StructReader> keys, int nrExpectedRecords ) throws IOException {

        GetRequest request = new GetRequest();
        request.setKeys(keys);
        request.setSource( path );
        request.setPartition( partition );

        GetClient client = new GetClient( config, conn );

        client.exec( request );
        client.waitFor();

        assertResults( client.getRecords(), nrExpectedRecords );

    }

    private void assertResults( List<Record> records, int nrExpectedRecords ) {

        for( Record current : records ) {
            System.out.printf( "    %s= %s\n", Hex.encode(current.getKey()), Hex.encode( current.getValue() ) );
        }

        System.out.printf( "Found %,d records. ", records.size() );

        assertEquals( nrExpectedRecords, records.size() );

    }

    public static void main( String[] args ) throws Exception {
        runTests(args);
    }

}
