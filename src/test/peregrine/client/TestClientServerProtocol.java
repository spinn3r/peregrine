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
import peregrine.io.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.util.*;

public class TestClientServerProtocol extends peregrine.BaseTestWithMultipleProcesses {

    // FIXME: update this code to write like 5-6 chunks (say 500MB) and then fetch all the
    // keys including invalid keys and this way we test to make sure we can span
    // chunks and other idiosyncratic issues.
    //
    // do the same thing with SCAN too and have a scan that reads the entire
    // table.  Also do this with invalid keys for the start and end keys to find
    // more bugs

    int max = 50;
    List<StructReader> keys = range( 1, max );
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

        doSetup( range( 1, max ) );

        doTestGetRequests( range( 1, max ), max );
        doTestGetRequests( range( 100, 150 ), 0 );
        doTestScanRequests();

    }

    private void doTestScanRequests() throws IOException {
        //now try with a scan.  This should be fun!

        ScanRequest scanRequest = new ScanRequest();
        scanRequest.getClientRequestMeta().setSource( path );
        scanRequest.getClientRequestMeta().setPartition(partition);

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
        request.getClientRequestMeta().setSource( path );
        request.getClientRequestMeta().setPartition( partition );

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
