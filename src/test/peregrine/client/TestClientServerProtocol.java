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

import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.util.*;

public class TestClientServerProtocol extends peregrine.BaseTestWithMultipleProcesses {

    public void doTest() throws Exception {

        String path = String.format( "/test/%s/test1.sstable", getClass().getName() );

        ExtractWriter writer = new ExtractWriter( config, path );

        int max = 50;

        List<StructReader> keys = range( 1, max );
        
        for( StructReader key : keys ) {
            writer.write( key, StructReaders.wrap( "xxxx" ) );
        }

        writer.close();

        GetRequest request = new GetRequest();
        request.setKeys( keys );
        request.getClientRequestMeta().setSource( path );

        Connection conn = new Connection( "http://localhost:11112/0" );
        GetClient client = new GetClient( config, conn );

        client.exec( request );
        client.waitFor();

        for( Record current : client.getRecords() ) {
            System.out.printf( "    %s= %s\n", Hex.encode( current.getKey() ), Hex.encode( current.getValue() ) );
        }

        System.out.printf( "Found %,d records. ", client.getRecords().size() );

        assertEquals( keys.size(), client.getRecords().size() );
        
    }

    public static void main( String[] args ) throws Exception {
        runTests(args);
    }

}
