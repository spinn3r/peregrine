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

public class TestClientURLHandling extends peregrine.BaseTest {

    private Config config = null;
    
    private void doTest( List<StructReader> keys , boolean hashcode ) throws Exception {

        GetRequest request = new GetRequest();
        request.setKeys( keys );
        request.setSource( "/tmp/foo.test" );
        request.setHashcode( hashcode );

        Connection conn = new Connection( "http://localhost:11111" );
        Get client = new Get( config, conn );
        
        String url = GetRequestURLParser.toURL( client, request );
        
        request = GetRequestURLParser.toRequest( url );

        if( hashcode ) {
            assertEquals( GetRequestURLParser.hashcodes( keys ), request.getKeys() );
        } else { 
            assertEquals( keys, request.getKeys() );
        }

    }
    
    public void test1() throws Exception {

        config = ConfigParser.parse();

        doTest( range( 0 , 10 ), false );
        doTest( range( 0 , 10 ), true );

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
