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

    public void test1() {

        Config config = ConfigParser.parse();
        
        List<StructReader> keys = range( 0 , 100 );

        GetRequest request = new GetRequest();
        request.setKeys( keys );

        Connection conn = new Connection( "http://localhost:11111" );
        Get client = new Get( config, conn );
        
        String url = GetRequestURLParser.toURL( client, request );

        request = GetRequestURLParser.toRequest( url );

        assertEquals( keys, request.getKeys() );
        
    }
    
}
