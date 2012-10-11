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
package peregrine.http;

import java.io.*;
import java.net.*;

import peregrine.http.*;
import peregrine.config.*;

public class TestFailedRequests extends peregrine.BaseTest {

    public void test1() throws Exception {

        String link = "http://localhost:8000/foo/bar";

        for ( int i = 0; i < 500; ++i ) {

            try {
                HttpClient client = new HttpClient( new Config(), link );
                client.write( "hello world".getBytes() );
                client.close();
            } catch ( IOException e ) {
                System.out.printf( "x" );
            }

        }

        System.out.printf( "done\n" );
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
