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
package peregrine.pfs;

import java.net.*;

import peregrine.http.*;

public class TestEmptyWrites extends peregrine.BaseTestWithTwoPartitions {

    public void test1() throws Exception {

        HttpClient client =
            new HttpClient( new URI( "http://localhost:11112/0/shuffle/nr_nodes/from-partition/0/from-chunk/0" ) );
        
        client.close();
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
