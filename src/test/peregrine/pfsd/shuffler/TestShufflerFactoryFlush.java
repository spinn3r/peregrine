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
package peregrine.pfsd.shuffler;

import java.net.*;

import peregrine.http.*;

import org.jboss.netty.handler.codec.http.*;

public class TestShufflerFactoryFlush extends peregrine.BaseTestWithTwoDaemons {

    public void doTest() throws Exception {

        // now measure the flush time...

        QueryStringEncoder encoder = new QueryStringEncoder( "" );
        encoder.addParam( "action", "flush" );
        String query = encoder.toString();

        HttpClient client = new HttpClient( new URI( "http://localhost:11112/shuffler/RPC" ) );

        client.setMethod( HttpMethod.POST );
        client.write( query.getBytes() );
        client.close();
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
