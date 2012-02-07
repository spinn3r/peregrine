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
package peregrine;

import java.net.*;

import peregrine.http.*;
import peregrine.worker.*;

public class TestPutRequests extends peregrine.BaseTestWithTwoDaemons {

	@Override
	public void doTest() throws Exception {
		_test1();
		_test1WithHexPipeline();
		_test2();
	}
	
    public void doTest( int max ) throws Exception {

        for( int i = 0; i < max; ++i ) {

            System.out.printf( "Writing %,d out of %,d\n", i , max );
            
            HttpClient client = new HttpClient( new URI( String.format( "http://localhost:11112/foo/%s", i ) ) );

            client.write( "xxxxxxxxxxxxxxxxxx".getBytes() );

            client.close();
            
        }

    }
    
    public void _test1() throws Exception {

        doTest( 1000 );
    }

    public void _test1WithHexPipeline() throws Exception {

        HexPipelineEncoder.ENABLED = true;
        
        doTest( 1000 );
    }

    public void _test2() throws Exception {

        int max = 10;
        
        for( int i = 0; i < max; ++i ) {

            System.out.printf( "Writing %,d out of %,d\n", i , max );
            
            HttpClient client = new HttpClient( new URI( String.format( "http://localhost:11112/foo/%s", i ) ) );

            client.write( "xxxxxxxxxxxxxxxxxx".getBytes() );

            Thread.sleep( 1000L );
            
            client.write( "xxxxxxxxxxxxxxxxxx".getBytes() );

            client.close();
            
        }
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
