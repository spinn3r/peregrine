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
package peregrine.config;

import peregrine.*;
import peregrine.config.partitioner.*;

public class TestRangeRouter extends BaseTestWithMultipleDaemons {

	public TestRangeRouter() {
		super(5, 2, 51);
	}

	public void test1() {

		RangePartitioner router = new RangePartitioner();
		router.init(config);
		
	    for( int i = 0; i < 255; ++i ) {
	    	byte[] key = new byte[8];
	    	key[0] = (byte)i;
	    	
			Partition result = router.partition( StructReaders.wrap( key ) );
			
			System.out.printf( "result: %s\n", result );
			assertEquals( i , result.getId() );
			
	    }
		

	}
	
	public static void main(String[] args) throws Exception {

        runTests();
		
	}

}
