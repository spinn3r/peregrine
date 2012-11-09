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

import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.partitioner.*;

public class TestRangeRouter extends BaseTest {

	public void test1() {

        //TODO: this needs to be refactored to use BaseTestWithMultipleProcesses
        
        // int nr_partitions = 10;
        
		// //Partitioner partitioner = new HashPartitioner();
        // Partitioner partitioner = new RangePartitioner();
		// partitioner.init(nr_partitions);

        // // keep track of our results.
        
        // Map<Partition,AtomicInteger> map = new HashMap();

        // for( int i = 0; i < nr_partitions; ++i ) {
        //     map.put( new Partition( i ), new AtomicInteger() );
        // }

        // // the max value to distribute... 
        // int max = 500;

        // long gap = (long)((Math.pow( 2, 63 ) / 500));

        // long key = 0;
        
	    // for( long i = 0; i < 5000; ++i ) {

		// 	Partition result = partitioner.partition( StructReaders.hashcode( i ) );

        //     map.get( result ).getAndIncrement();

        //     key += gap;

		// 	//System.out.printf( "result: %s\n", result );
		// 	//assertEquals( i , result.getId() );
			
	    // }

        // System.out.printf( "map: %s\n", map );

	}
	
	public static void main(String[] args) throws Exception {

        runTests();
		
	}

}
