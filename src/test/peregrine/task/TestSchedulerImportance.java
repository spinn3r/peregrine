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
package peregrine.task;

import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.config.*;
import peregrine.config.partitioner.*;

public class TestSchedulerImportance extends BaseTest {

    Host host = new Host( "localhost" );

    Host host0 = host;
    Host host1 = new Host( "localhost1" );
    Host host2 = new Host( "localhost2" );

    Scheduler scheduler = new Scheduler();

	public void test1() throws Exception {

        List<Replica> replicas = new ArrayList();

        // add replicas by priority but out of order.
        replicas.add( new Replica( host, new Partition( 0 ), 1 ) );
        replicas.add( new Replica( host, new Partition( 0 ), 2 ) );
        replicas.add( new Replica( host, new Partition( 0 ), 0 ) );
        
        List<Work> work = getWork( replicas );
        
        work = scheduler.getWorkForExecutionByImportance( work );

        assertEquals( 0, work.get( 0 ).getPriority() );
        assertEquals( 1, work.get( 1 ).getPriority() );
        assertEquals( 2, work.get( 2 ).getPriority() );

	}

	private List<Work> getWork( List<Replica> replicas ) {
		
		List<Work> result = new ArrayList();
		
		for( Replica replica : replicas ) {
			
			result.add( new Work( new ReplicaWorkReference( replica ) , replica.getPriority() ) );
			
		}
		
		return result;
		
	}
	
	public void test2() throws Exception {


        Partition part0 = new Partition( 0 );
        Partition part1 = new Partition( 1 );
        Partition part2 = new Partition( 2 );
        Partition part3 = new Partition( 3 );
        Partition part4 = new Partition( 4 );
        Partition part5 = new Partition( 5 );
        
        List<Replica> replicas = new ArrayList();

        // two primary replicas, two secondary replicas, but one of the
        // secondary replicas is already executing.
        replicas.add( new Replica( host, part0, 0 ) );
        replicas.add( new Replica( host, part1, 0 ) );
        replicas.add( new Replica( host, part2, 1 ) );
        replicas.add( new Replica( host, part3, 1 ) );
        replicas.add( new Replica( host, part4, 2 ) );
        replicas.add( new Replica( host, part5, 2 ) );

        List<Work> work = getWork( replicas );       
        
        scheduler.executing = new MapSet();

        scheduler.executing.put( new Work( new ReplicaWorkReference( replicas.get( 0 ) ) ) , host0 );
        scheduler.executing.put( new Work( new ReplicaWorkReference( replicas.get( 1 ) ) ) , host0 );
        scheduler.executing.put( new Work( new ReplicaWorkReference( replicas.get( 2 ) ) ) , host0 );
        
        work = scheduler.getWorkForExecutionByImportance( work );

        dump( work );
        
        assertEquals( "[replica: partition:00000003, priority=1, host=localhost:11112]", work.get( 0 ).toString() );
        assertEquals( "[replica: partition:00000004, priority=2, host=localhost:11112]", work.get( 1 ).toString() );
        assertEquals( "[replica: partition:00000005, priority=2, host=localhost:11112]", work.get( 2 ).toString() );
        
	}

    public void dump( List<Work> list ) {

        System.out.printf( "work: \n" );
        
        for( Work work : list ) {
            System.out.printf( "  %s\n", work );
        }
        
    }
    
	public static void main(String[] args) throws Exception {

        runTests();
		
	}

}
