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
        
        replicas = scheduler.getReplicasForExecutionByImportance( replicas );

        assertEquals( 0, replicas.get( 0 ).getPriority() );
        assertEquals( 1, replicas.get( 1 ).getPriority() );
        assertEquals( 2, replicas.get( 2 ).getPriority() );

	}

	public void test2() throws Exception {

        List<Replica> replicas = new ArrayList();

        Partition part0 = new Partition( 0 );
        Partition part1 = new Partition( 1 );
        Partition part2 = new Partition( 2 );
        Partition part3 = new Partition( 3 );
        Partition part4 = new Partition( 4 );
        Partition part5 = new Partition( 5 );
        
        // two primary replicas, two secondary replicas, but one of the
        // secondary replicas is already executing.
        replicas.add( new Replica( host, part0, 0 ) );
        replicas.add( new Replica( host, part1, 0 ) );
        replicas.add( new Replica( host, part2, 1 ) );
        replicas.add( new Replica( host, part3, 1 ) );
        replicas.add( new Replica( host, part4, 2 ) );
        replicas.add( new Replica( host, part5, 2 ) );

        scheduler.executing = new MapSet();

        scheduler.executing.put( part0, host0 );
        scheduler.executing.put( part1, host0 );
        scheduler.executing.put( part2, host0 );
        
        replicas = scheduler.getReplicasForExecutionByImportance( replicas );

        dump( replicas );
        
        assertEquals( part3, replicas.get( 0 ).getPartition() );
        assertEquals( part4, replicas.get( 1 ).getPartition() );
        assertEquals( part5, replicas.get( 2 ).getPartition() );
        
	}

    public void dump( List<Replica> replicas ) {

        System.out.printf( "Replicas: \n" );
        
        for( Replica replica : replicas ) {
            System.out.printf( "  %s\n", replica );
        }
        
    }
    
	public static void main(String[] args) throws Exception {

        runTests();
		
	}

}
