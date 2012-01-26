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
package peregrine.io.driver;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.*;
import peregrine.task.*;

/**
 * Represents a way to add new input drivers to peregrine.
 */
public abstract class BaseIODriver implements IODriver {
	
    /**
     * Get a unit of work (input split, partition, etc) from the given string specification.
     */
	@Override
    public Map<Host,List<Work>> getWork( Config config, InputReference inputReference ) {

		Map<Host,List<Work>> result = new ConcurrentHashMap();
		
        for( Host host : config.getHosts() ) {
        
        	List<Work> entry = new ArrayList();
        	
        	for( Replica replica : config.getMembership().getReplicas( host ) ) {

            	Work work = new Work( new PartitionWorkReference( replica.getPartition() ) );
            	work.setHost( host );            	
            	work.setPriority( replica.getPriority() );
            	entry.add( work );	

        	}
        	
        	result.put( host, entry );
        	
        }

        return result;
        
    }
    
	@Override
	public WorkReference getWorkReference( String uri ) {
	    return new PartitionWorkReference( uri );
	}
	
}
