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

import java.util.concurrent.atomic.*;

import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.reduce.*;
import peregrine.config.partitioner.*;

import com.spinn3r.log5j.*;

/**
 * Represents a job (map, merge, or, reduce) which much be run by Peregrine.
 * All necessary metadata is included here and specified for an entire job.
 *
 */
public class ReduceJob extends Job implements MessageSerializable {

    private static final Logger log = Logger.getLogger();

	protected Class comparator = DefaultReduceComparator.class; 

    protected Class partitioner = RangePartitioner.class;

	public Class getComparator() {
		return comparator;
	}

	public ReduceJob setComparator(Class comparator) {
		this.comparator = comparator;
		return this;
	}

    public ReduceComparator getComparatorInstance() {

        try {
            return (ReduceComparator)getComparator().newInstance();
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
        
    }

    public Class getPartitioner() { 
        return this.partitioner;
    }

    public void setPartitioner( Class partitioner ) { 
        this.partitioner = partitioner;
    }

    public Partitioner getPartionerInstance() {

        try {
            return (Partitioner)getPartitioner().newInstance();
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
        
    }

    @Override
    public String toString() {

        return String.format( "%s (%s) for input=%s , output=%s , comparator=%s",
                              getDelegate().getName(),
                              getName(),
                              getInput(),
                              getOutput(),
                              comparator );

    }

    /**
     * Convert this to an RPC message.
     */
    @Override
    public Message toMessage() {

        Message message = super.toMessage();

        message.put( "class",       getClass().getName() );
        message.put( "comparator",  comparator );
        message.put( "partitioner", partitioner );

        return message;
        
    }

    @Override
    public void fromMessage( Message message ) {

        super.fromMessage( message );
        comparator = message.getClass( "comparator" );
        partitioner = message.getClass( "partitioner" );
        
    }
    
}
