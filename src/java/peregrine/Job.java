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

import peregrine.config.partitioner.*;
import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.util.*;
import peregrine.controller.*;

import com.spinn3r.log5j.Logger;

/**
 * Represents a job (map, merge, or, reduce) which much be run by Peregrine.
 * All necessary metadata is included here and specified for an entire job.
 *
 */
public class Job implements MessageSerializable {

    private static final Logger log = Logger.getLogger();

    private static AtomicInteger nonce = new AtomicInteger();

    protected long timestamp = System.currentTimeMillis();
    protected int identifier = nonce.getAndIncrement();
	protected String handle = String.format( "%010d.%010d", timestamp, identifier );
	protected String name = handle;
	protected String description = "";
	protected Class delegate = Mapper.class; 

    //TODO: a combiner does NOT make sense for a reduce job so move this to a
    //MapJob class (which we use with map and merge).
	protected Class combiner = null;
	protected Input input = new Input();
	protected Output output = new Output();
    protected Class partitioner = RangePartitioner.class;

    protected Partitioner partitionerInstance = null;

    protected int maxChunks = Integer.MAX_VALUE;

    protected Class comparator = DefaultSortComparator.class; 

    protected Message parameters = new Message();

    protected String state = JobState.SUBMITTED;

    protected String operation = JobOperation.MAP;

    /**
     * If this job failed this is the cause.
     */
    protected String cause = null;

    /**
     * Get a unique identifier for this job.  Every job stats at 0 (zero) and is
     * every new job identifier is incremented by 1 (one).
     */
    public int getIdentifier() {
        return identifier;
    }
    
    /**
     * Get the unique job ID (nonce).  This is an opaque string used to identify
     * the job.
     */
    public String getHandle() {
        return handle;        
    }

    /**
     * Get an optionally human readable name for this job.  Should be short and
     * only one line of text.
     */
	public String getName() {
		return name;
	}
    
	public Job setName(String name) {
		this.name = name;
		return this;
	}
	public String getDescription() {
		return description;
	}
	public Job setDescription(String description) {
		this.description = description;
		return this;
	}
	public Class getDelegate() {
		return delegate;
	}
	public Job setDelegate(Class delegate) {
		this.delegate = delegate;
		return this;
	}
	public Class getCombiner() {
		return combiner;
	}
	public Job setCombiner(Class combiner) {
		this.combiner = combiner;
		return this;
	}
	public Input getInput() {
		return input;
	}
	public Job setInput(Input input) {
		this.input = input;
		return this;
	}
	public Output getOutput() {
		return output;
	}
	public Job setOutput(Output output) {
		this.output = output;
		return this;
	}

    /**
     * The time the job was started on the controller.
     */
    public long getTimestamp() {
        return timestamp;
    }

    public Class getPartitioner() { 
        return this.partitioner;
    }

    public void setPartitioner( Class partitioner ) { 
        this.partitioner = partitioner;
    }

    public Partitioner getPartitionerInstance() {

        // we do not need the double check idiom here because this isn't
        // multithreaded code.
        
        if ( partitionerInstance == null ) {
        
            try {
                partitionerInstance = (Partitioner)getPartitioner().newInstance();
            } catch ( Throwable t ) {
                throw new RuntimeException( t );
            }

        }

        return partitionerInstance;
        
    }

	public Class getComparator() {
		return comparator;
	}

	public Job setComparator(Class comparator) {
		this.comparator = comparator;
		return this;
	}

    public SortComparator getComparatorInstance() {

        try {
            return (SortComparator)getComparator().newInstance();
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
        
    }

    public int getMaxChunks() { 
        return this.maxChunks;
    }

    public void setMaxChunks( int maxChunks ) { 
        this.maxChunks = maxChunks;
    }

    public Message getParameters() {
        return parameters;
    }

    public Job setParameters( Message parameters ) {
        this.parameters = parameters;
        return this;
    }

    public String getState() {
        return state;
    }

    public Job setState( String state ) {
        this.state = state;
        return this;
    }

    public String getOperation() {
        return operation;
    }

    public Job setOperation( String operation ) {
        this.operation = operation;
        return this;
    }

    public String getCause() {
        return cause;
    }

    public Job setCause( String cause ) {
        this.cause = cause;
        return this;
    }

    public Job setCause( Throwable t ) {
        return setCause( Strings.format( t ) );
    }

    @Override
    public String toString() {

        //TODO: include ALL fields.
        return String.format( "%s (%s) for input %s and output %s ",
                              getDelegate().getName(),
                              getName(),
                              getInput(),
                              getOutput() );

    }

    /**
     * Convert this to an RPC message.
     */
    @Override
    public Message toMessage() {

        Message message = new Message();

        message.put( "class",         getClass().getName() );
        message.put( "timestamp",     timestamp );
        message.put( "identifier",    identifier );
        message.put( "handle",        handle );
        message.put( "name",          name );
        message.put( "description",   description );
        message.put( "delegate",      delegate );
        message.put( "combiner",      combiner );
        message.put( "input",         input.getReferences() );
        message.put( "output",        output.getReferences() );
        message.put( "partitioner",   partitioner );
        message.put( "maxChunks",     maxChunks );
        message.put( "comparator",    comparator );
        message.put( "parameters",    parameters );
        message.put( "state",         state );
        message.put( "operation",     operation );
        message.put( "cause",         cause );

        return message;
        
    }

    @Override
    public void fromMessage( Message message ) {

        timestamp     = message.getLong( "timestamp" );
        identifier    = message.getInt( "identifier" );
        handle        = message.getString( "handle" );
        name          = message.getString( "name" );
        description   = message.getString( "description" );
        delegate      = message.getClass( "delegate" );
        combiner      = message.getClass( "combiner" );
        input         = new Input( message.getList( "input" ) );
        output        = new Output( message.getList( "output" ) );
        partitioner   = message.getClass( "partitioner" );
        maxChunks     = message.getInt( "maxChunks" );
        comparator    = message.getClass( "comparator" );
        parameters    = new Message( message.getString( "parameters" ) );
        state         = message.getString( "state" );
        operation     = message.getString( "operation" );
        cause         = message.getString( "cause" );
        
    }
    
}
