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

import peregrine.io.*;
import java.util.concurrent.atomic.*;

/**
 * Represents a job (map, merge, or, reduce) which much be run by Peregrine.
 * All necessary metadata is included here and specified for an entire job.
 *
 */
public class Job {

    private static AtomicInteger nonce = new AtomicInteger();

    protected long timestamp = System.currentTimeMillis();
    protected int identifier = nonce.getAndIncrement();
	protected String handle = String.format( "%010d.%010d", timestamp, identifier );
	protected String name = handle;
	protected String description = null;
	protected Class delegate = null; 
	protected Class combiner = null;
	protected Input input = null;
	protected Output output = null;

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

    @Override
    public String toString() {

        return String.format( "%s (%s) for input %s and output %s ",
                              getDelegate().getName(),
                              getName(),
                              getInput(),
                              getOutput() );

    }
    
}
