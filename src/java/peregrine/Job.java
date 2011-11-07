package peregrine;

import peregrine.io.*;

/**
 * Represents a job which much be run by Peregrine.  All necessary metadata is 
 * included here and specified for an entire job.
 * 
 * @author burton
 *
 */
public class Job {

	private String name;
	private String description;
	private Class delegate;
	private Class combiner;
	private Input input;
	private Output output;
	
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

	
}
