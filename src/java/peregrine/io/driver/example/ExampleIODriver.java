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
package peregrine.io.driver.example;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.*;

/**
 * Example of an IO driver... start here when writing a new driver.
 */
public class ExampleIODriver implements IODriver {

	@Override
	public String getScheme() {
		return "example";
	}

	@Override
	public InputReference getInputReference(String uri) {
		return new ExampleInputReference();
	}

	@Override
	public JobInput getJobInput( InputReference inputReference, Config config, Partition partition ) throws IOException {		
	    return new ExampleJobInput( partition );
	}

	@Override
	public OutputReference getOutputReference(String uri) {
		return new ExampleOutputReference();
	}

	@Override
	public JobOutput getJobOutput( OutputReference outputReference, Config config, Partition partition ) throws IOException {
		return new ExampleJobOutput();
	}

	@Override
	public String toString() {
		return getScheme();
	}

}

class ExampleJobInput extends BaseJobInput implements JobInput {

	private List<StructReader> list = new ArrayList();
	
	private Iterator<StructReader> iterator = null;
	
	private StructReader current = null;

    private boolean fired = false;
    
    private ChunkReference chunkRef;
    
    private StructReader key = null;
    
    private StructReader value = null;
    
	public ExampleJobInput( Partition partition ) {

        // stick in example data.
		for( long i = 0; i < 100; ++i ) {
			list.add( StructReaders.hashcode( i ) );
		}

	    iterator = list.iterator();

        chunkRef = new ChunkReference( partition );

	}
	
	@Override
	public boolean hasNext() throws IOException {
        return iterator.hasNext();
	}

	@Override
	public void next() throws IOException {

        if ( fired == false ) {
            chunkRef.incr();
            fireOnChunk( chunkRef );
            fired = true;
        }

		current = iterator.next();
		
	}
	
	
	@Override
	public StructReader key() throws IOException {
		return current;		
	}

	@Override
	public StructReader value() throws IOException {
		return current;
	}

	@Override
	public void close() throws IOException {
        fireOnChunkEnd( chunkRef );
	}
	
}

class ExampleJobOutput implements JobOutput {

	@Override
	public void flush() throws IOException {
        
	}

	@Override
	public void emit(StructReader key, StructReader value) {

		// noop since this is an example but in production you should write
		// to your database / datastore.
		
	}

	@Override
	public void close() throws IOException {
		// noop but you should free up any used resources.
	}
	
}

class ExampleInputReference implements InputReference {
	
	@Override
	public String getScheme() {
		return "example";
	}

    @Override
    public String toString() {
        return getScheme() + ":";
    }

}

class ExampleOutputReference implements OutputReference {
	
	@Override
	public String getScheme() {
		return "example";
	}

    @Override
    public String toString() {
        return getScheme() + ":";
    }
    
}

