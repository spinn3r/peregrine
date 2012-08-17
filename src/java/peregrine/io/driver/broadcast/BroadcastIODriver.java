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
package peregrine.io.driver.broadcast;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.*;
import peregrine.task.*;

public class BroadcastIODriver  extends BaseIODriver implements IODriver {

	@Override
	public String getScheme() {
		return "broadcast";
	}

	@Override
	public InputReference getInputReference(String uri) {
		return new BroadcastInputReference( uri );
	}

	@Override
	public JobInput getJobInput( Config config, Job job, InputReference inputReference, WorkReference work  ) throws IOException {		
	    throw new IOException( "not implemented" );
	}

	@Override
	public OutputReference getOutputReference(String uri) {
		return new BroadcastOutputReference( uri );
	}

	@Override
	public JobOutput getJobOutput( Config config, Job job, OutputReference outputReference, WorkReference work  ) throws IOException {
		PartitionWorkReference partitionWork = (PartitionWorkReference)work;
        BroadcastOutputReference bcast = (BroadcastOutputReference) outputReference;
        return new BroadcastJobOutput( config, bcast.getName(), partitionWork.getPartition() );
	}

	@Override
	public String toString() {
		return getScheme();
	}

}





