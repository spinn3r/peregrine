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
package peregrine.io.driver.shuffle;

import java.io.*;
import java.util.*;
import java.net.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.*;
import peregrine.task.*;

public class ShuffleIODriver extends BaseIODriver implements IODriver {

	@Override
	public String getScheme() {
		return "shuffle";
	}

	@Override
	public InputReference getInputReference( String uri ) {
		return new ShuffleInputReference( getSchemeSpecificPart( uri ) );
	}

	@Override
	public JobInput getJobInput( InputReference inputReference, Config config, Work work ) throws IOException {		
	    throw new IOException( "not implemented" );
	}

	@Override
	public OutputReference getOutputReference(String uri) {
		return new ShuffleOutputReference( getSchemeSpecificPart( uri ) );
	}

    private String getSchemeSpecificPart( String uri ) {
        return uri.replaceAll( getScheme() + ":", "" );
    }
    
	@Override
	public JobOutput getJobOutput( OutputReference outputReference, Config config, Work work  ) throws IOException {
		PartitionWork partitionWork = (PartitionWork)work;
        return new ShuffleJobOutput( config, (ShuffleOutputReference)outputReference, partitionWork.getPartition() );
	}

	@Override
	public String toString() {
		return getScheme();
	}

}
