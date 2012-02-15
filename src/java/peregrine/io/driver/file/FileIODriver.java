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
package peregrine.io.driver.file;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.*;
import peregrine.io.partition.*;
import peregrine.task.*;

public class FileIODriver  extends BaseIODriver implements IODriver {

	@Override
	public String getScheme() {
		return "file";
	}

	@Override
	public InputReference getInputReference(String uri) {
		return new FileInputReference( uri );
	}

	@Override
	public JobInput getJobInput( Config config, InputReference inputReference, WorkReference work ) throws IOException {
		PartitionWorkReference partitionWork = (PartitionWorkReference)work;
        FileInputReference file = (FileInputReference) inputReference;
        return new LocalPartitionReader( config, partitionWork.getPartition(), file.getPath() );
    }

	@Override
	public OutputReference getOutputReference(String uri) {
		return new FileOutputReference( uri );
	}

	@Override
	public JobOutput getJobOutput( Config config, OutputReference outputReference, WorkReference work ) throws IOException {
		PartitionWorkReference partitionWork = (PartitionWorkReference)work;
        FileOutputReference fileref = (FileOutputReference)outputReference;
        PartitionWriter writer = new DefaultPartitionWriter( config, partitionWork.getPartition(), fileref.getPath(), fileref.getAppend() );
        return new PartitionWriterJobOutput( writer );	
	}

	@Override
	public String toString() {
		return getScheme();
	}

}
