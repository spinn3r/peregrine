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
package peregrine.io.driver.cassandra;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.*;
import peregrine.io.partition.*;
import peregrine.task.*;

/**
 * IO driver for working with Cassandra.
 * 
 * URI scheme is:
 * 
 * cassandra://host:port/keyspace/columnfamily
 * 
 */
public class CassandraIODriver  extends BaseIODriver implements IODriver {

    protected static Pattern PATTERN
        = Pattern.compile( "cassandra://([^:/]+)(:([0-9]+))?/([^/]+)/([^/]+)" );

	@Override
	public String getScheme() {
		return "cassandra";
	}

	@Override
	public InputReference getInputReference(String uri) {
        return new CassandraInputReference( uri );
	}

	@Override
	public JobInput getJobInput( Config config, InputReference inputReference, WorkReference work ) throws IOException {
        throw new IOException( "not implemented" );
    }

	@Override
	public OutputReference getOutputReference(String uri) {
        throw new RuntimeException( "not implemented" );
	}

	@Override
	public JobOutput getJobOutput( Config config, OutputReference outputReference, WorkReference work ) throws IOException {
        throw new IOException( "not implemented" );
	}

	@Override
	public String toString() {
		return getScheme();
	}

}

