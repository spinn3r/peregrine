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

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.db.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

public class CassandraJobInput extends BaseJobInput implements JobInput {

    private boolean fired = false;
    
    private ChunkReference chunkRef;
    
    private StructReader key = null;
    
    private StructReader value = null;

    private ColumnFamilyRecordReader reader;
    
	public CassandraJobInput( CassandraIODriver driver,
                              CassandraInputReference ref,
                              CassandraWorkReference work ) throws IOException {

        ColumnFamilySplit columnFamilySplit = new ColumnFamilySplit( work.startToken,
                                                                     work.endToken,
                                                                     work.dataNodes );

        Configuration conf = driver.getConfiguration( ref );

        TaskAttemptContext context = new TaskAttemptContext( conf, new TaskAttemptID() );

        reader = new ColumnFamilyRecordReader();
        reader.initialize( columnFamilySplit, context );

        chunkRef = new ChunkReference( new Partition( -1 ) );

        next();
        
	}
	
	@Override
	public boolean hasNext() throws IOException {
        return key != null;
	}

	@Override
	public void next() throws IOException {

        if ( fired == false ) {
            chunkRef.incr();
            fireOnChunk( chunkRef );
            fired = true;
        }

		//current = iterator.next();
		
	}

	@Override
	public StructReader key() throws IOException {
		return key;		
	}

	@Override
	public StructReader value() throws IOException {
		return value;
	}

	@Override
	public void close() throws IOException {
        fireOnChunkEnd( chunkRef );
	}
	
}
