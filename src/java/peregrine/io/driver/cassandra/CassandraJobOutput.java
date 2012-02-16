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

import peregrine.*;
import peregrine.io.*;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.db.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

/**
 * 
 */
public class CassandraJobOutput implements JobOutput {

    private RecordWriter writer;

    private TaskAttemptContext context;

    public CassandraJobOutput( CassandraIODriver driver,
                               CassandraInputReference ref,
                               CassandraWorkReference work ) throws IOException {

        Configuration conf = driver.getConfiguration( ref );

        TaskAttemptContext context = new TaskAttemptContext( conf, new TaskAttemptID() );

        ColumnFamilyOutputFormat outputFormat = new ColumnFamilyOutputFormat();

        try {
            writer = outputFormat.getRecordWriter( context );
        } catch ( InterruptedException e ) {
            throw new IOException( e ) ;
        }
        
	}

    @Override
    public void emit( StructReader key , StructReader value ) {
        //noop
    }

    @Override
    public void flush() throws IOException {
        //noop
    }

    @Override
    public void close() throws IOException {

        try { 
            writer.close( context );
        } catch ( InterruptedException e ) {
            throw new IOException( e ) ;
        }

    }
    
}
