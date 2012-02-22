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

/**
 * IO driver for working with Cassandra.  Supports both input and output for map
 * jobs.
 *
 * <p>
 * All jobs are specified with a URL scheme:
 * 
 * <p>
 * URI scheme is:
 * 
 * <p>
 * cassandra://host:port/keyspace/columnfamily
 *
 * <p>
 * Where:
 *
 * <p>
 *  - host:port is a seed host to connect to.
 *
 * <p>
 *  - keyspace is obviously the keyspace to use 
 * 
 * <p>
 *  - columnfamily is obviously the keyspace to use
 * 
 * <p>
 * For example:
 *
 * <p>
 * cassandra://localhost:9160/rank/graph
 *
 * <p>
 * The map key is the Cassandra row key.  The value is a {@link SequenceReader},
 * implemented by {@link StructSequenceReader}, which has the key/value pairs
 * for the row.
 * 
 */
public class CassandraIODriver extends BaseIODriver implements IODriver {

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
	public JobInput getJobInput( Config config,
                                 InputReference inputReference,
                                 WorkReference workReference ) throws IOException {

        return new CassandraJobInput( this,
                                      (CassandraInputReference)inputReference,
                                      (CassandraWorkReference)workReference );

    }

	@Override
	public OutputReference getOutputReference(String uri) {
        return new CassandraOutputReference( uri );
	}

	@Override
	public JobOutput getJobOutput( Config config,
                                   OutputReference outputReference,
                                   WorkReference work ) throws IOException {

        return new CassandraJobOutput( this,
                                       (CassandraOutputReference)outputReference );

	}

    /**
     * Get a unit of work (input split, partition, etc) from the given string specification.
     */
	@Override
    public Map<Host,List<Work>> getWork( Config config, InputReference inputReference )
        throws IOException {

        CassandraInputReference ref = (CassandraInputReference)inputReference;

        ColumnFamilyInputFormat inputFormat = new ColumnFamilyInputFormat();

        Configuration conf = getInputConfiguration( ref );

        JobContext jobContext = new JobContext( conf, new JobID() );
        
        List<InputSplit> splits = inputFormat.getSplits( jobContext );

        TaskAttemptContext context = new TaskAttemptContext( conf, new TaskAttemptID() );

        Map<Host,List<Work>> result = new HashMap();

        for ( Host host : config.getHosts() ) {
            result.put( host, new ArrayList() );            
        }

        // all available work ...
        List<WorkReference> work = new ArrayList();
        
        for( InputSplit split : splits ) {

            ColumnFamilySplit columnFamilySplit = (ColumnFamilySplit)split;

            work.add( new CassandraWorkReference( columnFamilySplit.getStartToken(),
                                                  columnFamilySplit.getEndToken(),
                                                  columnFamilySplit.getLocations() ) );
            
        }

        // now randomly distribute this work to all hosts...

        Iterator<Host> it = config.getHosts().iterator();

        for( WorkReference w : work ) {

            if ( it.hasNext() == false ) {

                it = config.getHosts().iterator();

                if ( it.hasNext() == false ) {
                    throw new RuntimeException( "no hosts" );
                }
                
            }

            Host host = it.next();

            result.get( host ).add( new Work( host, w ) );
            
        }
        
        return result;

    }

    private Configuration getConfiguration( CassandraBaseReference ref ) {

        Configuration conf = new Configuration();

        ConfigHelper.setInitialAddress( conf, ref.getHost() );
        ConfigHelper.setRpcPort( conf, ref.getPort() );

        ConfigHelper.setPartitioner(conf, "org.apache.cassandra.dht.RandomPartitioner" );

        SlicePredicate sp = new SlicePredicate();

        SliceRange sr = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 100 );
        sp.setSlice_range(sr);

        ConfigHelper.setInputSlicePredicate(conf, sp);

        return conf;
        
    }

    protected Configuration getInputConfiguration( CassandraBaseReference ref ) {
        Configuration conf = getConfiguration( ref );
        ConfigHelper.setInputColumnFamily( conf, ref.getKeyspace(), ref.getColumnFamily() );
        return conf;
    }

    protected Configuration getOutputConfiguration( CassandraBaseReference ref ) {
        Configuration conf = getConfiguration( ref );
        ConfigHelper.setOutputColumnFamily( conf, ref.getKeyspace(), ref.getColumnFamily() );
        return conf;
    }
    
	@Override
	public WorkReference getWorkReference( String uri ) {
        return new CassandraWorkReference( uri );
    }

	@Override
	public String toString() {
		return getScheme();
	}
    
}

