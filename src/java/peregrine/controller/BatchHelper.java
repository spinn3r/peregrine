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
package peregrine.controller;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.sort.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * 
 * BatchHelper accepts a batch and allows high level manipulation of adding new
 * jobs to the Batch (sort, touch, etc).  These high level jobs are just
 * abstractions of lower level basic operations such as merge, map, etc.
 */
public class BatchHelper {

    private Batch batch;

    public BatchHelper() {
        this( new Batch() );
    }

    public BatchHelper( Batch batch ) {
        this.batch = batch;
    }

    public Batch getBatch() {
        return batch;
    }
    
    public Batch map( Class mapper,
                      String... paths ) throws Exception {
        map( mapper, new Input( paths ) );
        return batch;
    }

    public Batch map( final Class mapper,
                      final Input input ) throws Exception {
        map( mapper, input, null );
        return batch;
    }
    
    public Batch map( final Class delegate,
                      final Input input,
                      final Output output ) throws Exception {

    	batch.add(  new Job().setDelegate( delegate ) 
                    .setInput( input )
                    .setOutput( output ) );
        
        return batch;
    		
    }

    public Batch map( final Job job ) throws Exception {
        job.setOperation( JobOperation.MAP );
        batch.add( job );
        return batch;
    }

    public Batch merge( Class mapper,
                       String... paths ) throws Exception {

        merge( mapper, new Input( paths ) );
        return batch;
        
    }

    public Batch merge( final Class mapper,
                        final Input input ) throws Exception {

        merge( mapper, input, null );
        return batch;

    }

    public Batch merge( final Class delegate,
                       final Input input,
                       final Output output ) throws Exception {
    	
    	batch.add( new Job().setDelegate( delegate )
                   .setInput( input )
                   .setOutput( output ) );
        return batch;
        
    }

    public Batch merge( final Job job ) throws Exception {
        job.setOperation( JobOperation.MERGE );
        batch.add( job );
        return batch;
    }

    public Batch reduce( final Class delegate,
                         final Input input,
                         final Output output )  throws Exception {

        Job job = new Job();

        job.setDelegate( delegate )
           .setInput( input )
           .setOutput( output )
           ;
        
    	batch.add( job );

        return batch;
        
    }

    public Batch reduce( final Job job ) throws Exception {
        job.setOperation( JobOperation.REDUCE );
        batch.add( job );
        return batch;
    }

    /**
     * Touch / init a file so that it is empty and ready to be merged against.
     */
    public Batch touch( String output ) throws Exception {

        // map-only job that reads from an empty blackhole: stream and writes
        // nothing to the output file. 
        
        map( Mapper.class,
             new Input( "blackhole:" ),
             new Output( output ) );

        return batch;
        
    }

    public Batch sort( String input, String output, Class comparator ) throws Exception {

        Job job = new Job();
        job.setDelegate( ComputePartitionTableJob.Map.class );
        job.setInput( new Input( input ) );
        job.setOutput( new Output( "shuffle:default", "broadcast:partition_table" ) );
        // the comparator is needed so that we can setup the partition table.
        job.setComparator( comparator );
        // this is a sample job so we don't need to read ALL the data.
        job.setMaxChunks( 1 ); 

        map( job );

        // Step 2.  Reduce that partition table and broadcast it so everyone
        // has the same values.
        reduce( ComputePartitionTableJob.Reduce.class,
                new Input( "shuffle:partition_table" ),
                new Output( "/tmp/partition_table" ) );

        // Step 3.  Map across all the data in the input file and send it to
        // right partition which would need to hold this value.
        
        job = new Job();

        job.setDelegate( GlobalSortJob.Map.class );
        job.setInput( new Input( input, "broadcast:/tmp/partition_table" ) );
        job.setOutput( new Output( "shuffle:default" ) );
        job.setPartitioner( GlobalSortPartitioner.class );
        job.setComparator( comparator );
        
        map( job );

        job = new Job();
        
        job.setDelegate( GlobalSortJob.Reduce.class );
        job.setInput( new Input( "shuffle:default" ) );
        job.setOutput( new Output( output ) );
        job.setComparator( comparator );
        
        reduce( job );

        return batch;

    }

}
