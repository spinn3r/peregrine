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
package peregrine;

import java.util.*;

import peregrine.config.partitioner.*;
import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.util.*;
import peregrine.controller.*;

import com.spinn3r.log5j.Logger;

/**
 * A 'batch' of jobs sent to the controller at once.
 *
 */
public class Batch implements MessageSerializable {

    private static final Logger log = Logger.getLogger();

    protected String description = "";
    
    protected String name = "";

    protected List<Job> jobs = new ArrayList();

    protected long identifier = -1;

    protected int start = 0;

    protected int end = Integer.MAX_VALUE;
    
    public Batch() {}

    public Batch( Class clazz ) {
        this( clazz.getName() );
    }
    
    public Batch( String name ) {
        this.name = name;
    }
    
    public Batch map( Class mapper,
                      String... paths ) {
        return map( mapper, new Input( paths ) );
    }

    public Batch map( Class mapper,
                      Input input ) {
        return map( mapper, input, null );
    }
    
    public Batch map( Class delegate,
                      Input input,
                      Output output ) {

    	return map( new Job().setDelegate( delegate ) 
                    .setInput( input )
                    .setOutput( output ) );
    		
    }

    public Batch map( Job job ) {
        add( job.setOperation( JobOperation.MAP ) );
        return this;
    }

    public Batch merge( Class mapper,
                       String... paths ) {

        return merge( mapper, new Input( paths ) );
        
    }

    public Batch merge( Class mapper,
                        Input input ) {

        return merge( mapper, input, null );

    }

    public Batch merge( Class delegate,
                        Input input,
                        Output output ) {
    	
    	merge( new Job().setDelegate( delegate )
               .setInput( input )
               .setOutput( output ) );
        return this;
        
    }

    public Batch merge( Job job ) {
        add( job.setOperation( JobOperation.MERGE ) );
        return this;
    }

    public Batch reduce( final Class delegate,
                         final Input input,
                         final Output output )  {

        return reduce( new Job().setDelegate( delegate )
                                .setInput( input )
                                .setOutput( output ) );

    }

    public Batch reduce( Job job ) {
        add( job.setOperation( JobOperation.REDUCE ) );
        return this;
    }

    /**
     * Touch / init a file so that it is empty and ready to be merged against.
     */
    public Batch touch( String output ) {

        // map-only job that reads from an empty blackhole: stream and writes
        // nothing to the output file. 
        
        return map( Mapper.class,
                    new Input( "blackhole:" ),
                    new Output( output ) );
        
    }

    public Batch sort( String input, String output, Class comparator ) {

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

        return this;

    }

    // **** basic / primitive operations
    
    public void add( Job job ) {
        jobs.add( job );
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public String getName() { 
        return this.name;
    }

    public Batch setName( String name ) { 
        this.name = name;
        return this;
    }

    public String getDescription() { 
        return this.description;
    }

    public Batch setDescription( String description ) { 
        this.description = description;
        return this;
    }

    public long getIdentifier() {
        return this.identifier;
    }

    public Batch setIdentifier( long identifier ) {
        this.identifier = identifier;
        return this;
    }

    public int getStart() {
        return this.start;
    }

    public Batch setStart( int start ) {
        this.start = start;
        return this;
    }

    public int getEnd() {
        return this.end;
    }

    public Batch setEnd( int end ) {
        this.end = end;
        return this;
    }

    public void assertExecutionViability() {

        if ( getJobs().size() == 0 ) {
            throw new RuntimeException( "Batch has no jobs" );
        }

        if ( Strings.empty( name ) ) {
            throw new RuntimeException( "Batch has no name" );
        }

    }

    /**
     * Command line apps should parse args from the command line.
     */
    public void init( String[] args ) {
        Getopt getopt = new Getopt( args );
        this.start = getopt.getInt( "batch.start" );
        this.end   = getopt.getInt( "batch.end" );
    }
    
    /**
     * Convert this to an RPC message.
     */
    @Override
    public Message toMessage() {

        Message message = new Message();

        message.put( "name",          name );
        message.put( "description",   description );
        message.put( "identifier",    identifier );
        message.put( "start",         start );
        message.put( "end",           end );
        message.put( "jobs",          jobs );

        return message;
        
    }

    @Override
    public void fromMessage( Message message ) {

        name          = message.getString( "name" );
        description   = message.getString( "description" );
        identifier    = message.getLong( "identifier" );
        start         = message.getInt( "start" );
        end           = message.getInt( "end" );
        jobs          = message.getList( "jobs", Job.class );

    }

    @Override
    public String toString() {
        return String.format( "%s jobs=%s", getName(), jobs.toString() );
    }
    
}
