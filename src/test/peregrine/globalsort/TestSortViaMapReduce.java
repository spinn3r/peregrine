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
package peregrine.globalsort;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.primitive.*;
import peregrine.util.*;
import peregrine.reduce.*;

import com.spinn3r.log5j.*;

public class TestSortViaMapReduce extends peregrine.BaseTestWithMultipleProcesses {

    private static final Logger log = Logger.getLogger();

    private static String MODE = "all";

    public static class Reduce extends Reducer {

        AtomicInteger count = new AtomicInteger();
        
        @Override
        public void reduce( StructReader key, List<StructReader> values ) {
            emit( key, values.get( 0 ) );
        }

        @Override
        public void close() throws IOException {
            
        }

    }

    @Override
    public void doTest() throws Exception {

        doTest( ComputePartitionTableJob.MAX_SAMPLE_SIZE * 30 );
        
    }

    private void doTest( int max ) throws Exception {

        log.info( "Testing with %,d records." , max );

        Config config = getConfig();

        String path = String.format( "/test/%s/test1.in", getClass().getName() );

        ExtractWriter writer = new ExtractWriter( config, path );

        int range = 1000;

        Random r = new Random();
        
        for( long i = 0; i < max; ++i ) {

            StructReader key = StructReaders.hashcode( i );
            StructReader value = StructReaders.wrap( (long)r.nextInt( range ) );
            
            writer.write( key, value );
            
        }

        writer.close();
            
        // the writes worked correctly.
        
        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );

        try {

            /*
            ReduceJob job = new ReduceJob();
            
            job.setDelegate( Reduce.class );
            job.setInput( new Input( path ) );
            job.setOutput( new Output( output ) );
            job.setComparator( SortByValueReduceComparator.class );
            
            controller.reduce( job );
            */

            Job job = new Job();
            job.setDelegate( ComputePartitionTableJob.Map.class );
            job.setInput( new Input( path ) );
            job.setOutput( new Output( "shuffle:default",
                                       "broadcast:partition_table" ) );
            // this is a sample job so we don't need to read ALL the data.
            job.setMaxChunks( 1 ); 

            controller.map( job );

            controller.reduce( ComputePartitionTableJob.Reduce.class,
                               new Input( "shuffle:partition_table" ),
                               new Output( "/test/globalsort/partition_table" ) );

            LocalPartitionReader reader = new LocalPartitionReader( configs.get( 0 ),
                                                                    new Partition( 0 ),
                                                                    "/test/globalsort/partition_table" );
            
            // use a TreeSet and higher() to find the boundary for a given
            // value.
            
            TreeMap<StructReader,Long> partitionTable = new TreeMap( new StrictStructReaderComparator() );

            long partition_id = 0;
            
            while( reader.hasNext() ) {

                reader.next();

                partitionTable.put( reader.value(), partition_id );
                
                System.out.printf( "entry: %s\n", Hex.encode( reader.value() ) );

                ++partition_id;
                
            }

            System.out.printf( "%s\n", partitionTable );
            
            reader = new LocalPartitionReader( configs.get( 0 ), new Partition( 0 ), path );

            System.out.printf( "Going to read key/value pairs.\n" );

            HashMap<Long,Integer> histograph = new HashMap();

            for( long i = 0; i < partitionTable.size(); ++i ) {
                histograph.put( i, 0 );
            }
            
            int count = 0;

            while( reader.hasNext() ) {

                reader.next();

                StructReader ptr = StructReaders.join( reader.value(), reader.key() );

                StructReader key = partitionTable.higherKey( ptr );

                long partition = partitionTable.get( key );
                histograph.put( partition , histograph.get( partition ) + 1 );
                
                ++count;
                
            }

            System.out.printf( "histograph: %s\n", histograph );
            
            System.out.printf( "read %,d entries.\n", count );

            //now we should in theory be able to read all the values out of the
            //input and route them with the partition table and make sure we end
            //up with somewhat even buckets.
            
            //
            //nr_nodes = getBroadcastInput()
            //               .get( 0 )
            //               .getValue()
            //               .readInt();

            job = new Job();

            job.setDelegate( Mapper.class );
            job.setInput( new Input( path, "broadcast:/test/globalsort/partition_table" ) );
            job.setOutput( new Output( "shuffle:default" ) );
            job.setPartitioner( GlobalSortPartitioner.class );
            
            controller.map( job );

            // TODO: in production we can sample, then map right over ALL the
            // values, send them to the target partitions, then reduce them
            // there.
            
            /*

            ReduceJob job = new ReduceJob();
            
            job.setDelegate( Reduce.class );
            
            job.setInput( new Input( "shuffle:default",
                                     "broadcast:/test/globalsort/partition_table" ) );
                                     
            job.setOutput( new Output( "shuffle:intermediate" ) );
            job.setComparator( SortByValueReduceComparator.class );
            job.setPartitioner( GlobalSortPartitioner.class );
            
            controller.reduce( job );

            job = new ReduceJob();
            
            job.setDelegate( Reduce.class );
            job.setInput( new Input( "shuffle:intermediate" ) );
            job.setOutput( new Output( output ) );
            job.setComparator( SortByValueReduceComparator.class );
            
            controller.reduce( job );
            
            */
            
            // TODO: the output here needs to be a shuffle with a new
            // partitioner which knows how many items are in the result

            // then 
            
        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {

        System.setProperty( "peregrine.test.config", "1:1:2" ); // 3sec

        //setPropertyDefault( "peregrine.test.factor", "1" ); // 
        //setPropertyDefault( "peregrine.test.config", "01:01:01" ); // takes 3 seconds

        // 256 partitions... 
        //System.setProperty( "peregrine.test.config", "08:01:32" );  // 1m

        if ( args.length == 1 ) {
            MODE=args[0].split( "=" )[1];
        }

        System.out.printf( "MODE: %s\n" , MODE );
        
        runTests();
        
    }

}

class DefaultKeyValuePair implements KeyValuePair {

    StructReader key = null;
    StructReader value = null;

    public DefaultKeyValuePair( StructReader key, StructReader value ) {
        this.key = key;
        this.value = value;
    }

    public StructReader getKey() {
        return key;
    }

    public StructReader getValue() {
        return value;
    }

}
