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
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.io.util.*;
import peregrine.reduce.*;
import peregrine.sort.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

import com.spinn3r.log5j.*;

public class TestSortViaMapReduce extends peregrine.BaseTestWithMultipleProcesses {

    private static final Logger log = Logger.getLogger();

    public static final int MAX_DELTA_PERC = 5;
    
    private static String MODE = "all";

    /**
     * The values created by each test so that we can test them locally after we
     * emit them.
     */
    private static List<StructReader> values = null;

    @Override
    public void doTest() throws Exception {

        Config config = getConfig();

        int nr_partitions = config.getMembership().size();
        
        int max = ComputePartitionTableJob.MAX_SAMPLE_SIZE * 2 * nr_partitions;

        //doTest( max, 2 );
        //doTest( max, 1 );
        //doTest( max, 10 );
         doTest( max, 100 );
        // doTest( max, 1000 );
        // doTest( max, 10000 );
        // doTest( max, max );
        
    }

    /**
     * Given our real world partition layout, sort them and compute the ideal
     * partitions so that we can see how close our samples come.
     */
    private void computeIdealPartitions() throws Exception {

        Config config = getConfig();

        JobSortComparator comparator = new JobSortComparator();

        // // FIXME: for now sort it twice so we can sample it...
        Collections.sort( values, comparator );

        ComputePartitionTableJob.log( "FIXME: head of actual values: " , values.subList( 0, 200 ) );

        ComputePartitionTableJob.log( "FIXME: summarization of ALL values ", ComputePartitionTableJob.summarize( values, 80 ) );

        List<StructReader> boundaries = ComputePartitionTableJob.computePartitionBoundaries( config, new Partition( 666 ), comparator, values );

        int partition_id = 0;
        for( StructReader sr : boundaries ) {
            System.out.printf( "FIXME: Ideal partition %s (%s) for partition_id=%s\n", sr.toInteger(), sr.toString(), partition_id++ );
        }
        
        System.out.printf( "ideal hits: %s\n" , computePartitionHits( boundaries, values ) );
        
    }

    private void computeActualPartitions() throws Exception {

        // read the actual partition information we wrote on isk.
        File file = new File( "/tmp/peregrine-fs-11112/localhost/11112/0/tmp/partition_table/chunk000000.dat" );

        DefaultChunkReader reader = new DefaultChunkReader( file );

        reader.next();

        List<StructReader> boundaries = StructReaders.unwrap( reader.value() );

        int partition_id = 0;
        for( StructReader sr : boundaries ) {
            System.out.printf( "FIXME: Actual partition %s (%s) for partition_id=%s\n", sr.toInteger(), sr.toString(), partition_id++ );
        }

        System.out.printf( "actual partitioned data: %s\n", computePartitionHits( boundaries, values ) );
        
    }

    private static Map<Partition,Integer> computePartitionHits( List<StructReader> boundaries, List<StructReader> values ) {

        JobSortComparator comparator = new JobSortComparator();

        TreeMap<StructReader,Partition> partitionTable = new TreeMap( comparator );
        Map<Partition,Integer> hits = new HashMap();

        int partition_id = 0;
        for( StructReader sr : boundaries ) {

            Partition part = new Partition( partition_id++ );
            
            partitionTable.put( sr, part );
            hits.put( part, 0 );
        }

        for ( StructReader ptr : values ) {

            Partition target = partitionTable.ceilingEntry( ptr ).getValue();
            hits.put( target, hits.get( target ) + 1 );

        }

        return hits;
        
    }
    
    private void doTest( int max, int range ) throws Exception {

        System.out.printf( "FIXME: max=%s\n", max );
        
        log.info( "Testing with %,d records." , max );

        Config config = getConfig();

        values = new ArrayList();
        
        String path = String.format( "/test/%s/test1.in", getClass().getName() );

        ExtractWriter writer = new ExtractWriter( config, path );

        Random r = new Random();

        JobSortComparator comparator = new JobSortComparator();
        
        for( long i = 0; i < max; ++i ) {

            StructReader key = StructReaders.hashcode( i );
            StructReader value = StructReaders.wrap( (long)r.nextInt( range ) );
            //StructReader value = StructReaders.wrap( 0L );
            
            writer.write( key, value );

            //values.add( comparator.getSortKey( key, value ) );
            
        }

        writer.close();

        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );

        try {

            // technically, we can't just use the input data which is written
            // via the ExtractWriter because the keys won't be written in the
            // same order as a fully random mapper.  We have to map it like it
            // would be in production.

            controller.map( new Job().setDelegate( Mapper.class )
                                     .setInput( path )
                                     .setOutput( "shuffle:default" ) );

            controller.reduce( new Job().setDelegate( Reducer.class )
                                        .setInput( "shuffle:default" )
                                        .setOutput( path ) );
            
            controller.sort( path, output, JobSortComparator.class );
            
        } finally {
            controller.shutdown();
        }
        
        // ********** local work which reads directly from the filesystem to
        // ********** make sure we have correct results

        // now test the distribution of the keys... 

        // map from partition to disk usage in bytes
        Map<Partition,Long> usage = new HashMap();
        
        for ( Config c : configs ) {

            System.out.printf( "host: %s\n", c.getHost() );

            // /tmp/peregrine-fs-11112/localhost/11112/0/

            List<Partition> partitions = c.getMembership().getPartitions( c.getHost() );

            for( Partition part : partitions ) {

                int port = c.getHost().getPort();
                
                String dir = String.format( "/tmp/peregrine-fs-%s/localhost/%s/%s/%s", port, port, part.getId(), output );

                File file = new File( dir );

                if( ! file.exists() )
                    continue;

                long du = Files.usage( file );

                System.out.printf( "%s=%s\n", file.getPath(), du );

                usage.put( part, du );
                
            }

        }

        //computeIdealPartitions();
        computeActualPartitions();
        
        double total = 0;

        for( long val : usage.values() ) {
            total += val;
        }

        Map<Partition,Integer> perc = new HashMap();

        for( Partition part : usage.keySet() ) {

            long du = usage.get( part );

            int p = (int)((du / total) * 100);

            perc.put( part , p );
            
        }

        System.out.printf( "perc: %s\n", perc );

        int last = -1;
        
        for( Partition part : perc.keySet() ) {

            if ( last != -1 ) {

                int delta = perc.get( part ) - last;

                if ( delta > MAX_DELTA_PERC ) {
                    throw new RuntimeException( String.format( "invalid partition layout with max=%s and range=%s : %s", max, range, perc ) );
                }
                
            }

            last = perc.get( part );
            
        }
        
    }

    public static void main( String[] args ) throws Exception {

        //System.setProperty( "peregrine.test.config", "8:1:1" ); // 3sec
        //System.setProperty( "peregrine.test.config", "2:1:4" ); // 3sec
        System.setProperty( "peregrine.test.config", "4:1:1" ); // 3sec
        runTests();

    }

}
