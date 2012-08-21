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

import com.spinn3r.log5j.*;

public class TestSortViaMapReduce extends peregrine.BaseTestWithMultipleProcesses {

    private static final Logger log = Logger.getLogger();

    private static String MODE = "all";
    
    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
                         StructReader value ) {

            emit( key, value );
            
        }

    }

    public static class Reduce extends Reducer {

        AtomicInteger count = new AtomicInteger();
        
        @Override
        public void reduce( StructReader key, List<StructReader> values ) {
            emit( key, values.get( 0 ) );
        }

        @Override
        public void cleanup() {
            
        }

    }

    @Override
    public void doTest() throws Exception {

        doTest( 2500 * getFactor() );
        
    }

    private void doTest( int max ) throws Exception {

        log.info( "Testing with %,d records." , max );

        Config config = getConfig();

        String path = String.format( "/test/%s/test1.in", getClass().getName() );

        if ( MODE.equals( "extract" ) || MODE.equals( "all" ) ) {
        
            ExtractWriter writer = new ExtractWriter( config, path );

            for( int i = 0; i < max; ++i ) {

                StructReader key = StructReaders.hashcode( i );
                StructReader value = StructReaders.wrap( i );
                
                writer.write( key, value );
                
            }

            writer.close();

        }
            
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

            if ( MODE.equals( "map" ) || MODE.equals( "all" ) ) {

                controller.map( Map.class,
                                new Input( path ),
                                new Output( "shuffle:default" ) );

            }
                
            // make sure the shuffle output worked

            if ( MODE.equals( "reduce" ) || MODE.equals( "all" ) ) {

                ReduceJob job = new ReduceJob();
                
                job.setDelegate( Reduce.class );
                job.setInput( new Input( "shuffle:default" ) );
                job.setOutput( new Output( "shuffle:intermediate" ) );
                job.setComparator( SortByValueReduceComparator.class );
                job.setPartitioner( GlobalSortPartitioner.class );
                
                controller.reduce( job );

            }
                
            // TODO: the output here needs to be a shuffle with a new
            // partitioner which knows how many items are in the result

            // then 
            
        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {

        //System.setProperty( "peregrine.test.config", "1:1:2" ); // 3sec

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
