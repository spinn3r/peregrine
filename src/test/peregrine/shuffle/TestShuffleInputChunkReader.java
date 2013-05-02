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
package peregrine.shuffle;

import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.LongBytes;
import peregrine.config.Partition;
import peregrine.controller.*;
import peregrine.io.*;

/**
 * Test the FULL shuffle path, not just pats of it...including running with two
 * daemons, writing a LOT of data, and then reading it back in correctly as if
 * we were a reducer.
 */
public class TestShuffleInputChunkReader extends peregrine.BaseTestWithMultipleDaemons {

    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
                         StructReader value ) {

            emit( key, value );
            
        }

    }

    public TestShuffleInputChunkReader() {

        concurrency = 1;
        replicas    = 1;
        nr_daemons  = 1;

    }
    
    public void test1() throws Exception {

        String path = "/tmp/test.in";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        // FIXME: the partition count seems to be off by 4x... W T F.. 
        int max = 10000;
        
        for( long i = 0; i < max; ++i ) {

        	StructReader key = StructReaders.wrap( i );
        	StructReader value = key;
            writer.write( key, value );
            
        }

        writer.close();
        
        Controller controller = new Controller( config );

        try {

            Batch batch = new Batch( getClass() );

            batch.map( Map.class,
                       new Input( path ),
                       new Output( "shuffle:default" ) );

            controller.exec( batch );
            
        } finally {
            controller.shutdown();
        }

        //now create a ParallelShuffleInputChunkReader for one of the
        //partitions so that we can see if it actually works

        path = "/tmp/peregrine/fs/localhost/11112/tmp/shuffle/default/0000000000.tmp";

        Partition partition = new Partition( 0 );

        List<Partition> partitions = new ArrayList();
        partitions.add( partition );
        
        ShuffleInputReader shuffleInputReader = new ShuffleInputReader( config, path, partitions );

        System.out.printf( "Shuffle header: %s\n", shuffleInputReader.getHeader( partition ) );
        
        ShuffleInputChunkReader reader = new ShuffleInputChunkReader( configs.get(0), partition, path );

        assertTrue( reader.count() > 0 );

        while( reader.hasNext() ) {

            reader.next();
            
            System.out.printf( "%s = %s\n", Hex.encode( reader.key() ), Hex.encode( reader.value() ) );
            
        }
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
