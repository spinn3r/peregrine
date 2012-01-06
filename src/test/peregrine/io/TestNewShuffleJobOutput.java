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
package peregrine.io;

import peregrine.*;
import peregrine.config.Partition;
import peregrine.controller.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.shuffle.*;
import peregrine.shuffle.sender.*;

public class TestNewShuffleJobOutput extends peregrine.BaseTestWithTwoDaemons {

    private void doTestIter( int max_emits ) throws Exception {

        Partition part = new Partition( 0 );
        
        ShuffleJobOutput output = new ShuffleJobOutput( config, part );

        ChunkReference chunkRef = new ChunkReference( part );
        chunkRef.local = 0;

        output.onChunk( chunkRef );

        for ( int i = 0; i < max_emits; ++i ) {

            StructReader key = StructReaders.hashcode( i );

            StructReader value = key;

            output.emit( key, value );
            
        }

        output.onChunkEnd( chunkRef );

        output.close();

        Controller controller = new Controller( config );

        try {
            controller.flushAllShufflers();
        } finally {
            controller.shutdown();
        }
        
    }

    public void doTest( int iterations, int max_emits ) throws Exception {

        assertEquals( config.getHosts().size(), 2 );

        System.out.printf( "Running with %,d hosts.\n", config.getHosts().size() );
        
        for( int i = 0; i < iterations; ++i ) {
            doTestIter( max_emits );
        }

    }
    
    public void test1() throws Exception {
        doTest( 20, 1 );
        doTest( 100, 3 );
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
