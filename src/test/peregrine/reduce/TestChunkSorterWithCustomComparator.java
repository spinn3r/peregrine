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
package peregrine.reduce;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.os.*;
import peregrine.task.*;
import peregrine.util.*;

import peregrine.reduce.sorter.*;

/**
 * Tests running a reduce but also has some code to benchmark them so that we
 * can look at the total performance.
 */
public class TestChunkSorterWithCustomComparator extends peregrine.BaseTest {

    public void test1() throws Exception {

       Config config = ConfigParser.parse();

        // ******** write the chunk file
        
        File file = new File( "/tmp/test.chunk" );

        DefaultChunkWriter writer = new DefaultChunkWriter( config, file );

        int max = 100;
        
        for( int i = 0; i < max; ++i ) {

            StructReader key   = StructReaders.hashcode( i );
            StructReader value = StructReaders.wrap( i );
            
            writer.write( key, value );
            
        }
        
        writer.close();

        // ******** now create a keylookup for that file.

        DefaultChunkReader reader = new DefaultChunkReader( config, file );
        
        CompositeChunkReader composite = new CompositeChunkReader( config, reader );

        KeyLookup lookup = new KeyLookup( composite );

        //now look through it printing out the key and value pairs.

        while( lookup.hasNext() ) {

            lookup.next();

            KeyEntry entry = lookup.get();

            System.out.printf( "%d\n", entry.getValue().readInt() );
            
        }

        reader.close();

        /*
        ChunkSorter sorter = new ChunkSorter( config , partition );
        SequenceReader result = sorter.sort( work, out, jobOutput );
        */
        
    }

    public static void main( String[] args ) throws Exception {
        
        runTests();

    }

}
