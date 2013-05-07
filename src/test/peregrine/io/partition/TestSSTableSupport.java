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
package peregrine.io.partition;

import java.io.*;
import java.util.*;
import peregrine.*;
import peregrine.config.*;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.util.*;

public class TestSSTableSupport extends peregrine.BaseTestWithMultipleProcesses {

    public void doTest() throws Exception {

        long max = 100;

        String path = "/tmp/test";

        // STEP 1... make a new file and write lots of chunks to it.
        
        config.setChunkSize( 1000 );

        Partition part = new Partition( 0 );
        
        PartitionWriter writer = new DefaultPartitionWriter( config, part, path );
        
        for ( long i = 0; i < max; ++i ) {

        	StructReader key = StructReaders.wrap( i );
                
        	StructReader value = key;

            writer.write( key, value );
            
        }

        writer.close();

        LocalPartitionReader reader = new LocalPartitionReader( configs.get( 0 ), part, path );

        int found = 0;
        while( reader.hasNext() ) {

            reader.next();
            ++found;
            
            System.out.printf( "   %s\n", Hex.encode( reader.key() ) );
            
        }
        
        reader.close();

        assertEquals( max, found );
        
        // // STEP 2: make sure we have LOTS of chunks on disk.
        
        // List<DefaultChunkReader> readers = LocalPartition.getChunkReaders( configs.get( 0 ), part, path );

        // System.out.printf( "We have %,d readers\n", readers.size() );
        
        // assertTrue( readers.size() >= 1 ) ;

        // // now create another PartitionWriter this time try to overwrite the
        // // existing file and all chunks should be removed.
        
        // writer = new DefaultPartitionWriter( config, part, path );
        // writer.close();

        // readers = LocalPartition.getChunkReaders( config, part, path );

        // System.out.printf( "We have %,d readers\n", readers.size() );
        
        // assertEquals( 0, readers.size() ) ;
        
    }

    public static void main( String[] args ) throws Exception {
        System.setProperty( "peregrine.test.factor", "1" ); // 1m
        System.setProperty( "peregrine.test.config", "1:1:1" ); // takes 3 seconds
        runTests();
    }

}
