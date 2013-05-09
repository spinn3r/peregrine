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

        long max = 1000;

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

        // reopen it to test again.
        reader = new LocalPartitionReader( configs.get( 0 ), part, path );

        for( long i = 0; i < max; ++i ) {

            Record record = reader.seekTo( StructReaders.wrap( i ) );

            assertNotNull( record );

        }

        assertNull( reader.findDataBlockReference( StructReaders.wrap( Long.MAX_VALUE ) ) );
        assertNull( reader.findDataBlockReference( StructReaders.wrap( (long)max * 2 ) ) );
        assertNull( reader.findDataBlockReference( StructReaders.wrap( (long)max ) ) );

        if ( max > 0 ) {
            assertNotNull( reader.findDataBlockReference( StructReaders.wrap( (long)max-1 ) ) );
        }

        reader.close();

    }

    public static void main( String[] args ) throws Exception {
        System.setProperty( "peregrine.test.factor", "1" ); // 1m
        System.setProperty( "peregrine.test.config", "1:1:1" ); // takes 3 seconds
        runTests();
    }

}
