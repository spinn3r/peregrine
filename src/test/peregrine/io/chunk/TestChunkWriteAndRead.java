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
package peregrine.io.chunk;

import java.io.*;
import java.util.*;
import peregrine.*;
import peregrine.util.*;
import peregrine.config.Partition;
import peregrine.io.partition.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;

public class TestChunkWriteAndRead extends BaseTest {

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        System.out.printf( "Writing new chunk data.\n" );

        File file = new File( "/tmp/test.chunk" );
        
        DefaultChunkWriter writer = new DefaultChunkWriter( null, file );

        int max = 100;
        
        for( long i = 0; i < max; ++i ) {
            writer.write( StructReaders.wrap( i ), StructReaders.wrap( i ) );
        }
        
        writer.close();

        DefaultChunkReader reader = new DefaultChunkReader( null, file );

        int count = 0;
        
        while( reader.hasNext() ) {

            reader.next();
            reader.key();
            reader.value();
            ++count;

        }

        assertEquals( count, max );
        
        reader.close();
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
