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
import peregrine.sort.*;

import com.spinn3r.log5j.*;

public class TestSortDescendingViaMapReduce extends peregrine.BaseTestWithMultipleProcesses {

    private static final Logger log = Logger.getLogger();

    private static String MODE = "all";

    @Override
    public void doTest() throws Exception {

        doTest( ComputePartitionTableJob.MAX_SAMPLE_SIZE * 2 );
        
    }

    private void doTest( int max ) throws Exception {

        log.info( "Testing with %,d records." , max );

        Config config = getConfig();

        String path = String.format( "/test/%s/test1.in", getClass().getName() );

        ExtractWriter writer = new ExtractWriter( config, path );

        int range = max;

        Random r = new Random();
        
        for( long i = 0; i < max; ++i ) {

            StructReader key = StructReaders.hashcode( i );
            StructReader value = StructReaders.wrap( (long)r.nextInt( range ) );
            
            writer.write( key, value );
            
        }

        writer.close();

        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );

        try {

            controller.sort( path, output, JobSortDescendingComparator.class );
            
        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {

        System.setProperty( "peregrine.test.config", "1:1:2" ); // 3sec
   
        runTests();
        
    }

}

