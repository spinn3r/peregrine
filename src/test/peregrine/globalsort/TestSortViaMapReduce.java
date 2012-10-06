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
import peregrine.io.util.*;
import peregrine.reduce.*;
import peregrine.sort.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

import com.spinn3r.log5j.*;

public class TestSortViaMapReduce extends peregrine.BaseTestWithMultipleProcesses {

    private static final Logger log = Logger.getLogger();

    private static String MODE = "all";

    @Override
    public void doTest() throws Exception {

        int max = ComputePartitionTableJob.MAX_SAMPLE_SIZE * 2;
        
        doTest( max, 1 );
        doTest( max, 2 );
        doTest( max, 10 );
        doTest( max, 100 );
        doTest( max, 1000 );
        doTest( max, 10000 );
        doTest( max, max );
        
    }

    private void doTest( int max, int range ) throws Exception {

        log.info( "Testing with %,d records." , max );

        Config config = getConfig();

        String path = String.format( "/test/%s/test1.in", getClass().getName() );

        ExtractWriter writer = new ExtractWriter( config, path );

        Random r = new Random();
        
        for( long i = 0; i < max; ++i ) {

            StructReader key = StructReaders.hashcode( i );
            //StructReader value = StructReaders.wrap( (long)r.nextInt( range ) );
            StructReader value = StructReaders.wrap( 0L );
            
            writer.write( key, value );
            
        }

        writer.close();

        String output = String.format( "/test/%s/test1.out", getClass().getName() );

        Controller controller = new Controller( config );

        try {

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

                if ( delta > 2 ) {
                    throw new RuntimeException( "invalid partition layout: " + perc );
                }
                
            }

            last = perc.get( part );
            
        }
        
    }

    public static void main( String[] args ) throws Exception {

        //System.setProperty( "peregrine.test.config", "8:1:1" ); // 3sec
        //System.setProperty( "peregrine.test.config", "2:1:4" ); // 3sec
        System.setProperty( "peregrine.test.config", "2:1:1" ); // 3sec
        runTests();
        
    }

}
