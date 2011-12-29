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

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.config.Partition;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.util.*;

/**
 * 
 */
public class TestFullOuterJoin extends peregrine.BaseTestWithTwoPartitions {

    public void test1() throws Exception {

        //write keys to two files but where there isn't a 100%
        //intersection... then try to join against these files. 

        //now test writing two regions to a file and see if both sides of the
        //join are applied correctly

        Partition part = new Partition( 0 );
        
        PartitionWriter writer;

        writer = new DefaultPartitionWriter( config, part, "/tmp/left" );

        write( writer, 1 );
        write( writer, 2 );
        write( writer, 3 );
        write( writer, 4 );
        write( writer, 5 );

        writer.close();

        writer = new DefaultPartitionWriter( config, part, "/tmp/right" );

        write( writer, 4 );
        write( writer, 5 );
        write( writer, 6 );
        write( writer, 7 );
        write( writer, 8 );

        writer.close();

        List<ChunkReader> readers = new ArrayList();
        
        readers.add( new LocalPartitionReader( config, part, "/tmp/left" ) );
        readers.add( new LocalPartitionReader( config, part, "/tmp/right" ) );
        
        LocalMerger merger = new LocalMerger( readers );

        //FIXME: make sure the results come back ordered correctly... 
        
        while( true ) {

            JoinedTuple joined = merger.next();

            if ( joined == null )
                break;

            System.out.printf( "joined: %s, left=%s, right=%s\n",
                               Hex.encode( joined.key ), Hex.encode( joined.values.get(0) ), Hex.encode( joined.values.get(1) ) );
            
        }

    }

    public static void write( PartitionWriter writer,
                              int v ) throws IOException {

    	StructReader key = StructReaders.wrap(v);
    	StructReader value = key;
        
        writer.write( key, value );
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}

