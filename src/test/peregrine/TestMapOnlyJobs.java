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
package peregrine;

import java.io.*;
import peregrine.config.Partition;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;

public class TestMapOnlyJobs extends peregrine.BaseTestWithTwoDaemons {

    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
        		         StructReader value ) {

            emit( key, value );
            
        }

    }

    public void doTest() throws Exception {

        String path = "/test/map.only/test1";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        for( int i = 0; i < 100; ++i ) {

        	StructReader key = StructReaders.hashcode((long)i);
        	StructReader value = key;
            writer.write( key, value );
            
        }
        
        writer.close();

        String output = "/test/map.only/test1.out";

        Controller controller = new Controller( config );

        try {
            
            controller.map( Map.class, new Input( path ), new Output( output ) );

            Partition part = new Partition( 1 );
            
            LocalPartitionReader reader = new LocalPartitionReader( configs.get( 1 ), part, output );

            if ( reader.hasNext() == false )
                throw new IOException( "nothing written" );

        } finally {
            controller.shutdown();
        }
            
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}
