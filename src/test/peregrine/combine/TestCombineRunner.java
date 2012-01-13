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
package peregrine.combine;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.app.pagerank.*;

public class TestCombineRunner extends peregrine.BaseTestWithMultipleConfigs {

    @Override
    public void doTest() throws Exception {

        CombineRunner runner = new CombineRunner();

        Config config = configs.get( 0 );

        File file = new File( "/tmp/test.chunk" );

        //write some values to a chunk writer

        DefaultChunkWriter writer = new DefaultChunkWriter( config, file );

        for( int i = 0; i < 100; ++i ) {

            StructReader key   = StructReaders.hashcode( i );
            StructReader value = StructReaders.wrap( i );
            
            writer.write( key, value );
            writer.write( key, value );
            
        }
        
        writer.close();
        
        //create a chunk reader from it

        ChunkReader reader = new DefaultChunkReader( config, file );

        Partition partition = new Partition( 0 );

        Combiner combiner = new Combiner() {

                public void combine( StructReader key, List<StructReader> values ) {

                    int sum = 0;

                    for( StructReader value : values ) {
                        sum += value.readInt();                        
                    }

                    System.out.printf( "combine result: %s\n", sum );

                    //FIXME this doens't work just yet.
                    //emit( key, StructReaders.wrap( sum ) );
                    
                }

            };

        CombineRunner combineRunner = new CombineRunner();

        combineRunner.combine( config, partition, reader, combiner );
        
    }

    public static void main( String[] args ) throws Exception {
        //System.setProperty( "peregrine.test.config", "04:01:32" ); 
        //System.setProperty( "peregrine.test.config", "01:01:1" ); 
        //System.setProperty( "peregrine.test.config", "8:1:32" );
        //System.setProperty( "peregrine.test.config", "2:1:3" ); 
        //System.setProperty( "peregrine.test.config", "2:1:3" ); 
        System.setProperty( "peregrine.test.config", "1:1:1" ); 
        runTests();
        
    }

}
