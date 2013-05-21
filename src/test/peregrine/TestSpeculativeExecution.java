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
import java.util.*;
import java.util.concurrent.atomic.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;

import peregrine.util.*;
import com.spinn3r.log5j.*;

public class TestSpeculativeExecution extends peregrine.BaseTestWithMultipleConfigs {

    private static final Logger log = Logger.getLogger();

    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
                         StructReader value ) {

            Partition part = getPartition();
            Config config = getConfig();
            Host host = config.getHost();
            Host firstHost = config.getMembership().getHosts( part ).get( 0 );

            //on the FIRST host that we are schedule on... run SLOW... this way
            //the second host take our work and finishes before us at which
            //point we will be killed.
            if ( getPartition().getId() == 0 &&
                 getConfig().getReplicas() >= 2 &&
                 host.equals( firstHost ) ) {

                try {
                    Thread.sleep( 5000L );
                } catch ( InterruptedException e ) {
                    throw new RuntimeException( e ) ;
                }

            }
            
            emit( key, value );
            
        }

    }

    @Override
    public void doTest() throws Exception {

        doTest( 2500 * getFactor() );
        
    }
    
    private void doTest( int max ) throws Exception {

        log.info( "Testing with %,d records." , max );

        String path = String.format( "/test/%s/test1.in", getClass().getName() );
        
        ExtractWriter writer = new ExtractWriter( config, path );

        for( int i = 0; i < max; ++i ) {

            StructReader key   = StructReaders.hashcode( i );
            StructReader value = StructReaders.wrap( i );
            
            writer.write( key, value );
            
        }

        writer.close();

        Controller controller = new Controller( config );

        try {

            Batch batch = new Batch( getClass() );

            batch.map( Map.class,
                       new Input( path ),
                       new Output( "shuffle:default" ) );

            controller.exec( batch );

        } finally {
            controller.shutdown();
        }
        
    }

    public static void main( String[] args ) throws Exception {

        //System.setProperty( "peregrine.test.config", "1:1:1" ); // 3sec

        System.setProperty( "peregrine.test.factor", "10" ); // 1m
        System.setProperty( "peregrine.test.config", "01:02:1" ); // takes 3 seconds

        // 256 partitions... 
        //System.setProperty( "peregrine.test.config", "08:01:32" );  // 1m

        runTests();

    }

}
