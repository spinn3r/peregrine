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
package peregrine.io.driver.cassandra;

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

import com.spinn3r.log5j.*;

public class TestCassandraJobs extends peregrine.BaseTestWithMultipleConfigs {

    private static final Logger log = Logger.getLogger();

    public void doTest() throws Exception {
        
        String output = String.format( "/test/%s/test.out", getClass().getName() );

        Controller controller = new Controller( config );

        try {

            controller.map( Mapper.class,
                            new Input( "cassandra://localhost:9160/mykeyspace/graph" ),
                            new Output( "shuffle:default" ) );

            controller.reduce( Reducer.class,
                               new Input( "shuffle:default" ),
                               new Output( output ) );

        } finally {
            controller.shutdown();
        }
        
    }

    public static void main( String[] args ) throws Exception {

        //System.setProperty( "peregrine.test.config", "1:1:1" ); // 3sec

        System.setProperty( "peregrine.test.factor", "10" ); // 1m
        System.setProperty( "peregrine.test.config", "01:01:02" ); // takes 3 seconds

        // 256 partitions... 
        //System.setProperty( "peregrine.test.config", "08:01:32" );  // 1m

        runTests();

    }

}
