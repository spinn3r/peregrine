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

import peregrine.io.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.app.pagerank.*;

public class TestPagerank extends peregrine.BaseTestWithMultipleProcesses {

    @Override
    public void doTest() throws Exception {

        doTest( 5000 * getFactor() , 100 ); 

    }

    private void doTest( int nr_nodes,
                         int max_edges_per_node ) throws Exception {

        Config config = getConfig();

        // only 0 and 1 should be dangling.

        String graph = "/pr/graph";
        String nodes_by_hashcode = "/pr/nodes_by_hashcode";

        GraphBuilder builder = new GraphBuilder( config, graph, nodes_by_hashcode );
        
        builder.buildRandomGraph( nr_nodes , max_edges_per_node );

        builder.close();

        Controller controller = null;
        
        try {

            controller = new Controller( config );
            
            Pagerank pr = new Pagerank( config, graph, nodes_by_hashcode );

            controller.exec( pr );

        } finally {
            if ( controller != null ) 
                controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {

        /*
        BaseTestWithMultipleProcesses.BASEDIR_MAP.put( 11112 , "/d0" );
        BaseTestWithMultipleProcesses.BASEDIR_MAP.put( 11113 , "/d1" );
        BaseTestWithMultipleProcesses.BASEDIR_MAP.put( 11114 , "/d2" );
        BaseTestWithMultipleProcesses.BASEDIR_MAP.put( 11115 , "/d3" );
        */
        
        //setPropertyDefault( "peregrine.test.config", "2:1:4" ); 

        /*
        setPropertyDefault( "peregrine.test.factor", "10" ); 
        setPropertyDefault( "peregrine.test.config", "4:1:1" ); 
        */

        /*
        System.setProperty( "peregrine.test.factor", "10" ); 
        System.setProperty( "peregrine.test.config", "1:1:4" ); 
        */
        
        System.setProperty( "peregrine.test.factor", "10" ); 
        System.setProperty( "peregrine.test.config", "2:1:8" ); 
        runTests();

    }

}
