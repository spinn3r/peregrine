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

        String path = "/pr/test.graph";

        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder builder = new GraphBuilder( writer );
        
        builder.buildRandomGraph( nr_nodes , max_edges_per_node );

        writer.close();
        
        new Pagerank( config, path ).exec();

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
