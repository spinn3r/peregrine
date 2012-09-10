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
package peregrine.app.pagerank;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.worker.*;

public class Main {

    public static void main( String[] args ) throws Exception {

        Getopt getopt = new Getopt( args );
        
        int nr_nodes = getopt.getInt( "nr_nodes", 500 );
        int max_edges_per_node = getopt.getInt( "max_edges_per_node", 500 );
        
        System.out.printf( "Running with nr_nodes: %,d , max_edges_per_node: %,d\n", nr_nodes, max_edges_per_node );
        
        Config config = ConfigParser.parse( args );
        new Initializer( config ).controller();

        String graph = "/pr/graph";
        String nodes_by_hashcode = "/pr/nodes_by_hashcode";

        GraphBuilder builder = new GraphBuilder( config, graph, nodes_by_hashcode );
        
        builder.buildRandomGraph( nr_nodes , max_edges_per_node );
        
        builder.close();

        new Pagerank( config, graph, nodes_by_hashcode ).exec();
        
    }

}
