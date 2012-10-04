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

import peregrine.*;
import peregrine.app.flow.*;
import peregrine.app.pagerank.extract.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.util.*;
import peregrine.worker.*;

/**
 * Command line interface for submitting pagerank jobs to the controller.
 */
public class Main {

    public static void main( String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );
        new Initializer( config ).basic( Main.class );

        Getopt getopt = new Getopt( args );

        String graph               = getopt.getString( "graph", "/pr/graph" );
        String nodes_by_hashcode   = getopt.getString( "nodes_by_hashcode", "/pr/nodes_by_hashcode" );
        String corpus              = getopt.getString( "corpus" );
        boolean sortedGraph        = getopt.getBoolean( "sortedGraph" );

        if ( "random".equals( corpus ) ) {

            //build a grandom graph
            
            int nr_nodes = getopt.getInt( "nr_nodes", 500 );
            int max_edges_per_node = getopt.getInt( "max_edges_per_node", 500 );
            
            System.out.printf( "Running with nr_nodes: %,d , max_edges_per_node: %,d\n", nr_nodes, max_edges_per_node );
            
            GraphBuilder builder = new GraphBuilder( config, graph, nodes_by_hashcode );
            
            builder.buildRandomGraph( nr_nodes , max_edges_per_node );
        
            builder.close();

        } else {
            System.out.printf( "Using existing graph.\n" );
        }

        Batch batch = new Batch( Main.class );

        // TODO: see if we first need to run extract ... 
        
        if ( getopt.getBoolean( "extract" ) ) {

        }
                                                       
        // see if we first need to run flow to prune disconnected graphs.

        if ( getopt.containsKey( "flow" ) ) {
           
            getopt.require( "flow", "sources" );

            String sources           = getopt.getString( "sources" );
            int iterations           = getopt.getInt( "iterations", 5 );
            boolean caseInsensitive  = getopt.getBoolean( "caseInsensitive" );
            String output            = getopt.getString( "output", "/pr/graph.flowed" );
            
            Flow flow = new Flow( graph, output, sources, iterations, caseInsensitive );
            batch.add( flow );

            // we need to use the flowed graph as the input for pagerank now
            graph = output;
            
        }
        
        batch.add( new Pagerank( config, graph, nodes_by_hashcode, sortedGraph ) );

        batch.init( args );
        
        ControllerClient.submit( config, batch );
        
    }

}
