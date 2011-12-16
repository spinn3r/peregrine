package peregrine.app.pagerank;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;

import org.apache.log4j.xml.DOMConfigurator;

public class Main {

    public static void main( String[] args ) throws Exception {

        Getopt getopt = new Getopt( args );
        
        int nr_nodes = getopt.getInt( "nr_nodes", 500 );
        int max_edges_per_node = getopt.getInt( "max_edges_per_node", 500 );
        
        System.out.printf( "Running with nr_nodes: %,d , max_edges_per_node: %,d\n", nr_nodes, max_edges_per_node );
        
        DOMConfigurator.configure( "conf/log4j.xml" );
        Config config = ConfigParser.parse( args );

        String path = "/pr/test.graph";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder.buildRandomGraph( writer, nr_nodes , max_edges_per_node );
        
        writer.close();

        new Pagerank( config ).exec( path );
        
    }

}