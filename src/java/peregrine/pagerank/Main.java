package peregrine.pagerank;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;

import org.apache.log4j.xml.DOMConfigurator;

public class Main {

    public static void main( String[] args ) throws Exception {

        int nr_nodes = Integer.parseInt( args[0] );
        int max_edges_per_node = Integer.parseInt( args[1] );

        System.out.printf( "Running with nr_nodes: %,d , max_edges_per_node: %,d\n", nr_nodes, max_edges_per_node );
        
        DOMConfigurator.configure( "conf/log4j.xml" );
        Config config = Config.parse( "conf/peregrine.conf", "conf/peregrine.hosts" );

        String path = "/pr/test.graph";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder.buildRandomGraph( writer, nr_nodes , max_edges_per_node );
        
        writer.close();

        new Pagerank( config ).exec( path );
        
    }

}