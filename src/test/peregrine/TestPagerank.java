package peregrine;

import peregrine.io.*;
import peregrine.pagerank.*;

public class TestPagerank extends peregrine.BaseTestWithMultipleConfigs {

    @Override
    public void doTest() throws Exception {

        doTest( 5000 * getFactor() , 100 ); 

    }

    private void doTest( int nr_nodes,
                         int max_edges_per_node ) throws Exception {

        // only 0 and 1 should be dangling.

        String path = "/pr/test.graph";

        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder.buildRandomGraph( writer, nr_nodes , max_edges_per_node );

        writer.close();
        
        new Pagerank( config ).exec( path );

    }

    public static void main( String[] args ) throws Exception {
        System.setProperty( "peregrine.test.config", "02:02:5" ); 
        runTests();
    }

}