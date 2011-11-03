package peregrine;

import peregrine.io.*;
import peregrine.pagerank.*;

public class TestPagerank extends peregrine.BaseTestWithOneDaemon {

    public void test1() throws Exception {

        // only 0 and 1 should be dangling.

/*        
        String path = "/pr/test.graph";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder.addRecord( writer, 2, 0, 1 );
        GraphBuilder.addRecord( writer, 3, 1, 2 );
        GraphBuilder.addRecord( writer, 4, 2, 3 );
        GraphBuilder.addRecord( writer, 5, 2, 3 );
        GraphBuilder.addRecord( writer, 6, 2, 3 );
        GraphBuilder.addRecord( writer, 7, 2, 3 );
        GraphBuilder.addRecord( writer, 8, 2, 3 );
        GraphBuilder.addRecord( writer, 9, 2, 3 );
        GraphBuilder.addRecord( writer, 10, 2, 3 );
        GraphBuilder.addRecord( writer, 11, 2, 3 );

        writer.close();

        new Pagerank( config ).exec( path );

*/        
        
    }

    private void doRandomTest( int nr_nodes,
                               int max_edges_per_node ) throws Exception {

        // only 0 and 1 should be dangling.

        String path = "/pr/test.graph";

        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder.buildRandomGraph( writer, nr_nodes , max_edges_per_node );

        writer.close();
        
        new Pagerank( config ).exec( path );

    }
    
    public void test2() throws Exception {

        doRandomTest( 50000 , 100 );
        //doRandomTest( 500, 10 );

        //FIXME: this is the main test... 
        //doRandomTest( 50000000, 10 );
        
/*        
        doRandomTest( 500, 10 );
        doRandomTest( 600, 10 );
        doRandomTest( 700, 10 );
        doRandomTest( 800, 10 );
        doRandomTest( 900, 10 );
        doRandomTest( 1000, 100 );
        doRandomTest( 5000, 100 );

*/
        // doRandomTest( 2000, 100 );
        // doRandomTest( 3000, 100 );
        // doRandomTest( 4000, 100 );

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}