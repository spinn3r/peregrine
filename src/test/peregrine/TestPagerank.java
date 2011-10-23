package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.pagerank.*;

public class TestPagerank extends peregrine.BaseTestWithTwoDaemons {

    public void test1() throws Exception {

        // only 0 and 1 should be dangling.

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

    }

    public void test2() throws Exception {

        // only 0 and 1 should be dangling.

        String path = "/pr/test.graph";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder.buildRandomGraph( writer, 500 , 10 );

        writer.close();

        new Pagerank( config ).exec( path );

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}