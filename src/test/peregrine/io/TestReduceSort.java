package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.reduce.*;
import peregrine.io.*;

import junit.framework.*;

public class TestReduceSort extends peregrine.BaseTestWithTwoDaemons {

    /**
     * test running with two lists which each have different values.
     */
    public void test1() throws Exception {

        final SortAsserter asserter = new SortAsserter();
        
        SortListener listener = new SortListener() {
                
                public void onFinalValue( byte[] key, List<byte[]> values ) {

                    asserter.assertAscending( key );
                    
                }
                
            };

        LocalReducer sorter = new LocalReducer( config, new Partition( 0 ), listener, new ShuffleInputReference( "default" ) );

        sorter.add( TestChunkSorter.makeRandomSortChunk( 500 ) );
        sorter.add( TestChunkSorter.makeRandomSortChunk( 200 ) );
        sorter.add( TestChunkSorter.makeRandomSortChunk( 700 ) );

        //now run the sort to make sure there are no duplicate values.

        sorter.sort();
        
    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}

class SortAsserter {

    FullKeyComparator comparator = new FullKeyComparator();

    byte[] last = null;
    
    public void assertAscending( byte[] key ) {

        if ( last != null && comparator.compare( last, key ) > 0 )
            throw new RuntimeException();
        
        last = key;

    }
    
}