package maprunner.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;
import maprunner.io.*;

import junit.framework.*;

public class TestReduceSort extends TestCase {

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

        MapOutputSorter sorter = new MapOutputSorter( listener );

        sorter.add( TestChunkSorter.makeRandomSortChunk( 500 ) );
        sorter.add( TestChunkSorter.makeRandomSortChunk( 200 ) );
        sorter.add( TestChunkSorter.makeRandomSortChunk( 700 ) );

        //now run the sort to make sure there are no duplicate values.

        sorter.sort();
        
    }

    public static void main( String[] args ) throws Exception {

        TestReduceSort t = new TestReduceSort();

        t.test1();
        
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