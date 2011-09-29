
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;
import maprunner.io.*;

/**
 * 
 */
public class ChunkSorter {

    public static int DEFAULT_PARTITION_WIDTH = 25;

    private IntermediateChunkHelper intermediateChunkHelper
        = new IntermediateChunkHelper();

    private SortEntryFactory defaultSortEntryFactory = new DefaultSortEntryFactory();

    private SortEntryFactory topLevelSortEntryFactory = new TopLevelSortEntryFactory();

    //keeps track of the current input we're sorting.
    private int id = -1;
    
    public ChunkReader sort( ChunkReader input ) throws IOException {

        ++id;
        
        int size = input.size();

        Tuple[] data = new Tuple[size];
        Tuple[] dest = new Tuple[size];

        int idx = 0;
        while ( true ) {

            Tuple t = input.read();

            if ( t == null )
                break;

            data[idx] = t;
            dest[idx] = t;

            ++idx;
            
        }

        sort( data, dest , new FullTupleComparator() );

        TupleArrayChunkReader result = new TupleArrayChunkReader( dest );

        // TODO: this seems to add about 15-30% more CPU time to the job based
        // on my benchmarks.

        for( Tuple t : dest ) {
            System.out.printf( "FIXME: ChunkSorter thread=%s id=%d key=%s\n",
                               Thread.currentThread().getId(), id, Hex.encode( t.key ) );
        }

        //writer.close();
        
        return result;
        
    }

    public static <T> void sort(T[] aux, T[] a, Comparator<? super T> c) {
        //T[] aux = (T[])a.clone();
        mergeSort(aux, a, 0, a.length, 0, c);
    }

    private static final int INSERTIONSORT_THRESHOLD = 7;

    /**
     * Src is the source array that starts at index 0 Dest is the (possibly
     * larger) array destination with a possible offset low is the index in dest
     * to start sorting high is the end index in dest to end sorting off is the
     * offset into src corresponding to low in dest
     */
    private static void mergeSort( Object[] src,
                                   Object[] dest,
                                   int low,
                                   int high,
                                   int off,
                                   Comparator c ) {

        int length = high - low;

    	// Insertion sort on smallest arrays
    	if (length < INSERTIONSORT_THRESHOLD) {
    	    for (int i=low; i<high; i++)
                for (int j=i; j>low && c.compare(dest[j-1], dest[j])>0; j--)
                    swap(dest, j, j-1);
    	    return;
    	}

        // Recursively sort halves of dest into src
        int destLow  = low;
        int destHigh = high;
        low  += off;
        high += off;
        int mid = (low + high) >>> 1;
        mergeSort(dest, src, low, mid, -off, c);
        mergeSort(dest, src, mid, high, -off, c);
        
        // If list is already sorted, just copy from src to dest.  This is an
        // optimization that results in faster sorts for nearly ordered lists.
        if (c.compare(src[mid-1], src[mid]) <= 0) {
            // burton: this is a good optimization.  Keep it.
            System.arraycopy(src, low, dest, destLow, length);
            return;
        }

        // Merge sorted halves (now in src) into dest
        for(int i = destLow, p = low, q = mid; i < destHigh; i++) {
            if (q >= high || p < mid && c.compare(src[p], src[q]) <= 0)
                dest[i] = src[p++];
            else
                dest[i] = src[q++];
        }
    }

    /**
     * Swaps x[a] with x[b].
     */
    private static void swap(Object[] x, int a, int b) {
        Object t = x[a];
        x[a] = x[b];
        x[b] = t;
    }

}
