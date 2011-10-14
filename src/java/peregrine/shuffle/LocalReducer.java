
package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

public class LocalReducer {

    private List<ChunkReader> input = new ArrayList();

    private SortListener listener = null;

    public LocalReducer( SortListener listener ) {
        this.listener = listener;
    }

    public void add( ChunkReader reader ) {
        this.input.add( reader );
    }
    
    public void sort() throws Exception {

        ChunkSorter sorter = new ChunkSorter();

        List<ChunkReader> sorted = new ArrayList();

        // FIXME: these need to go to disk.. 
        
        for ( ChunkReader reader : input ) {

            System.out.printf( "SORTED\n" );
            sorted.add( sorter.sort( reader ) );
        }

        final AtomicInteger nr_tuples = new AtomicInteger();

        ChunkMerger merger = new ChunkMerger( listener );

        merger.merge( sorted );

    }

}
