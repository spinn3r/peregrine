
package peregrine.reduce;

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
    private Config config;
    private Partition partition;
    private ShuffleInputReference shuffleInput;

    public LocalReducer( Config config,
                         Partition partition,
                         SortListener listener,
                         ShuffleInputReference shuffleInput ) {

        this.config = config;
        this.partition = partition;
        this.listener = listener;
        this.shuffleInput = shuffleInput;

    }

    public void add( ChunkReader reader ) {
        this.input.add( reader );
    }
    
    public void sort() throws Exception {

        ChunkSorter sorter = new ChunkSorter( config , partition, shuffleInput );

        List<ChunkReader> sorted = new ArrayList();
        
        for ( ChunkReader reader : input ) {

            ChunkReader result = sorter.sort( reader );

            if ( result != null )
                sorted.add( result );

        }

        final AtomicInteger nr_tuples = new AtomicInteger();
        
        ChunkMerger merger = new ChunkMerger( listener );
        
        merger.merge( sorted );
     
    }

}
