
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
import peregrine.reduce.sorter.*;
import peregrine.reduce.merger.*;

import com.spinn3r.log5j.Logger;

public class LocalReducer {

    private static final Logger log = Logger.getLogger();

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

        //ChunkSorter sorter = new ChunkSorter( config , partition, shuffleInput );
        ChunkSorter2 sorter = new ChunkSorter2( config , partition, shuffleInput );
        
        List<ChunkReader> sorted = new ArrayList();
        
        for ( ChunkReader reader : input ) {

            try {

                ChunkReader result = sorter.sort( reader );
                
                if ( result != null )
                    sorted.add( result );

            } catch ( Exception e ) {
                throw new Exception( "Unable to sort input: " + reader, e );
            }

        }

        final AtomicInteger nr_tuples = new AtomicInteger();
        
        ChunkMerger merger = new ChunkMerger( listener );
        
        merger.merge( sorted );
     
    }

}
