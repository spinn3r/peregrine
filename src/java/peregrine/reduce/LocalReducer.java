
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

    private List<File> input = new ArrayList();

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

    public void add( File in ) {
        this.input.add( in );
    }
    
    public void sort() throws Exception {

        // The input list should first be sorted so that we sort by the order of
        // the shuffle files and not an arbitrary order

        Collections.sort( input );
        
        ChunkSorter sorter = new ChunkSorter( config , partition, shuffleInput );

        List<ChunkReader> sorted = sort( input );
        
        ChunkMerger merger = new ChunkMerger( listener );
        
        merger.merge( sorted );
     
    }

    public List<ChunkReader> sort( List<File> input ) throws IOException {

        List<ChunkReader> sorted = new ArrayList();

        int id = 0;

        String sort_dir = config.getPath( partition, String.format( "/tmp/%s" , shuffleInput.getName() ) );

        new File( sort_dir ).mkdirs();
        
        for ( File in : input ) {

            String path = String.format( "%s/sort-%s.tmp" , sort_dir, id++ );
            File out    = new File( path );
            
            log.info( "Writing temporary sort file %s", path );

            ChunkSorter sorter = new ChunkSorter( config , partition, shuffleInput );

            ChunkReader result = sorter.sort( in, out );
                
            if ( result != null )
                sorted.add( result );

        }

        return sorted;

    }
    
}
