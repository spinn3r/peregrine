
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

        ChunkSorter sorter = new ChunkSorter( config , partition, shuffleInput );
        
        List<ChunkReader> sorted = sort( input );
        
        ChunkMerger merger = new ChunkMerger( listener );
        
        merger.merge( sorted );
     
    }

    public List<ChunkReader> sort( List<File> input ) throws IOException {

        List<ChunkReader> sorted = new ArrayList();

        int id = 0;
        
        for ( File in : input ) {

            String relative = String.format( "/tmp/%s/sort-%s.tmp" , shuffleInput.getName(), id++ );
            String path     = config.getPath( partition, relative );
            File out        = new File( path );
            
            log.info( "Writing temporary sort file %s", path );

            ChunkSorter sorter = new ChunkSorter( config , partition, shuffleInput );

            ChunkReader result = sorter.sort( in, out );
                
            if ( result != null )
                sorted.add( result );

        }

        return sorted;

    }
    
}
