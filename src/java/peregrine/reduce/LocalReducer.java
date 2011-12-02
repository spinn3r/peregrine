
package peregrine.reduce;

import java.io.*;
import java.util.*;

import peregrine.config.Config;
import peregrine.config.Partition;
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
    
    public void sort() throws IOException {

        // The input list should first be sorted so that we sort by the order of
        // the shuffle files and not an arbitrary order
        Collections.sort( input );

        int pass = 0;
        
        String sort_dir = getTargetDir( pass );

        List<ChunkReader> readers = sort( input, sort_dir );

        while( true ) {

            log.info( "Working with %,d readers now." , readers.size() );
            
            if ( readers.size() < config.getMergeFactor() ) {

                finalMerge( readers );
                
                break;

            } else {

                readers = interMerge( readers, ++pass );
                
            }
            
        }

        cleanup();
        
    }

    private void cleanup() throws IOException {

        // Now cleanup after ourselves.  See if the temporary directories exists
        // and if so purge them.
        
        for( int i = 0; i < Integer.MAX_VALUE; ++i ) {

            String path = getTargetDir( i );

            File dir = new File( path );
            if ( dir.exists() ) {
                Files.remove( dir );
            } else {
                break;
            }
            
        }

    }
    
    /**
     * Do the final merge including writing to listener when we are finished.
     */
    protected void finalMerge( List<ChunkReader> readers ) throws IOException {

        ChunkMerger merger = new ChunkMerger( listener, partition );
        
        merger.merge( readers );

    }

    /**
     * Do an intermediate merge writing to a temp directory.
     */
    protected List<ChunkReader> interMerge( List<ChunkReader> readers, int pass )
        throws IOException {

        String target_dir = getTargetDir( pass );

        // make sure the parent dir exists.
        new File( target_dir ).mkdirs();
        
        // chunk readers pending merge.
        List<ChunkReader> pending = new ArrayList();
        pending.addAll( readers );

        List<ChunkReader> result = new ArrayList();

        int id = 0;
        
        while( pending.size() != 0 ) {

            String path = String.format( "%s/merged-%s.tmp" , target_dir, id++ );
            File file = new File( path );
            
            List<ChunkReader> work = new ArrayList( config.getMergeFactor() );

            // move readers from pending into work until work is full .
            while( work.size() < config.getMergeFactor() && pending.size() > 0 ) {
                work.add( pending.remove( 0 ) );
            }

            log.info( "Merging %,d readers into %s", work.size(), path );
            
            ChunkMerger merger = new ChunkMerger( null, partition );
        
            merger.merge( readers, new DefaultChunkWriter( file ) );

            result.add( new DefaultChunkReader( file ) );
            
        }

        return result;

    }
    
    protected String getTargetDir( int pass ) {

        return config.getPath( partition, String.format( "/tmp/%s.%s" , shuffleInput.getName(), pass ) );

    }

    protected List<ChunkReader> sort( List<File> input, String target_dir ) throws IOException {

        List<ChunkReader> sorted = new ArrayList();

        int id = 0;

        // make the parent dir for holding sort files.
        new File( target_dir ).mkdirs();

        log.info( "Going to sort %,d files for %s", input.size(), partition );
        
        for ( File in : input ) {

            String path = String.format( "%s/sorted-%s.tmp" , target_dir, id++ );
            File out    = new File( path );
            
            log.info( "Writing temporary sort file %s", path );

            ChunkSorter sorter = new ChunkSorter( config , partition, shuffleInput );

            ChunkReader result = sorter.sort( in, out );

            if ( result != null )
                sorted.add( result );

        }

        log.info( "Sorted %,d files for %s", sorted.size(), partition );
        
        return sorted;

    }
    
}
