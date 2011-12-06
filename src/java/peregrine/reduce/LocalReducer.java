
package peregrine.reduce;

import java.io.*;
import java.util.*;

import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.io.util.*;
import peregrine.reduce.sorter.*;
import peregrine.reduce.merger.*;
import peregrine.util.netty.*;
import peregrine.os.*;

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

        PrefetchReader prefetchReader = null;

        try {

            prefetchReader = createPrefetchReader( readers );

            ChunkMerger merger = new ChunkMerger( listener, partition );
        
            merger.merge( readers );

        } finally {
            new Closer( prefetchReader ).close();
        }

    }

    /**
     * Do an intermediate merge writing to a temp directory.
     */
    protected List<ChunkReader> interMerge( List<ChunkReader> readers, int pass )
        throws IOException {

        PrefetchReader prefetchReader = null;

        try {
            
            prefetchReader = createPrefetchReader( readers );
        
            String target_path = getTargetPath( pass );
            
            // chunk readers pending merge.
            List<ChunkReader> pending = new ArrayList();
            pending.addAll( readers );

            List<ChunkReader> result = new ArrayList();

            int id = 0;
            
            while( pending.size() != 0 ) {

                String path = String.format( "%s/merged-%s.tmp" , target_path, id++ );
                
                List<ChunkReader> work = new ArrayList( config.getMergeFactor() );

                // move readers from pending into work until work is full .
                while( work.size() < config.getMergeFactor() && pending.size() > 0 ) {
                    work.add( pending.remove( 0 ) );
                }

                log.info( "Merging %,d readers into %s on intermediate pass %,d", work.size(), path, pass );
                
                ChunkMerger merger = new ChunkMerger( null, partition );
            
                merger.merge( readers, newInterChunkWriter( path ) );

                result.add( newInterChunkReader( path ) );
                
            }

            return result;

        } finally {
            new Closer( prefetchReader ).close();
        }

    }

    protected PrefetchReader createPrefetchReader( List<ChunkReader> readers ) throws IOException {
        
        List<MappedFile> mappedFiles = new ArrayList();

        for( ChunkReader reader : readers ) {

            if ( reader instanceof DefaultChunkReader ) {

                DefaultChunkReader defaultChunkReader = (DefaultChunkReader) reader;
                mappedFiles.add( defaultChunkReader.getMappedFile() );

            } else if ( reader instanceof LocalPartitionReader ) {

                LocalPartitionReader localPartitionReader = (LocalPartitionReader)reader;
                
                List<DefaultChunkReader> defaultChunkReaders = localPartitionReader.getDefaultChunkReaders();

                for( DefaultChunkReader defaultChunkReader : defaultChunkReaders ) {
                    mappedFiles.add( defaultChunkReader.getMappedFile() );
                }

            } else {
                throw new IOException( "Unknown reader type: " + reader.getClass().getName() );
            }

        }

        PrefetchReader prefetchReader = new PrefetchReader( config, mappedFiles );
        prefetchReader.setEnableLog( true );
        prefetchReader.start();
        
        return prefetchReader;

    }

    protected ChunkReader newInterChunkReader( String path ) throws IOException {

        return new LocalPartitionReader( config, partition, path );
        
    }
    
    protected ChunkWriter newInterChunkWriter( String path ) throws IOException {
            
        return new DefaultPartitionWriter( config,
                                           partition,
                                           path,
                                           false,
                                           new ArrayList() {{
                                               add( config.getHost() );
                                           }} );

    }

    protected String getTargetPath( int pass ) {

        return String.format( "/tmp/%s.%s" , shuffleInput.getName(), pass );
        
    }
    
    protected String getTargetDir( int pass ) {

        return config.getPath( partition, getTargetPath( pass ) );

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
