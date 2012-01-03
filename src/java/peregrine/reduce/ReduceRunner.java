/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
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
import peregrine.shuffle.*;
import peregrine.sysstat.*;

import com.spinn3r.log5j.Logger;

/**
 * Run a reduce over the the given partition.  This handles intermerging, memory
 * allocation of sorting, etc.
 */
public class ReduceRunner {

    private static final Logger log = Logger.getLogger();

    private List<File> input = new ArrayList();

    private SortListener listener = null;
    private Config config;
    private Partition partition;
    private ShuffleInputReference shuffleInput;

    private List<JobOutput> jobOutput = null;

    public ReduceRunner( Config config,
                         Partition partition,
                         SortListener listener,
                         ShuffleInputReference shuffleInput,
                         List<JobOutput> jobOutput ) {

        this.config = config;
        this.partition = partition;
        this.listener = listener;
        this.shuffleInput = shuffleInput;
        this.jobOutput = jobOutput;

    }

    /**
     * Add a given file to sort. 
     */
    public void add( File in ) {
        this.input.add( in );
    }
    
    public void sort() throws IOException {

        // The input list should first be sorted so that we sort by the order of
        // the shuffle files and not an arbitrary order
        Collections.sort( input );

        int pass = 0;
        
        String sort_dir = getTargetDir( pass );

        // on the first pass we're going to sort and use shuffle input...

        List<ChunkReader> readers = sort( input, sort_dir );

        while( true ) {

            log.info( "Working with %,d readers now." , readers.size() );
            
            if ( readers.size() < config.getShuffleSegmentMergeParallelism() ) {

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
    public void finalMerge( List<ChunkReader> readers ) throws IOException {

        log.info( "Merging on final merge with %,d readers (strategy=finalMerge)", readers.size() );
        
        PrefetchReader prefetchReader = null;

        try {

            SystemProfiler profiler = config.getSystemProfiler();

            prefetchReader = createPrefetchReader( readers );
            
            ChunkMerger merger = new ChunkMerger( listener, partition, jobOutput );
        
            merger.merge( readers );

            log.info( "Merged with profiler rate: \n%s", profiler.rate() );

        } finally {
            new Closer( prefetchReader ).close();
        }

    }

    /**
     * Do an intermediate merge writing to a temp directory.
     */
    public List<ChunkReader> interMerge( List<ChunkReader> readers, int pass )
        throws IOException {

        String target_path = getTargetPath( pass );
        
        // chunk readers pending merge.
        List<ChunkReader> pending = new ArrayList();
        pending.addAll( readers );

        List<ChunkReader> result = new ArrayList();

        int id = 0;
        
        while( pending.size() != 0 ) {

            String path = String.format( "%s/merged-%s.tmp" , target_path, id++ );
            
            List<ChunkReader> work = new ArrayList();

            // move readers from pending into work until work is full .
            while( work.size() < config.getShuffleSegmentMergeParallelism() && pending.size() > 0 ) {
                work.add( pending.remove( 0 ) );
            }

            log.info( "Merging %,d work readers into %s on intermediate pass %,d (strategy=interMerge)",
                      work.size(), path, pass );

            PrefetchReader prefetchReader = null;

            try { 

                SystemProfiler profiler = config.getSystemProfiler();

                prefetchReader = createPrefetchReader( work );

                ChunkMerger merger = new ChunkMerger( null, partition, jobOutput );

                final DefaultPartitionWriter writer = newInterChunkWriter( path );

                // when the cache is exhausted we first have to flush it to disk.
                prefetchReader.setListener( new PrefetchReaderListener() {
                        
                        public void onCacheExhausted() {

                            try {
                                log.info( "Writing %s to disk with flush()." , writer );
                                writer.flush();
                            } catch ( IOException e ) {
                                // this should NOT happen because we are only
                                // using a MappedByteBuffer here which shouldn't
                                // throw an exception but we need this interface
                                // to throw an exception because it could be
                                // doing other types of IO like networked IO
                                // which may in fact have an exception.
                                throw new RuntimeException( e );
                            }
                                
                        }

                    } );

                merger.merge( work, writer );

                result.add( newInterChunkReader( path ) );

                log.info( "Merged with profiler rate: \n%s", profiler.rate() );

            } finally {
                new Closer( prefetchReader ).close();
            }
                
        }

        return result;

    }

    protected PrefetchReader createPrefetchReader( List<ChunkReader> readers ) throws IOException {
        
        List<MappedFileReader> mappedFiles = new ArrayList();

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
        
        return prefetchReader;

    }

    protected ChunkReader newInterChunkReader( String path ) throws IOException {

        return new LocalPartitionReader( config, partition, path );
        
    }
    
    protected DefaultPartitionWriter newInterChunkWriter( String path ) throws IOException {

        boolean append = false;

        // we set autoSync to false for now so that pages don't get
        // automatically sent do disk.
        boolean autoSync = false;

        List<Host> hosts = new ArrayList() {{
            add( config.getHost() );
        }};
        
        return new DefaultPartitionWriter( config,
                                           partition,
                                           path,
                                           append,
                                           hosts,
                                           autoSync );

    }

    protected String getTargetPath( int pass ) {

        return String.format( "/tmp/%s.%s" , shuffleInput.getName(), pass );
        
    }
    
    protected String getTargetDir( int pass ) {

        return config.getPath( partition, getTargetPath( pass ) );

    }

    /**
     * Sort a given set of input files adn wite the results to the output
     * directory.
     */
    public List<ChunkReader> sort( List<File> input, String target_dir ) throws IOException {

        SystemProfiler profiler = config.getSystemProfiler();

        List<ChunkReader> sorted = new ArrayList();

        int id = 0;

        // make the parent dir for holding sort files.
        new File( target_dir ).mkdirs();

        log.info( "Going to sort() %,d files for %s", input.size(), partition );

        List<File> pending = new ArrayList();
        pending.addAll( input );

        Iterator<File> pendingIterator = pending.iterator();
        
        while( pendingIterator.hasNext() ) {
        	
        	List<ShuffleInputChunkReader> work = new ArrayList();
        	long workSize = 0;

            //factor in the overhead of the key lookup before we sort.
            //We will have to create the shuffle input readers HERE and then
            //pass them INTO the chunk sorter.  I also factor in the
            //amount of data IN this partition and not the ENTIRE file size.
        	while( pendingIterator.hasNext() ) {
        		
        		File current = pendingIterator.next();
                String path = current.getPath();                
                
                ShuffleInputReader reader = null;
                ShuffleHeader header = null;

                try {

                    reader = new ShuffleInputReader( config, path, partition );
                    header = reader.getHeader( partition );
                     
                } finally {
                    new Closer( reader ).close();
                }

        		workSize += header.length;
                workSize += header.count * KeyLookup.KEY_SIZE;
                
        		if ( workSize > config.getSortBufferSize() ) {
        			pendingIterator = pending.iterator();
        			break;        			
        		}

        		work.add( new ShuffleInputChunkReader( config, partition, path ) );
        		pendingIterator.remove();
        		
        	}
        	
            String path = String.format( "%s/sorted-%s.tmp" , target_dir, id++ );
            File out    = new File( path );
            
            log.info( "Writing temporary sort file %s", path );

            ChunkSorter sorter = new ChunkSorter( config , partition, shuffleInput );

            ChunkReader result = sorter.sort( work, out, jobOutput );

            if ( result != null )
                sorted.add( result );

        }

        log.info( "Sorted %,d files for %s", sorted.size(), partition );
        
        log.info( "Sorted with profiler rate: \n%s", profiler.rate() );

        return sorted;

    }
    
}
