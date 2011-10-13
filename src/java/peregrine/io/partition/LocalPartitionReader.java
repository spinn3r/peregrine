package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

/**
 * Read data from a partition from local storage.
 */
public class LocalPartitionReader {

    private String path = null;

    private List<ChunkReader> chunkReaders = new ArrayList();

    private Iterator<ChunkReader> iterator = null;

    private ChunkReader chunkReader = null;

    private Tuple t = null;

    private List<LocalPartitionReaderListener> listeners = new ArrayList();

    private ChunkReference chunkRef = null;

    private boolean hasNext = false;
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 Host host, 
                                 String path ) throws IOException {
        
        this( config, partition, host, path, new ArrayList() );
        
    }

    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 Host host, 
                                 String path,
                                 LocalPartitionReaderListener listener ) throws IOException {

        this( config, partition, host, path, new ArrayList() );

        listeners.add( listener );
        
    }
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 Host host, 
                                 String path,
                                 List<LocalPartitionReaderListener> listeners ) throws IOException {

        this.chunkReaders = LocalPartition.getChunkReaders( config, partition, host, path );
        this.iterator = chunkReaders.iterator();
        this.listeners = listeners;
        this.path = path;
        
        this.chunkRef = new ChunkReference( partition );
         
    }

    public boolean hasNext() throws IOException {

        if ( chunkReader != null )
            hasNext = chunkReader.hasNext();

        if ( hasNext == false ) {

            fireOnChunkEnd();
            
            if ( iterator.hasNext() ) {

                chunkRef.incr();

                if ( chunkReader != null )
                    chunkReader.close();
                
                chunkReader = iterator.next();

                fireOnChunk();
                
                hasNext = chunkReader.hasNext();

            } else {
                hasNext = false;
            }

        }

        return hasNext;
        
    }

    private void fireOnChunk() {

        for( LocalPartitionReaderListener listener : listeners ) {
            System.out.printf( "FIXME firing... onChunk: %s %s\n" , chunkRef, listeners );
            listener.onChunk( chunkRef );
        }
        
    }
    
    private void fireOnChunkEnd() {

        if ( chunkRef.local >= 0 ) {

            System.out.printf( "FIXME firing... onChunkEnd: %s %s\n" , chunkRef, listeners );

            for( LocalPartitionReaderListener listener : listeners ) {
                listener.onChunkEnd( chunkRef );
            }

        }

    }
    
    public byte[] key() throws IOException {
        return chunkReader.key();
    }

    public byte[] value() throws IOException {
        return chunkReader.value();
    }

    public void close() throws IOException {

        if ( chunkReader != null ) {
            chunkReader.close();
            fireOnChunkEnd();
        }

    }

    public String toString() {
        return this.path;
    }
    
}
