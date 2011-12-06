package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.chunk.*;

/**
 * Read data from a partition from local storage.
 */
public class LocalPartitionReader implements ChunkReader {

    private String path = null;

    private List<DefaultChunkReader> chunkReaders = new ArrayList();

    private Iterator<DefaultChunkReader> iterator = null;

    private DefaultChunkReader chunkReader = null;

    private List<LocalPartitionReaderListener> listeners = new ArrayList();

    private ChunkReference chunkRef = null;

    private boolean hasNext = false;

    private Partition partition;
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path ) throws IOException {
        
        this( config, partition, path, new ArrayList() );
        
    }

    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path,
                                 LocalPartitionReaderListener listener ) throws IOException {

        this( config, partition, path, new ArrayList() );

        listeners.add( listener );
        
    }
    
    public LocalPartitionReader( Config config,
                                 Partition partition,
                                 String path,
                                 List<LocalPartitionReaderListener> listeners ) throws IOException {

        this.partition = partition;
        this.chunkReaders = LocalPartition.getChunkReaders( config, partition, path );
        this.iterator = chunkReaders.iterator();
        this.listeners = listeners;
        this.path = path;

        this.chunkRef = new ChunkReference( partition );

    }

    public List<DefaultChunkReader> getDefaultChunkReaders() {
        return chunkReaders;
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
            listener.onChunk( chunkRef );
        }
        
    }
    
    private void fireOnChunkEnd() {

        if ( chunkRef.local >= 0 ) {

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
        return String.format( "%s (%s):%s", path, partition, chunkReaders );
    }
    
}
