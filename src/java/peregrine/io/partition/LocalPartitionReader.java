package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

/**
 * Read data from a partition from local storage.
 */
public class LocalPartitionReader {

    private String path = null;

    private List<ChunkReader> chunkReaders = new ArrayList();

    private Iterator<ChunkReader> iterator = null;

    private ChunkReader chunkReader = null;

    private Tuple t = null;

    private LocalPartitionReaderListener listener = null;

    private ChunkReference chunkRef = null;

    private boolean hasNext = false;
    
    public LocalPartitionReader( Partition partition,
                                 Host host, 
                                 String path ) throws IOException {
        
        this( partition, host, path, new DefaultLocalPartitionReaderListener() );
        
    }
    
    public LocalPartitionReader( Partition partition,
                                 Host host, 
                                 String path,
                                 LocalPartitionReaderListener listener ) throws IOException {

         this.chunkReaders = LocalPartition.getChunkReaders( partition, host, path );
         this.iterator = chunkReaders.iterator();
         this.listener = listener;
         this.path = path;
         
         this.chunkRef = new ChunkReference( partition );
         
    }

    public boolean hasNext() throws IOException {

        if ( chunkReader != null )
            hasNext = chunkReader.hasNext();

        if ( hasNext == false ) {

            if ( chunkRef.local >= 0 )
                listener.onChunkEnd( chunkRef );
            
            if ( iterator.hasNext() ) {

                chunkRef.incr();
                
                chunkReader = iterator.next();

                listener.onChunk( chunkRef );
                
                hasNext = chunkReader.hasNext();

            } else {
                hasNext = false;
            }

        }

        return hasNext;
        
    }

    public byte[] key() throws IOException {
        return chunkReader.key();
    }

    public byte[] value() throws IOException {
        return chunkReader.value();
    }

    public void close() throws IOException {

        if ( chunkReader != null )
            chunkReader.close();

    }

    public String toString() {
        return this.path;
    }
    
}

class DefaultLocalPartitionReaderListener implements LocalPartitionReaderListener {

    @Override
    public void onChunk( ChunkReference ref ) {}

    @Override
    public void onChunkEnd( ChunkReference ref ) {}

}