package peregrine.io;

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

         this.chunkRef = new ChunkReference( partition );
         
    }

    public Tuple read() throws IOException {

        if ( chunkReader != null )
            t = chunkReader.read();

        if ( t == null ) {

            if ( chunkRef.local >= 0 )
                listener.onChunkEnd( chunkRef );
            
            if ( iterator.hasNext() ) {

                chunkRef.incr();
                
                chunkReader = iterator.next();

                listener.onChunk( chunkRef );
                
                t = chunkReader.read();
            }

        }

        return t;
        
    }

    public void close() throws IOException {

        if ( chunkReader != null )
            chunkReader.close();

    }

}

class DefaultLocalPartitionReaderListener implements LocalPartitionReaderListener {

    @Override
    public void onChunk( ChunkReference ref ) {}

    @Override
    public void onChunkEnd( ChunkReference ref ) {}

}