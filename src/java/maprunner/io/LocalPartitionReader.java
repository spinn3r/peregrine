package maprunner.io;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.io.*;

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

    private int local_chunk_id = -1;
    
    public LocalPartitionReader( Partition partition,
                                 Host host, 
                                 String path ) throws IOException {

        this( partition, host, path, new LocalPartitionReaderListener() );
        
    }
    
    public LocalPartitionReader( Partition partition,
                                 Host host, 
                                 String path,
                                 LocalPartitionReaderListener listener ) throws IOException {

        this.chunkReaders = LocalPartition.getChunkReaders( partition, host, path );
        this.iterator = chunkReaders.iterator();
        this.listener = listener;
        
    }

    public Tuple read() throws IOException {

        if ( chunkReader != null )
            t = chunkReader.read();

        if ( t == null ) {

            if ( local_chunk_id >= 0 )
                listener.onChunkEnd( local_chunk_id );
            
            if ( iterator.hasNext() ) {
                
                ++local_chunk_id;
                chunkReader = iterator.next();
                
                listener.onChunkStart( local_chunk_id );
                
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