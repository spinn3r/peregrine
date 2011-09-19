
package maprunner.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public class MapCallable implements Callable {

    private Partition partition;
    private Host host;
    private String path;
    private int nr_shards;
    final private Mapper mapper;
    
    public MapCallable( Partition partition,
                        Host host ,
                        String path,
                        int nr_shards,
                        Mapper mapper ) {

        this.partition = partition;
        this.host = host;
        this.path = path;
        this.nr_shards = nr_shards;
        this.mapper = mapper;
        
    }

    public Object call() throws Exception {

        System.out.printf( "Running map jobs on host: %s\n", host );

        String chunk_path = Config.getDFSPath( host, path );

        File chunk_dir = new File( chunk_path ) ;

        File[] files = chunk_dir.listFiles();

        long host_chunk_prefix = (long)host.getId() << 32;
        
        int local_chunk_id = 0;

        for ( File file : files ) {

            if ( file.isDirectory() )
                continue;

            if ( ! file.getName().startsWith( "chunk" ) )
                continue;

            // all hosts in this partition have the same chunks but we only
            // index the ones we are responsible for.

            ++local_chunk_id;

            if ( local_chunk_id % (host.getPartitionMemberId() + 1) == 0 ) {

                long chunk_id = host_chunk_prefix | local_chunk_id;
                
                handleChunk( file, chunk_id );
                
            }

        }

        mapper.cleanup( partition );
        
        return null;
        
    }

    private void handleChunk( File file, long chunk_id ) throws Exception {

        ChunkListener listener = new ChunkListener() {

                public void onEntry( byte[] key, byte[] value ) {
                    mapper.map( key, value );
                }

            };

        mapper.onChunkStart( chunk_id );
        
        ExtractChunkReader reader = new ExtractChunkReader( file, listener );
        reader.read();

        mapper.onChunkEnd( chunk_id );

    }
    
}