
package maprunner.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public class MapperCallable implements Callable {

    private Partition partition;
    private Host host;
    private String path;
    private int nr_hosts_in_partition;
    final private Mapper mapper;
    
    public MapperCallable( Partition partition,
                        Host host ,
                        String path,
                        int nr_hosts_in_partition,
                        Mapper mapper ) {

        this.partition = partition;
        this.host = host;
        this.path = path;
        this.nr_hosts_in_partition = nr_hosts_in_partition;
        this.mapper = mapper;
        
    }

    public Object call() throws Exception {

        System.out.printf( "Running map jobs on host: %s\n", host );

        String chunk_path = Config.getDFSPath( partition, host, path );

        File chunk_dir = new File( chunk_path ) ;

        File[] files = chunk_dir.listFiles();

        // NOTE: there are two ways to compute the partition_chunk_prefix ... we
        // could simply shift host ID 32 bits but then the printed form of the
        // int isn't usable for debug purposes.  If we just use some padding of
        // zeros then we still have plenty of hosts and plenty of chunks but
        // it's a bit more readable.

        long partition_chunk_prefix = (long)partition.getId() * 1000000000;
        
        int local_chunk_id = 0;

        for ( File file : files ) {

            if ( file.isDirectory() )
                continue;

            if ( ! file.getName().startsWith( "chunk" ) )
                continue;

            // all hosts in this partition have the same chunks but we only
            // index the ones we are responsible for.
            
            if ( ( local_chunk_id % nr_hosts_in_partition ) == host.getPartitionMemberId() ) {

                long global_chunk_id = partition_chunk_prefix + local_chunk_id;

                callMapperOnChunk( file, global_chunk_id, local_chunk_id );
                
            }

            ++local_chunk_id;

        }

        mapper.cleanup( partition );
        
        return null;
        
    }

    private void callMapperOnChunk( File file,
                                    final long global_chunk_id,
                                    final int local_chunk_id ) throws Exception {

        System.out.printf( "Handling chunk: %s on partition: %s with global_chunk_id: %016d, local_chunk_id: %s\n",
                           file.getPath(), partition, global_chunk_id, local_chunk_id );
        
        ChunkListener listener = new ChunkListener() {

                public void onEntry( byte[] key, byte[] value ) {
                    mapper.map( global_chunk_id, key, value );
                }

            };
        
        ChunkReader reader = new ChunkReader( file, listener );
        reader.read();

    }
    
}