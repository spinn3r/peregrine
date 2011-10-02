
package peregrine.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.io.*;

import peregrine.map.*;
import peregrine.shuffle.*;

public class MapperTask extends BaseMapperTask {

    private Mapper mapper;

    public Object call() throws Exception {

        this.mapper = (Mapper)super.newMapper();
        super.setup( this.mapper );

        //find the input for this task... Right now for the Mapper task only one
        //input file is supported.
        FileInputReference ref = (FileInputReference)getInput().getReferences().get( 0 );

        String path = ref.getPath();
        
        System.out.printf( "Running map jobs on host: %s\n", host );

        List<File> chunks = LocalPartition.getChunkFiles( partition, host, path );

        // NOTE: there are two ways to compute the partition_chunk_prefix ... we
        // could simply shift host ID 32 bits but then the printed form of the
        // int isn't usable for debug purposes.  If we just use some padding of
        // zeros then we still have plenty of hosts and plenty of chunks but
        // it's a bit more readable.

        long partition_chunk_prefix = (long)partition.getId() * 1000000000;
        
        int local_chunk_id = 0;

        for ( File file : chunks ) {

            long global_chunk_id = partition_chunk_prefix + local_chunk_id;
            
            callMapperOnChunk( file, global_chunk_id, local_chunk_id );

            ++local_chunk_id;

        }

        super.teardown( mapper );
        
        return null;
        
    }

    private void callMapperOnChunk( File file,
                                    final long global_chunk_id,
                                    final int local_chunk_id ) throws Exception {

        System.out.printf( "Handling chunk: %s on partition: %s with global_chunk_id: %016d, local_chunk_id: %s\n",
                           file.getPath(), partition, global_chunk_id, local_chunk_id );

        shuffleJobOutput.global_chunk_id = global_chunk_id;
        
        ChunkReader reader = new DefaultChunkReader( file );

        while( true ) {

            Tuple t = reader.read();

            if ( t == null )
                break;

            mapper.map( t.key, t.value );

        }

        reader.read();

    }
    
}
