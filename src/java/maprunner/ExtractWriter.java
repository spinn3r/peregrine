package maprunner;

import java.io.*;
import java.util.*;

import maprunner.util.*;

/**
 * Take a given stream of input in the form of (K,V) and route it and write the
 * data to the correct shard.  If your data is already sharded, with the correct
 * algorithm, at least right now you can run these in parallel
 */
public class ExtractWriter {

    private Object[] SHARDS = null;
    private Object[] SHARD_OUTPUT = null;

    private String path = null;

    private VarintWriter varintWriter = new VarintWriter();

    private int nr_shards = -1;
    
    public ExtractWriter( String path ) throws IOException {

        this.path = path;

        Map<Partition,List<Host>> shardMembership = Config.getShardMembership();
        
        int nr_shards = shardMembership.size();

        SHARDS       = new Object[nr_shards];
        SHARD_OUTPUT = new Object[nr_shards];

        int i = 0; 
        for( Partition key : shardMembership.keySet() ) {

            System.out.printf( "key: %s\n", key );

            List<Host> membership = shardMembership.get( key );

            List<ChunkWriter> output = new ArrayList();
            
            for ( Host member : membership ) {

                String chunk_path = Config.getDFSPath( member, path );
                String chunk = String.format( "%s/chunk0.dat" , chunk_path );
                
                output.add( new ChunkWriter( chunk ) );
                
                System.out.printf( "member path: %s\n", chunk_path );
                
            }

            SHARD_OUTPUT[i] = output;
            SHARDS[i]       = membership;

        }
        
    }

    public void write( Key key, Value value )
        throws IOException {

        write( key, value, false );
        
    }

    /**
     * If the Key is already a hashcode and we can route over it specify
     * keyIsHashcode=true.
     */
    public void write( Key key, Value value, boolean keyIsHashcode )
        throws IOException {

        byte[] key_bytes   = key.toBytes();
        byte[] value_bytes = value.toBytes();

        int shard = Config.route( key_bytes, nr_shards, keyIsHashcode );

        write( shard, key_bytes, value_bytes );
        
    }

    private void write( int shard, byte[] key_bytes, byte[] value_bytes )
        throws IOException {

        List<ChunkWriter> output = (List<ChunkWriter>) SHARD_OUTPUT[shard];

        for( ChunkWriter out : output ) {
            out.write( key_bytes, value_bytes );
        }
        
    }

    public void close() throws IOException {

        for (int i = 0; i < SHARD_OUTPUT.length; ++i ) {
            List<ChunkWriter> output = (List<ChunkWriter>) SHARD_OUTPUT[i];
            
            for( ChunkWriter out : output ) {
                out.close();
            }
        }

    }

}