package peregrine.pfsd.shuffler;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.async.*;

import com.spinn3r.log5j.Logger;

/**
 * Write shuffle output to disk.
 *
 * The file is a binary file.
 *
 * It first contains a header of:
 *
 * MAGIC
 * 
 *
 * partition ID: int (4 bytes).
 * length: int (4 bytes). 
 * data: binary data of `length' bytes.  
 * 
 */
public class ShuffleOutputWriter {

    private static final Logger log = Logger.getLogger();

    /**
     * Chunk size for rollover files.
     */
    public static long COMMIT_SIZE = 134217728;

    /**
     * Real world HTTP chunks size we would see.
     */
    public static int HTTP_CHUNK_SIZE = 3000;

    /**
     * Magic number for this file.  Right now it is 'PSO1' which stands for
     * Peregrine Shuffle Output version 1.
     */
    public static final byte[] MAGIC =
        new byte[] { (byte)'P', (byte)'S', (byte)'O', (byte)'1' };
    
    private AtomicInteger ptr = new AtomicInteger();

    private ShufflePacket[] index;

    /**
     * Path to store this output buffer once closed.
     */
    private String path;

    /**
     * True if we are closed so that we don't corupt ourselves.
     */
    private boolean closed = false;

    private int length = 0;

    private Config config;
    
    public ShuffleOutputWriter( Config config, String path ) {

        this.index = new ShufflePacket[ (int)(COMMIT_SIZE / HTTP_CHUNK_SIZE) ];
        this.path = path;
        this.config = config;
        
    }
    
    public void accept( int from_partition,
                        int from_chunk,
                        int to_partition,
                        byte[] data ) throws IOException {

        if ( closed )
            throw new IOException( "closed" );
        
        ShufflePacket pack = new ShufflePacket( from_partition, from_chunk, to_partition, data );

        this.length += data.length;
        
        index[ ptr.getAndIncrement() ] = pack;

    }

    public int length() {
        return this.length;
    }
    
    public void close() throws IOException {

        closed = true;
        
        // we are done working with this buffer.  serialize it to disk now and
        // close it out.

        List<Partition> partitions = config.getPartitionMembership().getPartitions( config.getHost() );

        log.info( "Going to write shuffle for %s", partitions );
        
        if ( partitions == null || partitions.size() == 0 )
            throw new IOException( "No partitions defined for: " + config.getHost() );
        
        Map<Integer,List<ShufflePacket>> lookup = new HashMap();

        // init the lookup with one ArrayList per partition.
        for( Partition part : partitions ) {
            lookup.put( part.getId(), new ArrayList() );
        }

        for( int i = 0; i < index.length; ++i ) {

            ShufflePacket current = index[i];

            if ( current == null )
                continue;

            if ( i > ptr.get() )
                break;

            List<ShufflePacket> packets = lookup.get( current.to_partition );

            if ( packets == null )
                throw new IOException( "No locally defined partition for: " + current.to_partition );
            
            packets.add( current );
            
        }

        log.info( "Going write output buffer with %,d entries.", lookup.size() );
        
        // now stream these out to disk...

        AsyncOutputStream out = new AsyncOutputStream( path );

        out.write( MAGIC );
        out.write( IntBytes.toByteArray( lookup.size() ) );
        
        // the offset in this chunk to start reading the data from this
        // partition and chunk.

        int nr_bytes_per_header = 3;
        
        int off = MAGIC.length + IntBytes.LENGTH + (lookup.size() * IntBytes.LENGTH * nr_bytes_per_header);

        for( int part : lookup.keySet() ) {

            List<ShufflePacket> packets = lookup.get( part );

            int width = 0;

            for( ShufflePacket pack : packets ) {

                int integers_per_shuffle_packet = 4;
                
                width += (IntBytes.LENGTH * integers_per_shuffle_packet);
                width += pack.data.length;
                
            }

            int count = packets.size();
            
            out.write( IntBytes.toByteArray( part ) );
            out.write( IntBytes.toByteArray( off ) );
            out.write( IntBytes.toByteArray( count ) );
            
            off += width;
                
        }
        
        for( int part : lookup.keySet() ) {

            List<ShufflePacket> packets = lookup.get( part );

            for( ShufflePacket pack : packets ) {
                out.write( IntBytes.toByteArray( pack.from_partition ) );
                out.write( IntBytes.toByteArray( pack.from_chunk ) );
                out.write( IntBytes.toByteArray( pack.to_partition ) );
                out.write( IntBytes.toByteArray( pack.data.length ) );
                out.write( pack.data );
            }
            
        }

        out.close();
        
    }
    
}
