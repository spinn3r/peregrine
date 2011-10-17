package peregrine.shuffle;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.async.*;

import com.spinn3r.log5j.Logger;

import static peregrine.pfsd.FSPipelineFactory.MAX_CHUNK_SIZE;

/**
 * Write shuffle output to disk.
 * 
 */
public class ShuffleOutputWriter {

    private static final Logger log = Logger.getLogger();

    /**
     * Chunk size for rollover files.
     */
    public static long COMMIT_SIZE = 134217728;

    /**
     * Magic number for this file.  Right now it is 'PSO1' which stands for
     * Peregrine.Reduce Output version 1.
     */
    public static final byte[] MAGIC =
        new byte[] { (byte)'P', (byte)'S', (byte)'O', (byte)'1' };
    
    private List<ShufflePacket> index = new ArrayList();

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

        this.path = path;
        this.config = config;

        new File( new File( path ).getParent() ).mkdirs();
        
    }
    
    public void accept( int from_partition,
                        int from_chunk,
                        int to_partition,
                        int count,
                        byte[] data ) throws IOException {

        if ( closed )
            throw new IOException( "closed" );
        
        ShufflePacket pack = new ShufflePacket( from_partition, from_chunk, to_partition, count, data );

        this.length += data.length;
        
        index.add( pack );

    }

    public boolean hasCapacity() {

        return length < COMMIT_SIZE;
        
    }

    private Map<Integer,ShuffleOutputPartition> buildLookup() throws IOException {

        // we are done working with this buffer.  serialize it to disk now and
        // close it out.

        List<Partition> partitions = config.getMembership().getPartitions( config.getHost() );

        log.info( "Going to write shuffle for %s to %s", partitions , path );
        
        if ( partitions == null || partitions.size() == 0 )
            throw new IOException( "No partitions defined for: " + config.getHost() );

        Map<Integer,ShuffleOutputPartition> lookup = new HashMap();

        // init the lookup with one ArrayList per partition.
        for( Partition part : partitions ) {
            lookup.put( part.getId(), new ShuffleOutputPartition() );
        }

        for( ShufflePacket current : index ) {

            if ( current == null ) {
                log.error( "Skipping null packet." );
                continue;
            }
            
            ShuffleOutputPartition shuffleOutputPartition = lookup.get( current.to_partition );

            if ( shuffleOutputPartition == null )
                throw new IOException( "No locally defined partition for: " + current.to_partition );

            shuffleOutputPartition.count += current.count;
            
            shuffleOutputPartition.packets.add( current );
            
        }

        return lookup;
        
    }
    
    public void close() throws IOException {

        closed = true;

        Map<Integer,ShuffleOutputPartition> lookup = buildLookup();

        log.info( "Going write output buffer with %,d entries.", lookup.size() );
        
        // now stream these out to disk...

        AsyncOutputStream out = new AsyncOutputStream( path );

        out.write( MAGIC );
        out.write( IntBytes.toByteArray( lookup.size() ) );
        
        // the offset in this chunk to start reading the data from this
        // partition and chunk.

        int nr_integers_per_header = 4;
        
        int off = MAGIC.length + IntBytes.LENGTH + (lookup.size() * IntBytes.LENGTH * nr_integers_per_header);

        for( int part : lookup.keySet() ) {

            ShuffleOutputPartition shuffleOutputPartition = lookup.get( part );

            int width = 0;

            for( ShufflePacket pack : shuffleOutputPartition.packets ) {

                int integers_per_shuffle_packet = 4;
                
                width += (IntBytes.LENGTH * integers_per_shuffle_packet);
                width += pack.data.length;
                
            }

            int nr_packets = shuffleOutputPartition.packets.size();

            //TODO: make sure ALL of these are acceptable.
            if ( shuffleOutputPartition.count < 0 )
                throw new IOException( "Header corrupted: count < 0" );
            
            out.write( IntBytes.toByteArray( part ) );
            out.write( IntBytes.toByteArray( off ) );
            out.write( IntBytes.toByteArray( nr_packets ) );
            out.write( IntBytes.toByteArray( shuffleOutputPartition.count ) );
            
            off += width;
                
        }
        
        for( int part : lookup.keySet() ) {

            ShuffleOutputPartition shuffleOutputPartition = lookup.get( part );

            for( ShufflePacket pack : shuffleOutputPartition.packets ) {
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

class ShuffleOutputPartition {

    public int count = 0;

    public List<ShufflePacket> packets = new ArrayList();
    
}