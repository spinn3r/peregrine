package peregrine.shuffle;

import java.io.*;
import java.util.*;
import peregrine.util.*;
import peregrine.util.primitive.IntBytes;
import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.config.Replica;
import peregrine.io.async.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * Write shuffle output to disk.
 * 
 */
public class ShuffleOutputWriter {

    public static final int LOOKUP_HEADER_SIZE = IntBytes.LENGTH * 5;
    public static final int PACKET_HEADER_SIZE = IntBytes.LENGTH * 4;

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
                        ChannelBuffer data ) throws IOException {
        
        if ( closed )
            throw new IOException( "closed" );

        ShufflePacket pack = new ShufflePacket( from_partition, from_chunk, to_partition, -1, count, data );
        
        this.length += data.capacity();
        
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

            if ( current.count < 0 )
                throw new IOException( "count < 0" );

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
        
        int offset =
            MAGIC.length + IntBytes.LENGTH + (lookup.size() * LOOKUP_HEADER_SIZE);

        // TODO: these should be stored in the primary order that they will be
        // reduce by priority on this box.

        // *** STEP 0 .. compute the order that we should write in
        List<Replica> replicas = config.getMembership().getReplicas( config.getHost() );

        // *** STEP 1 .. write the header information

        for( Replica replica : replicas ) {

            int part = replica.getPartition().getId();
            
            ShuffleOutputPartition shuffleOutputPartition = lookup.get( part );

            // the length of ALL the data for this partition.
            int length = 0;

            for( ShufflePacket pack : shuffleOutputPartition.packets ) {
                
                length += PACKET_HEADER_SIZE;
                length += pack.data.capacity();
                
            }

            int nr_packets = shuffleOutputPartition.packets.size();

            //TODO: make sure ALL of these are acceptable.
            if ( shuffleOutputPartition.count < 0 )
                throw new IOException( "Header corrupted: count < 0" );

            int count = shuffleOutputPartition.count;
            
            out.write( IntBytes.toByteArray( part ) );
            out.write( IntBytes.toByteArray( offset ) );
            out.write( IntBytes.toByteArray( nr_packets ) );
            out.write( IntBytes.toByteArray( count ) );
            out.write( IntBytes.toByteArray( length ) );
            
            offset += length;
                
        }

        // *** STEP 2 .. write the actual data packets
        
        for( Replica replica : replicas ) {

            int part = replica.getPartition().getId();

            ShuffleOutputPartition shuffleOutputPartition = lookup.get( part );

            for( ShufflePacket pack : shuffleOutputPartition.packets ) {

                out.write( IntBytes.toByteArray( pack.from_partition ) );
                out.write( IntBytes.toByteArray( pack.from_chunk ) );
                out.write( IntBytes.toByteArray( pack.to_partition ) );
                out.write( IntBytes.toByteArray( pack.data.capacity() ) );

                // TODO: migrate this to using a zero copy system and write it
                // directly to disk and avoid this copy.
                
                byte[] data = new byte[ pack.data.capacity() ];
                pack.data.getBytes( 0, data );
                
                out.write( data );

            }
            
        }

        out.close();

        index = null; // This is required for the JVM to more aggresively
                      // recover memory.  I did extensive testing with this and
                      // without setting index to null the JVM does not recover
                      // memory and it eventually leaks.  I don't think anything
                      // could be holding a reference to this though but this is
                      // a good pragmatic defense and solved the problem.
        
    }

    public String toString() {
        return String.format( "%s:%s", getClass().getSimpleName() , path );
    }
    
}

class ShuffleOutputPartition {

    public int count = 0;

    public List<ShufflePacket> packets = new ArrayList();
    
}