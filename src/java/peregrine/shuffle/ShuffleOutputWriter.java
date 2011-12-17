package peregrine.shuffle;

import java.io.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.util.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;
import peregrine.values.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * Write shuffle output to disk.
 * 
 */
public class ShuffleOutputWriter implements Closeable {

    public static final int LOOKUP_HEADER_SIZE = IntBytes.LENGTH * 5;
    public static final int PACKET_HEADER_SIZE = IntBytes.LENGTH * 4;

    private static final Logger log = Logger.getLogger();

    /**
     * Magic number for this file.  Right now it is 'PSO1' which stands for
     * Peregrine.Reduce Output version 1.
     */
    public static final byte[] MAGIC =
        new byte[] { (byte)'P', (byte)'S', (byte)'O', (byte)'1' };

    // NOTE: in the future if we moved to a single threaded netty implementation
    // we won't need this but for now if multiple threads are writing to the
    // shuffler we can end up with corruption...
    
    private SimpleBlockingQueue<ShufflePacket> index = new SimpleBlockingQueue();

    /**
     * Path to store this output buffer once closed.
     */
    private String path;

    private int length = 0;

    private Config config;
    
    private ChannelBufferWritable output;

    private Closer closer = new Closer();

    private boolean closed = false;
    
    public ShuffleOutputWriter( Config config, String path ) {

        this.path = path;
        this.config = config;

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
        
        index.put( pack );

    }

    public boolean hasCapacity() {
        return length < config.getShuffleBufferSize();
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

        Iterator<ShufflePacket> it = index.iterator();
        
        while( it.hasNext() ) {

            ShufflePacket current = it.next();
            
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
    
    private void write( ChannelBuffer buff ) throws IOException {
    	output.write( buff );
    }

    @Override
    public void close() throws IOException {

        try {

            if ( closed )
                return;
            
            closed = true;

            Map<Integer,ShuffleOutputPartition> lookup = buildLookup();

            log.info( "Going write output buffer with %,d entries.", lookup.size() );

            File file = new File( path );
            
            // make sure the parent directory exists first.
            new File( file.getParent() ).mkdirs();

            MappedFile mapped = new MappedFile( config, file, "w" );
            
            this.output = mapped.getChannelBufferWritable();
            
            write( ChannelBuffers.wrappedBuffer( MAGIC ) );
            
            write( new StructWriter( IntBytes.LENGTH )
                           .writeInt( lookup.size() )
                           .getChannelBuffer() );
            
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
                    length += pack.data.writerIndex();
                    
                }

                int nr_packets = shuffleOutputPartition.packets.size();

                //TODO: make sure ALL of these are acceptable.
                if ( shuffleOutputPartition.count < 0 )
                    throw new IOException( "Header corrupted: count < 0" );

                int count = shuffleOutputPartition.count;

                write( new StructWriter( LOOKUP_HEADER_SIZE )
                           .writeInt( part )
                           .writeInt( offset )
                           .writeInt( nr_packets )
                           .writeInt( count )
                           .writeInt( length )
                           .getChannelBuffer() );
                
                offset += length;
                    
            }

            // *** STEP 2 .. write the actual data packets
            
            for( Replica replica : replicas ) {

                int part = replica.getPartition().getId();

                ShuffleOutputPartition shuffleOutputPartition = lookup.get( part );

                for( ShufflePacket pack : shuffleOutputPartition.packets ) {

                    write( new StructWriter( PACKET_HEADER_SIZE )
                				.writeInt( pack.from_partition )
                				.writeInt( pack.from_chunk )
                				.writeInt( pack.to_partition )
                				.writeInt( pack.data.writerIndex() )
                				.getChannelBuffer() );

                    // TODO: migrate this to using a zero copy system and write it
                    // directly to disk and avoid this copy.
                    
                    write( pack.data );

                }
                
            }

            index = null; // This is required for the JVM to more aggresively
                          // recover memory.  I did extensive testing with this
                          // and without setting index to null the JVM does not
                          // recover memory and it eventually leaks.  I don't
                          // think anything could be holding a reference to this
                          // though but this is a good pragmatic defense and
                          // solved the problem.

        } catch ( Throwable t ) {

            IOException e = null;

            if ( t instanceof IOException ) {
                e = (IOException) t;
            } else {
                e = new IOException( t );
            }

            log.error( "Unable to close: " , t );

            throw e;

        } finally {

            closer.add( output );
            
            closer.close();
            
        }
        
    }

    public String toString() {
        return String.format( "%s:%s", getClass().getSimpleName() , path );
    }
    
}

class ShuffleOutputPartition {

    public int count = 0;

    public List<ShufflePacket> packets = new ArrayList();
    
}