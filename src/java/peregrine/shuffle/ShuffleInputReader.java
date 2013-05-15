/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.shuffle;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.nio.channels.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.config.*;
import org.jboss.netty.buffer.*;

/**
 * Read packets from a shuffle file for a given set of partitions (usually the
 * primary).
 */
public class ShuffleInputReader implements Closeable {

    /**
     * The current packet index we're on.
     */
    private int packet_idx = 0;

    /**
     * ALL known headers in this shuffle file.
     */
    protected List<ShuffleHeader> headers = new ArrayList();

    protected Map<Partition,ShuffleHeader> headersByPartition = new HashMap();
    
    protected ChannelBuffer buffer = null;

    /**
     * The number of packet we need to read for all given partitions.
     */
    protected int nr_packets = 0;

    /**
     * Used so that we can move over partitions.
     * 
     */
    protected Iterator<Partition> partitionIterator = null;

    /**
     * The current partition we're working on.
     */
    protected Partition current = null;
    
    protected MappedFileReader mappedFile = null;;

    protected Config config = null;
    
    public ShuffleInputReader( Config config, String path, final Partition partition ) throws IOException {
        this( config, path, new ArrayList() {{ add( partition ); }} );
    }
        
    public ShuffleInputReader( Config config, String path, List<Partition> partitions ) throws IOException {

        this.config = config;
        
        if ( config == null )
            throw new NullPointerException( "config" );
        
        // pull out the header information 

        File file = new File( path );

        // NOTE: we read directly from a FileInputStream here because we need to
        // use skip to jump over the preamble. This MAY be a slight performance
        // issue but I doubt it.  There are only 12 bytes per partition and if
        // the local partition has 25 partitions this is only 300 bytes and in
        // the worse case scenario we read 288 extra bytes.  Ideally we would
        // store the shuffle correctly so that the partitions this machine was
        // responsible for reducing over were at the beginning of the shuffle
        // data but in practice this may be a premature optimization.

        // mmap the WHOLE file. We won't actually use these pages if we don't
        // read them so this make it less difficult to figure out what to map.

        this.mappedFile = new MappedFileReader( config, file );
        
        this.buffer = mappedFile.map();
        
        StructReader struct = new StructReader( buffer );

        struct.read( new byte[ ShuffleOutputWriter.MAGIC.length ] );

        int nr_partitions = struct.readInt();

        for ( int i = 0; i < nr_partitions; ++i ) {

            ShuffleHeader current = new ShuffleHeader();
            
            current.partition    = struct.readInt();
            current.offset       = struct.readInt();
            current.nr_packets   = struct.readInt();
            current.count        = struct.readInt();
            current.length       = struct.readInt();

            if ( current.partition   < 0 ||
                 current.offset      < 0 ||
                 current.nr_packets  < 0 ||
                 current.count       < 0 ||
                 current.length      < 0 ) {
                
                throw new IOException( "Header corrupted: " + current );
                
            }

            // record this for usage later if necessary.
            headers.add( current );
            headersByPartition.put( new Partition( current.partition ), current );

        }

        // make sure we have headers for ALL the partitions we requested
        for( Partition partition : partitions ) {

            ShuffleHeader header = headersByPartition.get( partition );
            
            if ( header == null ) {

                throw new IOException( String.format( "Unable to find header for partition %s in file %s with headers %s",
                                                      partition, path, headers ) );
                
            }

            // determine the number of packets we need to read from all the partitions.
            nr_packets += header.nr_packets;
            
        }
        
        partitionIterator = partitions.iterator();

        nextPartition();
        
    }

    private void nextPartition() {

        current = partitionIterator.next();

        ShuffleHeader header = headersByPartition.get( current );

        this.buffer.readerIndex( header.offset );
        
    }
    
    public ChannelBuffer getBuffer() {
        return buffer;
    }

    public ShuffleHeader getHeader( Partition partition ) {
        return headersByPartition.get( partition );
    }
    
    public boolean hasNext() throws IOException {
        return packet_idx < nr_packets;
    }
    
    public ShufflePacket next() throws IOException {

        if ( packet_idx >= nr_packets ) {
            return null;
        }

        ++packet_idx;

        int from_partition  = buffer.readInt();
        int from_chunk      = buffer.readInt();
        int to_partition    = buffer.readInt();
        int len             = buffer.readInt();
        int offset          = buffer.readerIndex();
        
        if ( to_partition != current.getId() ) {
            // skip over this partition and move to the next one.
            nextPartition();

            --packet_idx; /* this doesn't count */
            return next();
        }

        ChannelBuffer data = buffer.readSlice( len );
        
        // TODO: why is count -1 here?  That makes NO sense.
        int count = -1;
        
        ShufflePacket pack = new ShufflePacket( from_partition, from_chunk, to_partition, offset, count, data );

        return pack;
        
    }

    @Override
    public void close() throws IOException {

        mappedFile.close();

        // TODO fadvise away these pages now that we are done with them.
        
    }

    public static void main( String[] args ) throws IOException {

        String path = args[0];

        // debug code to dump a shuffle.

        List<Partition> partitions = new ArrayList();
        
        for( int i = 1; i < args.length; ++i ) {
            partitions.add( new Partition( Integer.parseInt( args[i] ) ) );
        }
        
        System.out.printf( "Reading from partitions: %s\n", partitions );

        ShuffleInputReader reader = new ShuffleInputReader( null, path, partitions );

        System.out.printf( "Headers: \n" );
        for( ShuffleHeader header : reader.headers ) {
            System.out.printf( "\t%s\n", header );
        }
        
        while( reader.hasNext() ) {

            ShufflePacket pack = reader.next();

            System.out.printf( "pack: %s\n", pack );

            System.out.printf( "%s\n", Hex.pretty( pack.data ) );
            
        }

    }

}

