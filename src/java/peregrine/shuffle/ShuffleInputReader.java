package peregrine.shuffle;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import java.nio.*;
import java.nio.channels.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.async.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public class ShuffleInputReader {

    private static final Logger log = Logger.getLogger();

    public static int BUFFER_SIZE = 8192;

    // header lookup information for partition and where to start reading.
    private Map<Integer,Integer> lookup = new HashMap();

    private String path;

    private int packet_idx = 0;

    private int partition;

    /**
     * ALL known headers in this shuffle file.
     */
    protected List<Header> headers = new ArrayList();

    /**
     * The currently parsed header information for the given partition.
     */
    protected Header header = null;

    protected ChannelBuffer buffer = null;

    protected FileInputStream in = null;
    
    public ShuffleInputReader( String path, int partition ) throws IOException {

        this.path = path;
        this.partition = partition;

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
        
        in = new FileInputStream( file );
        StructReader struct = new StructReader( in );

        // read the magic.
        byte[] magic = struct.read( new byte[ ShuffleOutputWriter.MAGIC.length ] );

        int nr_partitions = struct.readInt();

        int start = -1;

        int point = ShuffleOutputWriter.MAGIC.length + IntBytes.LENGTH;
        
        for ( int i = 0; i < nr_partitions; ++i ) {

            Header current = new Header();
            
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

            if ( current.partition == partition ) {
                this.header = current;
                start = current.offset;
            }

            // record this for usage later if necessary.
            headers.add( current );
            
            point += ShuffleOutputWriter.LOOKUP_HEADER_SIZE;

        }

        if ( start == -1 ) {
            throw new IOException( String.format( "Unable to find start for partition %s in file %s",
                                                  partition, path ) );
        }

        MappedByteBuffer map = in.getChannel().map( FileChannel.MapMode.READ_ONLY, header.offset, header.length );
        this.buffer = ChannelBuffers.wrappedBuffer( map );

    }

    public ChannelBuffer getBuffer() {
        return buffer;
    }
    
    public Header getHeader() {
        return header;
    }
    
    public boolean hasNext() throws IOException {
        return packet_idx < header.nr_packets;
    }
    
    public ShufflePacket next() throws IOException {

        if ( packet_idx >= header.nr_packets )
            return null;

        ++packet_idx;

        int from_partition  = buffer.readInt();
        int from_chunk      = buffer.readInt();
        int to_partition    = buffer.readInt();
        int len             = buffer.readInt();

        if ( to_partition != this.partition )
           throw new IOException( "Read invalid partition data: " + to_partition );

        // FIXME: this should be a channel buffer slice.
        
        byte[] data = new byte[ len ];
        buffer.readBytes( data );

        // TODO: why is count -1 here?  That makes NO sense.
        int count = -1;
        
        ShufflePacket pack = new ShufflePacket( from_partition, from_chunk, to_partition, count, data );

        return pack;
        
    }

    public void close() throws IOException {
        in.close();
    }

    class Header {

        int partition;
        int offset;
        int nr_packets;
        int count;
        int length;
        
        public String toString() {
            
            return String.format( "partition: %s, offset: %,d, nr_packets: %,d, count: %,d, length: %,d" ,
                                  partition, offset, nr_packets, count, length );
            
        }
        
    }

    public static void main( String[] args ) throws IOException {

        String path = args[0];

        int partition = -1;
        
        if ( args.length == 2 ) {
            partition = Integer.parseInt( args[1] );
        }

        System.out.printf( "Reading from partition: %s\n", partition );
        
        // debug code to dump a shuffle.
        
        ShuffleInputReader reader = new ShuffleInputReader( path, partition );

        System.out.printf( "Headers: \n" );
        for( Header header : reader.headers ) {
            System.out.printf( "\t%s\n", header );
        }
        
        while( reader.hasNext() ) {

            ShufflePacket pack = reader.next();

            System.out.printf( "from_partition: %s, from_chunk: %s, to_partition: %s, data length: %,d\n",
                               pack.from_partition, pack.from_chunk, pack.to_partition, pack.data.length );

            System.out.printf( "%s\n", Hex.pretty( pack.data ) );
            
        }

    }

}

