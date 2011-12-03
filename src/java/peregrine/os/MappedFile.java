package peregrine.os;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.http.*;
import peregrine.util.netty.*;
import peregrine.io.util.*;

/**
 * Facade around a MappedByteBuffer but we also support mlock on the mapped
 * pages, and closing all dependent resources.
 *
 *
 */
public class MappedFile implements Closeable {

    /**
     * JDK <= 1.6 can only mmap files less than 2GB ... 
     */
    public static final int MAX_MMAP = Integer.MAX_VALUE;
    
    protected FileInputStream in;

    protected FileOutputStream out;

    protected FileChannel channel;

    protected ChannelBuffer map;

    protected long offset = 0;

    protected long length = 0;

    protected boolean lock = false;

    protected List<Closeable> closeables = new ArrayList();

    protected FileChannel.MapMode mode;

    protected File file;

    protected boolean closed = false;

    protected static Map<String,FileChannel.MapMode> modes = new HashMap() {{

        put( "r",  FileChannel.MapMode.READ_ONLY );
        put( "rw", FileChannel.MapMode.READ_WRITE );
        
    }};

    /**
     * Simpler API for opening a file based on a mode string.
     */
    public MappedFile( File file, String mode ) throws IOException {

        FileChannel.MapMode mapMode = modes.get( mode );

        if ( mapMode == null )
            throw new IOException( "Invalid mode: " + mode );

        init( file, mapMode );

    }

    /**
     * Open a file based on the mode constant.
     */
    public MappedFile( File file, FileChannel.MapMode mode ) throws IOException {
        init( file, mode );
    }

    private void init( File file, FileChannel.MapMode mode ) throws IOException {

        this.file = file;

        if( mode.equals( FileChannel.MapMode.READ_ONLY ) ) {
            this.in = new FileInputStream( file );
            this.channel = in.getChannel();
        } else if ( mode.equals( FileChannel.MapMode.READ_WRITE ) ) {
            this.out = new FileOutputStream( file );
            this.channel = out.getChannel();
        } else {
            throw new IOException( "Invalid mode: " + mode );
        }

        this.mode = mode;
        this.length = file.length();
        
    }

    /**
     * Read from this mapped file.
     */
    public ChannelBuffer map() throws IOException {

        try {

            if ( map == null ) {
                
                int nr_regions = (int)Math.ceil( length / (double)MAX_MMAP );

                int region_offset = 0;
                int region_length = MAX_MMAP;

                ByteBuffer[] buffs = new ByteBuffer[ nr_regions ];
                int buff_idx = 0;
                
                for( int i = 0 ; i < nr_regions; ++i ) {

                    if ( region_offset + region_length > length ) {
                        region_length = (int)(length - region_offset);
                    }

                    if ( lock ) {
                        closeables.add( new MemLock( file, in.getFD(), region_offset, region_length ) );
                    }

                    MappedByteBuffer map = channel.map( mode, region_offset, region_length );

                    buffs[buff_idx++] = map;
                    
                    closeables.add( new ByteBufferCloser( map ) );

                    region_offset += region_length;
                    
                }

                map = ChannelBuffers.wrappedBuffer( buffs );
                
            }

            return map;

        } catch ( IOException e ) {

            throw new IOException( String.format( "Failed to map %s of length %,d at %,d",
                                                  file.getPath(), length, offset ), e );
            
        }
        
    }

    /**
     * Enables writing to this mapped file.
     */
    public ChannelBufferWritable getChannelBufferWritable() throws IOException {
        return new MappedChannelBufferWritable();
    }
    
    public boolean getLock() { 
        return this.lock;
    }

    public void setLock( boolean lock ) { 
        this.lock = lock;
    }
    
    public void close() throws IOException {
        
        if ( closed )
            return;

        closeables.add( in );
        closeables.add( out );
        closeables.add( channel );

        Closer.close( closeables );
        
        closed = true;

    }

    class MappedChannelBufferWritable implements ChannelBufferWritable {

        public void write( ChannelBuffer buff ) throws IOException {

            //FIXME: I'm NOT sure that this is the fastest write path
            channel.write( buff.toByteBuffer() );
            
        }
        
        public void shutdown() throws IOException {
            // noop 
        }
        
        public void close() throws IOException {
            MappedFile.this.close();
        }

    }

    class ByteBufferCloser implements Closeable {

        private ByteBuffer buff;
        
        public ByteBufferCloser( ByteBuffer buff ) {
            this.buff = buff;
        }
        
        @Override
        public void close() throws IOException {

            sun.misc.Cleaner cl = ((sun.nio.ch.DirectBuffer)buff).cleaner();

            if (cl != null) {
                cl.clean();
            }

        }

    }
    
}
