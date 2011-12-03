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

    protected FileInputStream in;

    protected FileOutputStream out;

    protected FileChannel channel;

    protected ChannelBuffer map;

    protected long offset = 0;

    protected long length = 0;

    protected boolean lock = false;

    protected Closer closer = new Closer();

    protected FileChannel.MapMode mode;

    protected File file;

    protected static Map<String,FileChannel.MapMode> modes = new HashMap() {{

        put( "r",  FileChannel.MapMode.READ_ONLY );
        put( "rw", FileChannel.MapMode.READ_WRITE );
        
    }};

    public MappedFile( String path, String mode ) throws IOException {
        this( new File( path ), mode );
    }
    
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

            // in JDK 1.6 and earlier the max mmap we could have was 2GB so to
            // route around this problem we create a number of smaller mmap
            // files , one per 2GB region and then use a composite channel
            // buffer
            
            if ( map == null ) {

                if ( lock ) {
                    closer.add( new MemLock( file, in.getFD(), offset, length ) );
                }

                MappedByteBuffer map = channel.map( mode, offset, length );
                
                closer.add( new ByteBufferCloser( map ) );

                this.map = ChannelBuffers.wrappedBuffer( map );
                
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
        
        if ( closer.closed() )
            return;

        closer.add( channel );
        closer.add( in );
        closer.add( out );

        closer.close();

    }

    class MappedChannelBufferWritable implements ChannelBufferWritable {

        public void write( ChannelBuffer buff ) throws IOException {

            // we should probably fallocate here but it's going to be very
            // difficult to figure this out becasue stat() isn't super portable
            // due to a GCC and libc compilation issue and I won't be able to
            // get the block usage of a file.
            
            //length += buff.writerIndex();
            //if ( length 

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
