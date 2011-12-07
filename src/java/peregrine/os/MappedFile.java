package peregrine.os;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.http.*;
import peregrine.util.netty.*;
import peregrine.io.util.*;
import peregrine.config.*;

import com.spinn3r.log5j.Logger;

/**
 * Facade around a MappedByteBuffer but we also support mlock on the mapped
 * pages, and closing all dependent resources.
 *
 *
 */
public class MappedFile implements Closeable {

    private static final Logger log = Logger.getLogger();

    /**
     * How often should we force writing pages to disk.
     */
    public static long FORCE_PAGE_SIZE = 4096;
    
    public static boolean DEFAULT_AUTO_FORCE = true;

    public static boolean DEFAULT_AUTO_LOCK = false;

    protected FileInputStream in;

    protected FileOutputStream out;

    protected FileChannel channel;

    protected ChannelBuffer map;

    protected long offset = 0;

    protected long length = 0;

    protected boolean autoLock = DEFAULT_AUTO_LOCK;

    protected boolean autoForce = DEFAULT_AUTO_FORCE;

    protected Closer closer = new Closer();

    protected FileChannel.MapMode mode;

    protected File file;

    protected int fd;

    protected Config config;

    protected boolean fadviseDontNeedEnabled = false;

    protected long fallocateExtentSize = 0;

    protected StreamReader reader = null;

    protected MappedByteBuffer mappedByteBuffer = null;
    
    protected static Map<String,FileChannel.MapMode> modes = new HashMap() {{

        put( "r",  FileChannel.MapMode.READ_ONLY );
        put( "rw", FileChannel.MapMode.READ_WRITE );
        put( "w",  FileChannel.MapMode.READ_WRITE );
        
    }};

    public MappedFile( Config config, String path, String mode ) throws IOException {
        this( config, new File( path ), mode );
    }
    
    /**
     * Simpler API for opening a file based on a mode string.
     */
    public MappedFile( Config config, File file, String mode ) throws IOException {

        FileChannel.MapMode mapMode = modes.get( mode );

        if ( mapMode == null )
            throw new IOException( "Invalid mode: " + mode );

        init( config, file, mapMode );

    }

    /**
     * Open a file based on the mode constant.
     */
    public MappedFile( Config config, File file, FileChannel.MapMode mode ) throws IOException {
        init( config, file, mode );
    }

    private void init( Config config, File file, FileChannel.MapMode mode ) throws IOException {

        this.config = config;
        this.file = file;
        this.mode = mode;

        if ( config != null ) {

            fadviseDontNeedEnabled = config.getFadviseDontNeedEnabled();
            fallocateExtentSize = config.getFallocateExtentSize();
            
        }

        if( mode.equals( FileChannel.MapMode.READ_ONLY ) ) {

            this.in = new FileInputStream( file );
            this.channel = in.getChannel();
            this.fd = Native.getFd( in.getFD() );

        } else if ( mode.equals( FileChannel.MapMode.READ_WRITE ) ) {

            this.out = new FileOutputStream( file );
            this.fd = Native.getFd( out.getFD() );
            this.channel = out.getChannel();

        } else {
            throw new IOException( "Invalid mode: " + mode );
        }

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

                if ( autoLock ) {
                    closer.add( new MemLock( file, in.getFD(), offset, length ) );
                }

                mappedByteBuffer = channel.map( mode, offset, length );
                
                closer.add( new ByteBufferCloser( mappedByteBuffer ) );

                this.map = ChannelBuffers.wrappedBuffer( mappedByteBuffer );
                
            }

            reader = new StreamReader( map );
            
            return map;

        } catch ( IOException e ) {

            throw new IOException( String.format( "Failed to map %s of length %,d at %,d",
                                                  file.getPath(), length, offset ), e );
            
        }
        
    }

    public StreamReader getStreamReader() throws IOException {

        if ( reader == null )
            map();
        
        return reader;
    }
    
    /**
     * Enables writing to this mapped file.
     */
    public ChannelBufferWritable getChannelBufferWritable() throws IOException {

        log.info( "Creating new writer for %s with autoForce=%s", file, autoForce );
        
        return new MappedChannelBufferWritable();
        
    }
    
    public boolean getAutoLock() { 
        return this.autoLock;
    }

    public void setAutoLock( boolean autoLock ) { 
        this.autoLock = autoLock;
    }

    public boolean getAutoForce() { 
        return this.autoForce;
    }

    public void setAutoForce( boolean autoForce ) { 
        this.autoForce = autoForce;
    }

    public File getFile() {
        return file;
    }

    public int getFd() {
        return fd;
    }

    @Override
    public void close() throws IOException {
        
        if ( closer.closed() )
            return;

        closer.add( channel );
        closer.add( in );
        closer.add( out );

        closer.close();

    }

    @Override
    public String toString() {
        return file.toString();
    }
    
    class MappedChannelBufferWritable implements ChannelBufferWritable {

        long allocated = 0;

        long forced = 0;

        @Override
        public void write( ChannelBuffer buff ) throws IOException {

            length += buff.writerIndex();

            if ( length > allocated && fallocateExtentSize > 0 ) {
                
                fcntl.posix_fallocate( fd, allocated, fallocateExtentSize );

                allocated += fallocateExtentSize;
                
            }

            if ( autoForce && length > forced + FORCE_PAGE_SIZE ) {
                force();
                forced += length;
            }

            // this should use transferTo for the write.
            buff.getBytes( 0, channel, buff.writerIndex() );
            
        }
        
        @Override
        public void shutdown() throws IOException {
            // noop 
        }

        @Override
        public void force() throws IOException {
            channel.force( false );
        }

        @Override
        public void close() throws IOException {

            // if we're in fallocate mode, we now need to truncate the file so
            // that it is the correct length.

            if ( fallocateExtentSize > 0 ) {
                channel.truncate( length );
            }

            if ( autoForce ) {
                force();
            }
            
            MappedFile.this.close();
            
        }

        @Override
        public String toString() {
            return MappedFile.this.toString();
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

            if ( fadviseDontNeedEnabled ) {
                fcntl.posix_fadvise( fd, offset, length, fcntl.POSIX_FADV_DONTNEED );
            }
            
        }

    }
    
}
