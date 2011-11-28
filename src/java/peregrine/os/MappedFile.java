package peregrine.os;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import org.jboss.netty.buffer.*;

import peregrine.http.*;
import peregrine.util.netty.*;

/**
 * Facade around a MappedByteBuffer but we also support mlock on the mapped
 * pages, and closing all dependent resources.
 *
 *
 */
public class MappedFile {

    protected FileInputStream in;

    protected FileOutputStream out;

    protected FileChannel channel;

    protected MappedByteBuffer map;

    protected long offset = 0;

    protected long length = 0;

    protected boolean lock = false;

    protected MemLock memLock = null;

    protected FileChannel.MapMode mode;

    protected File file;
    
    public MappedFile( File file, FileChannel.MapMode mode ) throws IOException {

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
    public MappedByteBuffer map() throws IOException {

        if ( map == null ) {

            if ( lock ) 
                memLock = new MemLock( file, in.getFD(), offset, length );

            map = channel.map( mode, offset, length );
            
        }

        return map;
        
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

       if ( memLock != null )
            memLock.release();

       if ( map != null )
           close( map );
       
       channel.close();

       if ( in != null )
           in.close();

       if ( out != null )
           out.close();

    }

    @SuppressWarnings("all")
    private void close( MappedByteBuffer map ) {

        sun.misc.Cleaner cl = ((sun.nio.ch.DirectBuffer)map).cleaner();

        if (cl != null) {
            cl.clean();
        }

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

}