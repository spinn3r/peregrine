/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
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
 */
public class MappedFile implements Closeable {

    private static final Logger log = Logger.getLogger();

    public static boolean DEFAULT_AUTO_SYNC = true;

    public static boolean DEFAULT_AUTO_LOCK = false;

    protected FileInputStream in;

    protected FileOutputStream out;

    protected FileChannel channel;

    protected ChannelBuffer map;

    protected long offset = 0;

    protected long length = 0;

    protected boolean autoLock = DEFAULT_AUTO_LOCK;

    protected boolean autoSync = DEFAULT_AUTO_SYNC;

    protected Closer closer = new Closer();

    protected FileChannel.MapMode mode;

    protected File file;

    protected int fd;

    protected Config config;

    protected boolean fadviseDontNeedEnabled = false;

    protected long fallocateExtentSize = 0;

    protected long syncWriteSize = 0;

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

            fadviseDontNeedEnabled  = config.getFadviseDontNeedEnabled();
            fallocateExtentSize     = config.getFallocateExtentSize();
            syncWriteSize           = config.getSyncWriteSize();
            
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
                
                closer.add( new MappedByteBufferCloser( mappedByteBuffer ) );

                this.map = ChannelBuffers.wrappedBuffer( mappedByteBuffer );
                
            }

            reader = new StreamReader( map, this );
            
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

        log.info( "Creating new writer for %s with autoSync=%s", file, autoSync );
        
        return new MappedChannelBufferWritable();
        
    }
    
    public boolean getAutoLock() { 
        return this.autoLock;
    }

    public void setAutoLock( boolean autoLock ) { 
        this.autoLock = autoLock;
    }

    public boolean getAutoSync() { 
        return this.autoSync;
    }

    public void setAutoSync( boolean autoSync ) { 
        this.autoSync = autoSync;
    }

    public File getFile() {
        return file;
    }

    public int getFd() {
        return fd;
    }

    @Override
    public void close() throws IOException {

        if ( closer.isClosed() )
            return;

        closer.add( channel );
        closer.add( in );
        closer.add( out );

        closer.close();

    }

    public boolean isClosed() {
        return closer.isClosed();
    }
    
    @Override
    public String toString() {
        return file.toString();
    }

    /**
     * A writable which supports additional features such as continually forcing
     * pages to disk, fallocating the file in extents, etc.
     */
    class MappedChannelBufferWritable implements ChannelBufferWritable {

        long allocated = 0;

        /**
         * The number of bytes we have written out to disk (aligned on a 4k page
         * boundary for now).
         */
        long synced = 0;
                   
        @Override
        public void write( ChannelBuffer buff ) throws IOException {

            long newLength = length + buff.writerIndex();

            // see if we're about to write past the previous fallocate extent
            // size.
            
            if ( newLength > allocated && fallocateExtentSize > 0 ) {
                
                fcntl.posix_fallocate( fd, allocated, fallocateExtentSize );

                allocated += fallocateExtentSize;
                
            }

            if ( autoSync && syncWriteSize > 0 && newLength > synced + syncWriteSize ) {

                // slice the channel buffer into smaller regions which are page
                // aligned and then write these aligned pages then sync when the
                // pages are aligned.  Otherwise we would write a page 2x
                // because we first do a partial write, then when it is dirtied
                // again we re-write the same page.
                
                // the extra data that would be written.
                long extra = newLength % syncWriteSize;

                /*
                 * 
                 * This is a visual diagram of the page alignment.  Each
                 * character is a 512 byte sector to shorten the visual
                 * representation.
                 * 
                 * bytes written:   |===========|
                 * page alignment:  |==============|
                 * the write:                    |=====|
                 */
                
                if ( extra > 0 ) {
                    
                    // the boundary between page aligned and extra data for THIS
                    // buffer.
                    int boundary = (int)(buff.writerIndex() - extra);

                    // write the filled page BEFORE the extra data... 
                    write0( buff.readSlice( boundary ) );

                    // now update the buffer so that we write the data after the
                    // current filled page before we exit.
                    
                    buff = buff.readSlice( buff.writerIndex() - boundary );
                    
                }
                
                sync();

            }

            write0( buff );
            
        }

        /**
         * Write the entire ChannelBuffer RAW and update the length of bytes
         * we've written (and perform no other operations) to the current
         * channel.
         */
        private void write0( ChannelBuffer buff ) throws IOException {
            buff.getBytes( 0, channel, buff.writerIndex() );
            length += buff.writerIndex();
        }
        
        @Override
        public void shutdown() throws IOException {

            if ( autoSync && syncWriteSize > 0 ) {
                sync();
            }

            // if we're in fallocate mode, we now need to truncate the file so
            // that it is the correct length.

            if ( fallocateExtentSize > 0 ) {
                channel.truncate( length );
            }

        }

        @Override
        public void sync() throws IOException {
            
            channel.force( false );

            // TODO: I'm not sure this is the right strategy for ALL writes
            // ... temporary files should probably ALL be evicted but writes may
            // benefit if in situations where a box isn't tuned perfectly but it
            // doesn't make sense to optimize for poorly configured machines.

            /*
            if ( fadviseDontNeedEnabled ) {
                fcntl.posix_fadvise( fd, offset, length, fcntl.POSIX_FADV_DONTNEED );
            }
            */

            synced = length;

        }

        @Override
        public void flush() throws IOException {
            sync();
        }

        @Override
        public void close() throws IOException {

            shutdown();
            
            MappedFile.this.close();
            
        }

        @Override
        public String toString() {
            return MappedFile.this.toString();
        }
        
    }

    /**
     * A closeable which is smart enough to work on mapped byte buffers.
     */
    class MappedByteBufferCloser extends ByteBufferCloser {
        
        public MappedByteBufferCloser( ByteBuffer buff ) {
            super( buff );
        }
        
        @Override
        public void close() throws IOException {

            super.close();

            if ( fadviseDontNeedEnabled ) {
                fcntl.posix_fadvise( fd, offset, length, fcntl.POSIX_FADV_DONTNEED );
            }
            
        }

    }
    
}
