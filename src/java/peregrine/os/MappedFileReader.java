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
import java.lang.reflect.*;

import org.jboss.netty.buffer.*;

import peregrine.http.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.io.util.*;
import peregrine.config.*;
import peregrine.util.*;

import com.spinn3r.log5j.Logger;

/**
 * Facade around a MappedByteBuffer but we also support mlock on the mapped
 * pages, and closing all dependent resources.
 *
 */
public class MappedFileReader extends BaseMappedFile implements Closeable {

    private static final Logger log = Logger.getLogger();

    public static boolean DEFAULT_AUTO_LOCK = false;

    protected FileInputStream in;

    protected ChannelBuffer map;

    protected boolean autoLock = DEFAULT_AUTO_LOCK;

    protected StreamReader reader = null;

    protected ByteBuffer byteBuffer = null;

    protected FileMapper fileMapper = null;
    
    public static boolean USE_NATIVE_MAP_STRATEGY = true;
    
    public static boolean USE_FADVISE_ON_CLOSE = true;

    public static boolean USE_CHANNEL_FOREGROUND_CLOSER = true;
    
    public static boolean USE_NATIVE_FOREGROUND_CLOSER = true;

    protected MappedByteBufferCloser mappedByteBufferCloser = null;

    public MappedFileReader( Config config, String path ) throws IOException {
        this( config, new File( path ) );
    }
    
    public MappedFileReader( Config config, File file ) throws IOException {
        init( config, file );
    }

    public MappedFileReader( String path ) throws IOException {
        this( new File( path ) );
    }

    public MappedFileReader( File file ) throws IOException {
        init( null, file );
    }

    private void init( Config config, File file ) throws IOException {

        this.config = config;
        this.file = file;

        if ( config != null ) {
            fadviseDontNeedEnabled  = config.getFadviseDontNeedEnabled();
        }

        this.in = new FileInputStream( file );
        this.channel = in.getChannel();
        this.fd = Platform.getFd( in.getFD() );

        this.length = file.length();

    }

    public static boolean getHoldOpenOverClose() { 
        return holdOpenOverClose.get();
    }

    public static void setHoldOpenOverClose( boolean value ) { 
        holdOpenOverClose.set( value );
    }

    static ThreadLocal<Boolean> holdOpenOverClose = new ThreadLocal<Boolean>() {

        public Boolean initialValue() {
            return false;
        }
        
    };
    
    /**
     * Read from this mapped file.
     */
    public ChannelBuffer map() throws IOException {

        log.info( "%s", new Tracepoint( "holdOpenOverClose" , holdOpenOverClose.get() ) ); //FIXME remove this.

        closer.requireOpen();
        
        try {

            // in JDK 1.6 and earlier the max mmap we could have was 2GB so to
            // route around this problem we create a number of smaller mmap
            // files , one per 2GB region and then use a composite channel
            // buffer
            
            if ( map == null ) {

                if ( USE_NATIVE_MAP_STRATEGY ) {
                    fileMapper = new FileMapper( file, in.getFD(), offset, length );
                    fileMapper.setLock( autoLock );
                    new NativeMapStrategy().map();
                } else {
                    new ChannelMapStrategy().map();
                }

                this.map = ChannelBuffers.wrappedBuffer( byteBuffer );
                
            }

            if ( reader == null ) {
                reader = new StreamReader( map );
            }
            
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
     * @see MemLock#unlockRegion
     */
    public void unlockRegion( long len ) throws IOException {

        closer.requireOpen();

        if ( USE_NATIVE_MAP_STRATEGY )
            fileMapper.unlockRegion( len );
        
    }
    
    public boolean getAutoLock() { 
        return this.autoLock;
    }

    public void setAutoLock( boolean autoLock ) { 
        this.autoLock = autoLock;
    }

    @Override
    public void close() throws IOException {

        if ( closer.isClosed() )
            return;

        if ( mappedByteBufferCloser != null )
            closer.add( mappedByteBufferCloser );
        
        if ( fileMapper != null )
            closer.add( fileMapper );

        closer.add( channel, in );

        closer.close();
    }

    /**
     * A closeable which is smart enough to work on mapped byte buffers.
     */
    class MappedByteBufferCloser extends ByteBufferCloser {
        
        public MappedByteBufferCloser( ByteBuffer buff ) {
            super( buff );
        }
        
        @Override
        protected void doClose() throws IOException {

            super.doClose();

            if ( fadviseDontNeedEnabled && USE_FADVISE_ON_CLOSE ) {
                fcntl.posix_fadvise( fd, offset, length, fcntl.POSIX_FADV_DONTNEED );
            }
            
        }

    }

    /**
     * Closer that ONLY fadvises away pages.
     */
    class FadviseCloser extends IdempotentCloser {

        @Override
        protected void doClose() throws IOException {

            if ( fadviseDontNeedEnabled && USE_FADVISE_ON_CLOSE ) {
                fcntl.posix_fadvise( fd, offset, length, fcntl.POSIX_FADV_DONTNEED );
            }
            
        }

    }
    
    // FIXME: remove the ChannelMapStrategy and ONLY go with the NativeMapStrategy
    class ChannelMapStrategy {

        public void map() throws IOException {
            
            byteBuffer = channel.map( FileChannel.MapMode.READ_ONLY, offset, length );
            
            if ( USE_CHANNEL_FOREGROUND_CLOSER ) {
                mappedByteBufferCloser = new MappedByteBufferCloser( byteBuffer );
            }

        }
            
    }
    
    /**
     * A native mmap strategy that uses mmap directly via the FileMapper .
     */
    class NativeMapStrategy {

        public void map() throws IOException {

            try {

                byteBuffer = (ByteBuffer)byteBufferConstructor.newInstance( (int)length,
                                                                            fileMapper.getAddress(),
                                                                            new BackgroundCloser() );

                mappedByteBufferCloser = new MappedByteBufferCloser( byteBuffer );
 
            } catch ( Exception e ) {
                throw new IOException( e );
            }
                
        }

        /**
         * Closer that runs in the background during GC to free up memory used
         * by mapped buffers that are no longer used.
         */
        class BackgroundCloser implements Runnable {
            
            public void run() {
                
            }
            
        }
        
    }
        
    private static Constructor byteBufferConstructor = null;
        
    static {

        try {
        
            Class clazz = Class.forName( "java.nio.DirectByteBufferR" );
            
            byteBufferConstructor = clazz.getDeclaredConstructor( int.class,
                                                                  long.class,
                                                                  Runnable.class );

            byteBufferConstructor.setAccessible( true );

        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
        
    }

}
