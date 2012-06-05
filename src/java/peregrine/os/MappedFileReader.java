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
import peregrine.util.netty.*;
import peregrine.io.util.*;
import peregrine.config.*;

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

    protected MemLock memLock = null;

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

    /**
     * Read from this mapped file.
     */
    public ChannelBuffer map() throws IOException {

        closer.requireOpen();
        
        try {

            // in JDK 1.6 and earlier the max mmap we could have was 2GB so to
            // route around this problem we create a number of smaller mmap
            // files , one per 2GB region and then use a composite channel
            // buffer
            
            if ( map == null ) {

                if ( autoLock ) {
                    memLock = new MemLock( file, in.getFD(), offset, length );
                    closer.add( memLock );
                }

                if ( memLock != null ) {
                    new NativeMapStrategy().map();
                } else {
                    new ChannelMapStrategy().map();
                }
                
                this.map = ChannelBuffers.wrappedBuffer( byteBuffer );
                
            }

            if ( reader == null )
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
     * @see MemLock#unlockRegion
     */
    public void unlockRegion( long len ) throws IOException {

        closer.requireOpen();
        
        if ( memLock == null ) return;

        memLock.unlockRegion( len );
        
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

        // FIXME: the order here doesn't matter... 
        //closer.add( in );
        //closer.add( channel );

        //FIXME : this ONE line is fucking us over.. I think it's because the
        //MemLock was added above.  I need to track down what is causing this
        //but it's probably not good.  I wonder what happens if we REMOVE
        //MemLock or change the order.?

        closer.setTrace( true );
        
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
        public void close() throws IOException {

            super.close();

            if ( fadviseDontNeedEnabled ) {
                fcntl.posix_fadvise( fd, offset, length, fcntl.POSIX_FADV_DONTNEED );
            }
            
        }

    }

    interface MapStrategy {

        public void map() throws IOException;
        
    }

    class ChannelMapStrategy implements MapStrategy {

        public void map() throws IOException {
            
            byteBuffer = channel.map( FileChannel.MapMode.READ_ONLY, offset, length );

            //FIXME: add this back in:  closer.add( new MappedByteBufferCloser( byteBuffer ) );

        }

    }

    /**
     * A native mmap strategy that uses mmap directly via the MemLock .
     */
    class NativeMapStrategy implements MapStrategy {

        public void map() throws IOException {

            try {

                byteBuffer = (ByteBuffer)byteBufferConstructor.newInstance( (int)length,
                                                                            memLock.getAddress(),
                                                                            new Closer() );
                
                //FIXME: add this back in:  closer.add( new MappedByteBufferCloser( byteBuffer ) );

            } catch ( Exception e ) {
                throw new IOException( e );
            }
                
        }

        class Closer implements Runnable {
            
            public void run() {
                
                try {
                    memLock.close();
                } catch ( IOException e ) {
                    throw new RuntimeException( e );
                }
                
            }
            
        };

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
