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
public class MappedFileReader extends BaseMappedFile implements Closeable {

    private static final Logger log = Logger.getLogger();

    public static boolean DEFAULT_AUTO_LOCK = false;

    protected FileInputStream in;

    protected ChannelBuffer map;

    protected boolean autoLock = DEFAULT_AUTO_LOCK;

    protected StreamReader reader = null;

    protected MappedByteBuffer mappedByteBuffer = null;

    public MappedFileReader( Config config, String path ) throws IOException {
        this( config, new File( path ) );
    }
    
    public MappedFileReader( Config config, File file ) throws IOException {

        init( config, file );

    }

    private void init( Config config, File file ) throws IOException {

        this.config = config;
        this.file = file;

        if ( config != null ) {
            fadviseDontNeedEnabled  = config.getFadviseDontNeedEnabled();
        }

        this.in = new FileInputStream( file );
        this.channel = in.getChannel();
        this.fd = Native.getFd( in.getFD() );

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

                mappedByteBuffer = channel.map( FileChannel.MapMode.READ_ONLY, offset, length );
                
                closer.add( new MappedByteBufferCloser( mappedByteBuffer ) );

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

        closer.add( channel );
        closer.add( in );

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
    
}
