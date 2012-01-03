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
package peregrine.io.chunk;

import java.io.*;

import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;
import org.jboss.netty.buffer.*;

/**
 * Default ChunkReader implementation which uses mmap, and supports features
 * like CRC32, etc.
 */
public class DefaultChunkReader implements ChunkReader, Closeable {

    // magic numbers for chunk reader files.

    private static Charset ASCII = Charset.forName( "ASCII" );

    public static byte[] MAGIC_PREFIX   = "PC0".getBytes( ASCII );
    public static byte[] MAGIC_RAW      = "RAW".getBytes( ASCII );
    public static byte[] MAGIC_CRC32    = "C32".getBytes( ASCII );

    private File file = null;

    private StreamReader reader = null;

    private VarintReader varintReader;;

    /**
     * Length in bytes of the input.
     */
    private long length = -1;

    /**
     * number of key value pairs to deal with.
     */
    private int size = 0;

    /**
     * The current item we are reading from.
     */
    private int idx = 0;

    private MappedFileReader mappedFile;

    private boolean closed = false;

    public DefaultChunkReader( Config config, File file )
        throws IOException {

        mappedFile = new MappedFileReader( config, file );
        
        ChannelBuffer buff = mappedFile.map();
      
        init( buff, mappedFile.getStreamReader() );
        
    }

    public DefaultChunkReader( File file, ChannelBuffer buff )
        throws IOException {

        this.file = file;
        init( buff );

    }
    
    public DefaultChunkReader( byte[] data )
        throws IOException {
                 
        init( ChannelBuffers.wrappedBuffer( data ) );

    }

    public DefaultChunkReader( ChannelBuffer buff )
        throws IOException {

        init( buff );
        
    }
    
    /**
     * Get the backing file.
     */
    public MappedFileReader getMappedFile() {
        return mappedFile;
    }

    private void init( ChannelBuffer buff )
        throws IOException {

        init( buff, new StreamReader( buff ) );
        
    }
    
    private void init( ChannelBuffer buff, StreamReader reader )
        throws IOException {

        this.reader = reader;
        this.varintReader = new VarintReader( reader );
        this.length = buff.writerIndex();
        
        assertLength();
        setSize( buff.getInt( buff.writerIndex() - IntBytes.LENGTH ) );

    }
    
    @Override
    public boolean hasNext() throws IOException {

        if( idx < size ) {
            return true;
        } else {
            return false;
        }

    }

    @Override
    public StructReader key() throws IOException {
        ++idx;
        return readEntry();
    }

    @Override
    public StructReader value() throws IOException {
        return readEntry();
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;
        
        if ( mappedFile != null )
            mappedFile.close();

        closed = true;
        
    }

    @Override
    public String toString() {
        return String.format( "file: %s, length (in bytes): %,d, size: %,d", file, length, size );
    }

    private void assertLength() throws IOException {
        if ( this.length < IntBytes.LENGTH )
            throw new IOException( String.format( "File %s is too short (%,d bytes)", file.getPath(), length ) );
    }

    private void setSize( int size ) throws IOException {

        if ( size < 0 ) {
            throw new IOException( String.format( "Invalid size: %s (%s)", size, toString() ) );
        }

        this.size = size;
    }

    private StructReader readEntry() throws IOException {

        try {

            int len = varintReader.read();
            
            return reader.read( len );
            
        } catch ( Throwable t ) {
            throw new IOException( "Unable to parse: " + toString() , t );
        }
        
    }

    public static void main( String[] args ) throws Exception {

    }
    
}
