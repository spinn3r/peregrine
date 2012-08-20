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
import peregrine.io.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;
import org.jboss.netty.buffer.*;

/**
 * Default ChunkReader implementation which uses mmap, and supports features
 * like CRC32, etc.
 */
public class DefaultChunkReader implements SequenceReader, ChunkReader, Closeable {
    
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

    /**
     * The current key.
     */
    private StructReader key = null;
    
    /**
     * The current value.
     */
    private StructReader value = null;
    
    private int keyOffset = -1;
    
    private ChannelBuffer buffer = null;

    /**
     * When true, parse the number of items we are holding.
     */
    private boolean readSize = true;
    
    public DefaultChunkReader( ChannelBuffer buff ) {
        init( buff );
    }

    public DefaultChunkReader( ChannelBuffer buff, boolean readSize ) {
        this.readSize = false;
        init( buff );
    }

    public DefaultChunkReader( File file )
        throws IOException {

        this( null, file );
        
    }

    public DefaultChunkReader( Config config, File file )
        throws IOException {

        this.file = file;
        this.mappedFile = new MappedFileReader( config, file );
        
        ChannelBuffer buff = mappedFile.map();
      
        init( buff, mappedFile.getStreamReader() );
        
    }

    public void init( ChannelBuffer buff ) {

        init( buff, new StreamReader( buff ) );
        
    }
    
    private void init( ChannelBuffer buff, StreamReader reader ) {

    	this.buffer = buff;
        this.reader = reader;
        this.varintReader = new VarintReader( reader );
        this.length = buff.writerIndex();

        if ( readSize ) {
            assertLength();
            setSize( buff.getInt( buff.writerIndex() - IntBytes.LENGTH ) );
        }

    }

    /**
     * Get the backing file.
     */
    public MappedFileReader getMappedFile() {
        return mappedFile;
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
    public void next() throws IOException {

        ++idx;
        keyOffset = reader.index() + 1;
       
        key   = readEntry();
        value = readEntry();
        
    }
    
    @Override
    public StructReader key() throws IOException {
        return key;
    }

    @Override
    public StructReader value() throws IOException {
        return value;
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
    public int keyOffset() throws IOException {
        return keyOffset;
    }
    
    @Override 
    public int size() throws IOException {
        return size;
    }
    
    @Override
    public String toString() {
        return String.format( "file: %s, length (in bytes): %,d, size: %,d", file, length, size );
    }

    @Override 
    public ChannelBuffer getBuffer() {
    	return buffer;
    }
    
    private void assertLength() {
        if ( this.length < IntBytes.LENGTH )
            throw new RuntimeException( String.format( "File %s is too short (%,d bytes)", file.getPath(), length ) );
    }

    private void setSize( int size ) {

        if ( size < 0 ) {
            throw new RuntimeException( String.format( "Invalid size: %s (%s)", size, toString() ) );
        }

        this.size = size;
    }

    /**
     * Not part of the API but public so that sorting can read directly from
     * varint encoded key/value streams.
     */
    public StructReader readEntry() {

        try {

            int len = varintReader.read();
            
            return reader.read( len );
            
        } catch ( Throwable t ) {
            throw new RuntimeException( "Unable to read entry: " + toString() , t );
        }
        
    }

    public int index() {
        return reader.index();
    }
    
}
