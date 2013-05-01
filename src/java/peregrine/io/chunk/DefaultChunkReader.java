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
import java.util.*;
import java.math.*;

import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.sstable.*;
import peregrine.os.*;
import peregrine.sort.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;

import org.jboss.netty.buffer.*;

/**
 * Default ChunkReader implementation which uses mmap, and supports features
 * like CRC32, etc.
 */
public class DefaultChunkReader extends BaseSSTableChunk implements SequenceReader, ChunkReader, Closeable {
    
    //FIXME: seekTo uses the last block EVEN if the last key is too large.
    
    // magic numbers for chunk reader files.

    private static Charset ASCII = Charset.forName( "ASCII" );

    public static byte[] MAGIC_PREFIX   = "PC0".getBytes( ASCII );
    public static byte[] MAGIC_RAW      = "RAW".getBytes( ASCII );
    public static byte[] MAGIC_CRC32    = "C32".getBytes( ASCII );

    private File file = null;

    private StreamReader reader = null;

    /**
     * Length in bytes of the input.
     */
    private long length = -1;

    /**
     * The current item we are reading from.
     */
    private int idx = 0;

    private MappedFileReader mappedFile;

    private boolean closed = false;

    // The current key.
    private StructReader key = null;
    
    // The current value.
    private StructReader value = null;
    
    private int keyOffset = -1;

    // the buffer that backs this chunk
    private ChannelBuffer buffer = null;

    // the count of entries either in this block or in the entire file.  By
    // default it's the entire file but if we call restrict() with a given
    // DataBlock we update that value so hasNext() doesn't try to read past the
    // restricted block.
    private int count = 0;
    
    // When true, parse the number of items we are holding.
    private boolean readTrailer = true;

    protected TreeMap<StructReader,DataBlock> dataBlockLookup =
        new TreeMap( new StrictStructReaderComparator() );

    protected boolean minimal = false;
    
    // used internally for duplicate()
    protected DefaultChunkReader() {}
    
    public DefaultChunkReader( ChannelBuffer buff ) {
        init( buff );
    }

    /**
     * This allows us to read a SSTable without reading the trailer.  This is
     * slightly faster than reading the SSTable and seeking to the trailer but
     * not incredibly so...
     */
    public DefaultChunkReader( ChannelBuffer buff, boolean readTrailer ) {
        this.readTrailer = false;
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
        this.length = buff.writerIndex();

        if ( readTrailer ) {

            //seek the to the end and read the magic ... 
            assertLength(Integers.LENGTH);
            int magic = buffer.getInt( buffer.writerIndex() - Integers.LENGTH );

            if ( magic >= 0 ) {

                count = magic;
                minimal = true;
                
            } else {

                assertLength(Trailer.LENGTH);

                trailer.read( buff );
                count = (int)trailer.count;

                // read the file info
                if ( trailer.fileInfoOffset > 0 ) {
                    fileInfo.read( buff, trailer );
                }
                
                // read data and meta index

                if ( trailer.indexOffset > 0 ) {
                    
                    buff.readerIndex( (int)trailer.indexOffset );

                    for( int i = 0; i < trailer.indexCount; ++i ) {
                        DataBlock db = new DataBlock();
                        db.read( buff );
                        dataBlocks.add( db );

                        dataBlockLookup.put( StructReaders.wrap( db.firstKey ), db );
                    }

                    // TODO: add the last fake data block for the lastKey

                    BigInteger ptr = new BigInteger( fileInfo.lastKey );
                    ptr = ptr.add( BigInteger.valueOf( 1 ) );

                    byte[] pd = ptr.toByteArray();

                    // NOTE: BigInteger is conservative with padding so we have
                    // to add any byte padding back in.
                    if ( pd.length < fileInfo.lastKey.length ) {

                        byte[] tmp = new byte[fileInfo.lastKey.length];
                        System.arraycopy( pd, 0, tmp, fileInfo.lastKey.length - pd.length, pd.length );
                        pd = tmp;

                    }

                    dataBlockLookup.put( new StructReader( pd ), null );

                    for( int i = 0; i < trailer.indexCount; ++i ) {
                        MetaBlock mb = new MetaBlock();
                        mb.read( buff );
                        metaBlocks.add( mb );
                    }

                }

            }
                
            // jump us back to the beginning of the buffer.
            buff.readerIndex( 0 );

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

        if( idx < count ) {
            return true;
        } else {
            return false;
        }

    }

    @Override
    public void next() throws IOException {

        ++idx;
        keyOffset = reader.index() + 1;

        Record record = read( reader );

        key   = record.getKey();
        value = record.getValue();
        
    }

    public StructReader readEntry() {
        return readEntry( reader );
    }

    public static Record read( StreamReader reader ) {
        return new Record( readEntry( reader ),
                           readEntry( reader ) );
    }

    /**
     * Not part of the API but public so that sorting can read directly from
     * varint encoded key/value streams.
     */
    public static StructReader readEntry( StreamReader reader ) {

        try {

            int len = VarintReader.read( reader );
            
            return reader.read( len );
            
        } catch ( Throwable t ) {
            throw new RuntimeException( "Unable to read entry: " + reader , t );
        }
        
    }

    /**
     * Find the data block that could potentially hold the given key.
     */
    protected DataBlock findDataBlock( StructReader key ) {
        
        Map.Entry<StructReader,DataBlock> entry = dataBlockLookup.floorEntry(key);

        if ( entry == null )
            return null;

        return entry.getValue();

    }

    /**
     * Restrict the chunk reader hasNext and next operations to just the given
     * data block.
     */
    protected void restrict( DataBlock block ) {
        idx = 0;
        count = block.count;
        buffer.readerIndex( (int)block.offset );        
    }

    /**
     * Find a given record with a specific key.
     */
    public Record seekTo( StructReader key ) throws IOException {

        DataBlock block = findDataBlock( key );

        if ( block == null )
            return null;

        // FIXME: instead of creating a duplicate of the WHOLE
        // DefaultChunkReader we really only need to duplicate a context object.

        // FIXME: I do not think we should call duplicate() within seekTo ...
        
        DefaultChunkReader dup = duplicate();
        
        dup.restrict( block );

        while( dup.hasNext() ) {

            dup.next();

            if ( key.equals( dup.key() ) ) {
                return new Record( dup.key(), dup.value() );
            }
            
        }

        return null;
        
    }

    /**
     * Create a new instance with the same internal members.  This way we can
     * keep a cached index of open readers but concurrently search over them for
     * each new request without having them step on each other.
     */
    public DefaultChunkReader duplicate() {

        DefaultChunkReader dup = new DefaultChunkReader();

        dup.file = file;
        dup.reader = reader;
        dup.length = length;
        dup.idx = idx;
        dup.mappedFile = mappedFile;
        dup.closed = closed;
        dup.keyOffset = keyOffset;
        dup.buffer = buffer;
        dup.count = count;
        dup.readTrailer = readTrailer;
        dup.fileInfo = fileInfo;
        dup.trailer = trailer;
        dup.dataBlocks = dataBlocks;
        dup.metaBlocks = metaBlocks;
        dup.dataBlockLookup = dataBlockLookup;

        return dup;
        
    }
    
    @Override
    public StructReader key() throws IOException {
        return key;
    }

    @Override
    public StructReader value() throws IOException {
        return value;
    }

    /**
     * Get the raw data content with no trailer, index, etc.
     */
    public ChannelBuffer data() {

        if ( minimal ) {
            return buffer.slice( 0, buffer.writerIndex() - Integers.LENGTH );
        } else { 
            return buffer.slice( 0, (int)trailer.dataSectionLength );
        }

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
    public int count() throws IOException {
        return count;
    }

    @Override
    public String toString() {
        return String.format( "file: %s, length (in bytes): %,d, count: %,d", file, length, trailer.count );
    }

    @Override 
    public ChannelBuffer getBuffer() {
    	return buffer;
    }
    
    private void assertLength(int len) {
        if ( this.length < len )
            throw new RuntimeException( String.format( "File %s is too short (%,d bytes)", file.getPath(), length ) );
    }

    public int index() {
        return reader.index();
    }

}
