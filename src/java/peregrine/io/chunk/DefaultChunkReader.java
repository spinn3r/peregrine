/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import java.nio.charset.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.sstable.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;
import peregrine.worker.clientd.requests.BackendRequest;

/**
 * Default ChunkReader implementation which uses mmap, and supports features
 * like CRC32, etc.
 */
public class DefaultChunkReader extends BaseSSTableChunk
    implements SequenceReader, ChunkReader, Closeable {
    
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
    // default it's the entire file but if we call seekTo() with a given
    // DataBlock we update that value so hasNext() doesn't try to read past the
    // restricted block.
    private int count = 0;
    
    // When true, parse the number of items we are holding.
    private boolean readTrailer = true;

    // true when we are in minimal and non-indexed mode.
    protected boolean minimal = false;

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

    // used internally for duplicate()
    protected DefaultChunkReader() {}

    // used internally for duplicate()
    protected DefaultChunkReader( DefaultChunkReader template ) {

        this.file = template.file;
        this.length = template.length;
        this.idx = template.idx;
        this.mappedFile = template.mappedFile;
        this.closed = template.closed;
        this.keyOffset = template.keyOffset;
        this.count = template.count;
        this.readTrailer = template.readTrailer;
        this.fileInfo = template.fileInfo;
        this.trailer = template.trailer;
        this.dataBlocks = template.dataBlocks;
        this.metaBlocks = template.metaBlocks;

        // we must call duplicate on the underlying buffer so that the reader
        // and writer indexes aren't mutated globally across requests.

        ChannelBuffer buff = template.buffer.duplicate();
        
        this.reader = new StreamReader( buff );
        this.buffer = buff;
        
    }

    /**
     * Create a new instance with the same internal members.  This way we can
     * keep a cached index of open readers but concurrently search over them for
     * each new request without having them step on each other.
     */
    public DefaultChunkReader duplicate() {
        return new DefaultChunkReader( this );
    }

    private void init( ChannelBuffer buff ) {

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
                count = trailer.getCount();

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
                    }

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
     * Fetch all the keys in the given DataBlock.
     */
    public void seekTo( List<BackendRequest> requests, DataBlock block, RecordListener listener ) throws IOException {

        if ( requests.size() == 0 )
            return;
        
        // position us at the beginning of the block.
        buffer.readerIndex( (int)block.offset );        

        int seek_idx = 0;

        // the request+key we should be looking for...
        BackendRequest find = requests.remove( 0 );

        // keep a list of keys that haven't yet finished serving all records.
        // these are going to be SCAN requests in practice.
        List<BackendRequest> incomplete = new ArrayList<BackendRequest>();

        while( seek_idx < block.count ) {

            next();
            ++seek_idx;

            //FIXME: seekKey can probably go away now that we have visit I think?

            //FIXME: keep looping through the requests because two clients might
            //have requested the same keys.

            //FIXME: we need a metric for the number of keys we have read and
            //the number of keys we matched.

            //FIXME skip suspended clients...  Perhaps the way to do this is to
            //make the ITERATOR automatically skip suspended clients.  This way
            //ALL the code that uses BackendRequests can just transparently skip
            //them.  This is probably the right strategy moving forward since its
            //easy to implement and probably won't yield any bugs.

            ListIterator<BackendRequest> scanIterator = incomplete.listIterator();

            while( scanIterator.hasNext() ) {
                BackendRequest current = scanIterator.next();

                if ( current.visit( key(), value() ) ) {
                    scanIterator.remove();;
                }

            }

            if ( find.visit( key(), value() ) ) {

                listener.onRecord( find, key(), value() );

                if ( find.isComplete() == false ) {
                    incomplete.add( find );
                }

                if ( requests.size() > 0 ) {
                    find = requests.remove( 0 );
                } else {
                    break;
                }

            }
            
        }
        
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
        return String.format( "file: %s, length (in bytes): %,d, count: %,d", file, length, trailer.getCount() );
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
