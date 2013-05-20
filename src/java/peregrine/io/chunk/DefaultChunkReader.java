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

    public static byte[] MAGIC_PREFIX   = "PC0".getBytes(ASCII);
    public static byte[] MAGIC_RAW      = "RAW".getBytes( ASCII );
    public static byte[] MAGIC_CRC32    = "C32".getBytes( ASCII );

    private File file = null;

    private StreamReader reader = null;

    // Length in bytes of the input.
    private long length = -1;

    // The current item we are reading from.
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
        this( buff, true );
    }

    /**
     * This allows us to read a SSTable without reading the trailer.  This is
     * slightly faster than reading the SSTable and seeking to the trailer but
     * not incredibly so...
     */
    public DefaultChunkReader( ChannelBuffer buff, boolean readTrailer ) {
        this.readTrailer = readTrailer;
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
      
        init(buff, mappedFile.getStreamReader());

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
     * @return A list of all BackendRequests that are not complete.
     */
    public List<BackendRequest> seekTo( List<BackendRequest> requests, DataBlock block, RecordListener listener ) throws IOException {

        if ( requests.size() == 0 )
            return requests;
        
        // position us at the beginning of the block.
        buffer.readerIndex( (int)block.offset );        

        int seek_idx = 0;

        SeekToContext context = new SeekToContext( listener, requests );

        // the request+key we should be looking for...
        context.find = requests.remove( 0 );

        while( seek_idx < block.count ) {

            next();
            ++seek_idx;

            //FIXME: we need a metric for the number of keys we have read and
            //the number of keys we matched.

            //FIXME skip suspended clients...  Perhaps the way to do this is to
            //make the ITERATOR automatically skip suspended clients.  This way
            //ALL the code that uses BackendRequests can just transparently skip
            //them.  This is probably the right strategy moving forward since its
            //easy to implement and probably won't yield any bugs.

            context.handleScanRequests( key(), value() );

            context.handleCurrent( key(), value() );

            // terminate early if possible.
            if ( context.isFinished() )
                break;

        }

        // at this point any remaining requests are NOT matched.  If it's a scan
        // request it has an implicit start key (seek key) that wasn't matched
        // and if it's a get request for a specific key then that key wasn't found
        // in the routed data block.
        for( BackendRequest request : requests ) {
            request.setComplete(true);
        }

        if ( context.find != null )
            context.find.setComplete( true );

        // return any incomplete scan requests.  It does not make sense to return
        // any get/fetch requests because they won't be continued in the next
        // block
        return context.partialScanRequests;

    }

    // move some of the seek context information into a class to keep code and
    // methods tight.
    class SeekToContext {

        private RecordListener listener;

        private List<BackendRequest> requests;

        SeekToContext(RecordListener listener, List<BackendRequest> requests) {
            this.listener = listener;
            this.requests = requests;
        }

        protected BackendRequest find = null;

        // keep a list of keys that haven't yet finished serving all records.
        // these are going to be SCAN requests in practice.
        List<BackendRequest> partialScanRequests = new ArrayList<BackendRequest>();

        // once we've found scan requests we have to listen to them until they
        // are complete
        protected void handleScanRequests( StructReader key,
                                           StructReader value ) {

            Iterator<BackendRequest> scanIterator = partialScanRequests.iterator();

            while( scanIterator.hasNext() ) {

                BackendRequest current = scanIterator.next();

                current.visit( key, value );

                if ( current.isFound() ) {
                    listener.onRecord( current, key, value );
                }

                if ( current.isComplete() ) {
                    scanIterator.remove();
                }

            }

        }

        protected void handleCurrent( StructReader key, StructReader value ) {

            // find can be null during remaining scan requests.
            if ( find != null ) {

                while( true ) {

                    find.visit( key, value );

                    if ( find.isFound() ) {

                        listener.onRecord( find, key, value );

                        if ( find.isComplete() == false ) {
                            // this is a scan request and we're not done yet.
                            partialScanRequests.add(find);
                        }

                    }

                    if ( find.hasSibling() && requests.size() > 0 ) {
                        find = requests.remove( 0 );
                        continue;
                    }

                    break;

                }

                // either the key matched or we've gone PAST the key we are
                // searching for and we don't need to keep searching for this key.
                if ( find.isFound() || find.isComplete() ) {

                    if ( requests.size() > 0 ) {
                        find = requests.remove( 0 );
                    } else {
                        // we've handled all the keys in this request so we can quit now.
                        find = null;
                        return;
                    }

                }

            }

        }

        protected boolean isFinished() {

            // we are complete if there aren't any pending scan requests AND
            // there are no more requests to execute.
            return find == null && partialScanRequests.size() == 0;

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
