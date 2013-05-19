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
import java.nio.channels.*;

import com.spinn3r.log5j.Logger;
import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.http.*;
import peregrine.io.*;
import peregrine.io.sstable.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;

/**
 * <p>
 * Write key/value pairs to a given file on disk and include any additional
 * metadata (size, etc).
 *
 * <p> The output format is very simple.  The file is a collection of key value
 * pairs.  First read a varint.  This will denote how many bytes to read next.
 * That will become your key.  Then read another varint, then read that many
 * bytes and that will become your value.
 * 
 * https://bitbucket.org/burtonator/peregrine/issue/195/design-sstable-file-layout
 * 
 */
public class DefaultChunkWriter extends BaseSSTableChunk implements ChunkWriter {

    // FIXME: there should be three modes here.  MINIMAL ... which is the legacy
    // format.  Then we should have STREAM which includes the count and a
    // checksum.  Then we should have INDEXED which includes a block index as
    // well as CRC32.

    //FIXME: it would also be nice if we supported explicit meta blocks for the
    // stream protocol which are normally invisible to a client.  CRC32 would be
    // store here.  We could also include dapper style tracing in the response
    // and this way clients wouldn't see these optional response fields.

    // FIXME: DITCH the data block and meta block metaphor.. we are storing the
    // data blocks inline so at the END of the file we really only just have META
    // blocks.  JUST make them blocks. 

    // FIXME: this is WRONG .. the meta blocks need to go AFTER the data block
    // because otherwise to read the Nth meta block we have to read meta N-1 meta
    // blocks since they are variable width.  If we place them AFTER the data
    // blocks then we can easily read the Nth block by just seeking to the end of
    // the data block and reading the meta block off disk at that location.
    // ...
    // WRONG.. .we will STLL have the bloom filter issue.  Instead write a list
    // of offsets and lengths that the meta blocks can be indexed.

    protected static final Logger log = Logger.getLogger();

    public static int BUFFER_SIZE = 16384 ;

    // default block size for writing indexed chunks
    protected long blockSize = -1;

    // the main writable for all output.
    protected BufferedChannelBufferWritable writer = null;

    // the number of records written.
    private int count = 0;

    // true when closed.
    private boolean closed = false;

    // true when shutdown.
    private boolean shutdown = false;

    // the current DataBlock
    protected DataBlock dataBlock = null;

    // the current MetaBlock
    protected MetaBlock metaBlock = null;

    // the last key we saw in the whole stream.  This way we have the first key
    // of each block as well as the last key of the entire file.
    protected StructReader lastKey = null;

    // when true, skip the index and just write a minimal chunk stream.  Minimal
    // chunks have a -1 at the end of the stream which would normally be invalid
    // for a minimal stream since the last four bytes is the 'count'.
    protected boolean minimal = true;
    
    public DefaultChunkWriter( Config config, ChannelBufferWritable writer ) throws IOException {
        init( config, writer );
    }

    public DefaultChunkWriter( Config config, File file ) throws IOException {
        init( config, new MappedFileWriter( config, file ) );
    }

    private void init( Config config, ChannelBufferWritable writer ) {

        if ( config == null )
            throw new NullPointerException( "config" );
        
        this.writer = new BufferedChannelBufferWritable( writer, BUFFER_SIZE );
        this.blockSize = config.getSSTableBlockSize();
    }

    public void setBlockSize( long blockSize ) {
        this.blockSize = blockSize;
    }

    public long getBlockSize() {
        return this.blockSize;
    }

    public void setMinimal( boolean minimal ) {
        this.minimal = minimal;
    }

    public boolean getMinimal() {
        return this.minimal;
    }

    @Override
    public void write( StructReader key, StructReader value )
        throws IOException {

        if ( closed )
            throw new IOException( "closed" );

        if ( dataBlock == null || writer.length() - dataBlock.offset > blockSize ) {
            rollover( key );
        }

        write( writer, key, value );

        trailer.recordUsage += key.length() + value.length();
        trailer.incrCount();
        ++dataBlock.count;
        
        lastKey = key;
        
    }

    /**
     * Perform a DIRECT write on a ChannelBuffer of a key/value pair.
     */
    public static int write( ChannelBufferWritable writer ,
                             StructReader key,
                             StructReader value ) throws IOException {

    	int result = 0;

        // TODO: we have to use an atomic write so that the buffered writer can
        // make sure to get one key/value pair without truncating it
        // incorrectly.
        
        ChannelBuffer wrapped =
            ChannelBuffers.wrappedBuffer( StructReaders.varint( key.length() ).getChannelBuffer(),
                                          key.getChannelBuffer(),
                                          StructReaders.varint( value.length() ).getChannelBuffer(),
                                          value.getChannelBuffer() );

        writer.write( wrapped );
        
        result += wrapped.writerIndex();

        return result;
        
    }

    private void rollover( StructReader key) {
        endBlock();
        startDataBlock( key );
    }

    private void endBlock() {

        if ( dataBlock != null && dataBlock.count != 0 ) {
            endDataBlock();
            addMetaBlockToIndex();
        }

    }

    // FIXME: I hate all these method names ... startDataBlock, etc... rewrite
    // it all. 
    
    // create a new data block and add it to the data block list and return the
    // newly created block.
    private void startDataBlock( StructReader key ) {

        // create a new datablock with teh correct first key specified.
        dataBlock = new DataBlock( key.toByteArray() );
        dataBlock.offset = writer.length();

        dataBlocks.add( dataBlock );

    }

    private void endDataBlock() {

        //TODO: compression would go here.
        dataBlock.length = writer.length() - dataBlock.offset;
        dataBlock.lengthUncompressed = dataBlock.length;
    }

    private void startMetaBlock() {

        metaBlock = new MetaBlock();
        metaBlock.offset = writer.length();

        metaBlocks.add( metaBlock );
        
    }

    private void endMetaBlock() {
        metaBlock.length = writer.length() - metaBlock.offset;
    }

    // write out a new meta block file which would include bloom filter data as
    // well as any other additional metadata.
    private void addMetaBlockToIndex() {

        startMetaBlock();
        
        //noop right now.  We don't have any metadata to write.

        endMetaBlock();
        
    }

    /**
     * Perform all final operations before we actually close the writer.
     */
    public void shutdown() throws IOException {

        if ( shutdown )
            return;
        
        // write trailer to store the number of items.

        trailer.dataSectionLength = writer.length();
        
        endBlock();

        if ( minimal ) {

            writer.write( StructReaders.wrap( trailer.getCount() )
                              .getChannelBuffer() );
            
        } else {

            // write out file info
            if ( lastKey != null ) {
                fileInfo.lastKey = lastKey.toByteArray();
            }

            trailer.fileInfoOffset = writer.length();
            fileInfo.write( writer );

            trailer.indexOffset = writer.length();
            trailer.indexCount = dataBlocks.size();

            for( DataBlock db : dataBlocks ) {
                db.write( writer );
            }

            for( MetaBlock mb : metaBlocks ) {
                mb.write( writer );
            }

            // write the trailer
            trailer.write( writer );
        
        }

        writer.shutdown();
        
        shutdown = true;
        
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {

        if ( closed )
            return;

        shutdown();

        writer.close();

        closed = true;

    }

    @Override
    public long length() {
        return writer.length();
    }

}

