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

import com.spinn3r.log5j.Logger;
import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.sstable.*;
import peregrine.os.*;
import peregrine.util.netty.*;


/**
 * <p>
 * Write key/value pairs to a given file on disk and include any additional
 * metadata (size, etc).
 * </p>
 *
 * <p> The output format is very simple.  The file is a collection of key value
 * pairs.  First read a varint.  This will denote how many bytes to read next.
 * That will become your key.  Then read another varint, then read that many
 * bytes and that will become your value.
 * </p>
 *
 * <p>
 * The on disk block format is similar to hbase and bigtable.  Records are written
 * at the beginning of the table in series of implicit data blocks.  At the end
 * of the table we then write index blocks which store the start key and offset
 * of each data block.  After that we write a series of meta blocks which can
 * contain key/value pairs.  The meta blocks stores information like the bloom
 * filter index.
 * </p>
 *
 * <p>
 * https://bitbucket.org/burtonator/peregrine/issue/195/design-sstable-file-layout
 * </p>
 */
public class DefaultChunkWriter extends BaseSSTableChunk implements ChunkWriter {

    protected static final Logger log = Logger.getLogger();

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

    public static int BUFFER_SIZE = 16384 ;

    // default block size for writing indexed chunks
    protected long blockSize = -1;

    // the main writable for all output.
    protected BufferedChannelBufferWritable writer = null;

    // true when closed.
    private boolean closed = false;

    // true when shutdown.
    private boolean shutdown = false;

    // the current IndexBlock
    protected IndexBlock indexBlock = null;

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

        if ( indexBlock == null || writer.length() - indexBlock.getOffset() > blockSize ) {
            rollover( key );
        }

        write( writer, key, value );

        trailer.incrRecordUsage( key.length() + value.length() );
        trailer.incrCount();
        indexBlock.incrCount();
        
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
        startIndexBlock(key);
    }

    private void endBlock() {

        if ( indexBlock != null && indexBlock.getCount() != 0 ) {
            endIndexBlock();
            addMetaBlockToIndex();
        }

    }

    // create a new data block and add it to the data block list and return the
    // newly created block.
    private void startIndexBlock(StructReader key) {

        // create a new index block with teh correct first key specified.
        indexBlock = new IndexBlock( key.toByteArray() );
        indexBlock.setOffset(writer.length());

        indexBlocks.add(indexBlock);

    }

    private void endIndexBlock() {

        //TODO: compression would go here.
        indexBlock.setLength(writer.length() - indexBlock.getOffset());
        indexBlock.setLengthUncompressed(indexBlock.getLength());
    }

    private void startMetaBlock() {

        metaBlock = new MetaBlock();
        metaBlock.setOffset(writer.length());

        metaBlocks.add( metaBlock );
        
    }

    private void endMetaBlock() {
        metaBlock.setLength(writer.length() - metaBlock.getOffset());
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

        trailer.setDataSectionLength(writer.length());
        
        endBlock();

        if ( minimal ) {

            writer.write( StructReaders.wrap( trailer.getCount() )
                              .getChannelBuffer() );
            
        } else {

            // write out file info
            if ( lastKey != null ) {
                fileInfo.setLastKey( lastKey.toByteArray() );
            }

            trailer.setFileInfoOffset(writer.length());
            fileInfo.write( writer );

            trailer.setIndexOffset(writer.length());
            trailer.setIndexCount(indexBlocks.size());

            //FIXME: the MetaBlocks here are never used.  In fact they just flat
            //out aren't referenced.  Dump both of them.. ONLY have the concept
            //of an IndexBlock and an Index the IndexBlock can have key/value
            //pairs in the future.
            //
            // We basically need an Index and IndexBlock setup.  The Index is a
            // region of the file that has a count which is an integer followed
            // by offset:length fields (which are both ints and represent 4 bytes
            // each for 8 bytes total.  This way we can keep the IndexBlocks on
            // disk and optionally in VFS page cache (for large disk-only storage).
            // On large disk arrays (say 16TB and only 12GB) it will be tight to
            // keep the bloom filter data in memory and this way the VFS page
            // cache can optionally keep it around.
            //
            // This isn't right.  DataBlocks should probably be renamed to index
            // blocks ANYWAY because we need to keep the start key in them but
            // we STILL need metablocks which just store bloom filter data.

            for( IndexBlock db : indexBlocks) {
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

