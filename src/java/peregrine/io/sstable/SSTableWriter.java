package peregrine.io.sstable;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.os.*;

/**
 * SSTableWriter handles implementing the ChunkWriter interface for writing
 * key/value pairs but also adds support for writing data as blocks, optionally
 * compressing those blocks, and then keeping an index of metadata (bloom
 * filters) and indexing of the data blocks.  This way we can also support key
 * lookup by seekTo() and merging based on key seek when one of the left or
 * right side of the join is smaller.
 */
public class SSTableWriter implements SequenceWriter {

    // TODO: support per-SSTable metadata ... for example a schema so that we
    // could use something like protocol buffers or avro for the schema format.
    //
    // TODO: EVERY block should have a checksum algorithm.
    //
    // TODO: data blocks need to store the compressed size and decompressed
    // size.  This way I can quicky compute stats like % compression rate per
    // block.  I should also write on on the trailer so I can look there too.
    
    // default block size
    protected long blockSize = 65536;

    // information about the file we are writing to...
    protected FileInfo fileInfo = new FileInfo();

    // trailer information for the file
    protected Trailer trailer = new Trailer();

    // a list of all data blocks written so that we can write out their metadata on close()
    protected List<DataBlock> dataBlocks = new ArrayList();

    // a list of all meta blocks written so that we can write out their metadata on close()
    protected List<MetaBlock> metaBlocks = new ArrayList();

    // the writer for storing all data.
    protected MappedFileWriter writer = null;

    // the current DataBlock
    protected DataBlock dataBlock = null;

    // the current MetaBlock
    protected MetaBlock metaBlock = null;

    // the last key we saw in the whole stream.  This way we have the first key
    // of each block as well as the last key of the entire file.
    protected StructReader lastKey = null;

    public SSTableWriter( MappedFileWriter writer ) {

        this.writer = writer;

    }

    /**
     * Like SequenceWriter.write but also keeps track of data blocks and
     * metaBlocks and then writes this information to the SSTable on close.
     */
    @Override
    public void write( StructReader key, StructReader value ) throws IOException {

        // write the blocks to the current data block

        DefaultChunkWriter.write( writer, key, value );

        if ( dataBlock == null || writer.length() - dataBlock.offset > blockSize ) {
            rollover( key );
        }

        trailer.size += key.length() + value.length();
        ++trailer.count;
        ++dataBlock.count;

        lastKey = key;

    }

    // create a new data block and add it to the data block list and return the
    // newly created block.
    private void startDataBlock( StructReader key ) {

        dataBlock = new DataBlock( key.toByteArray() );
        dataBlock.offset = writer.length();

        dataBlocks.add( dataBlock );

    }

    private void endDataBlock() {

        //FIXME: count... 

        dataBlock.length = writer.length() - dataBlock.offset;
    }

    // write out a new meta block file which would include bloom filter data as
    // well as any other additional metadata.
    private void writeMetaBlock() {

        startMetaBlock();
        
        //noop right now.  We don't have any metadata to write.

        endMetaBlock();
        
    }

    private void startMetaBlock() {

        metaBlock = new MetaBlock();
        metaBlock.offset = writer.length();

        metaBlocks.add( metaBlock );
        
    }

    private void endMetaBlock() {

        metaBlock.length = writer.length() - metaBlock.offset;

    }

    private void rollover( StructReader key) {

        endBlock();
        
        startDataBlock( key );

    }

    private void endBlock() {

        if ( dataBlock != null && dataBlock.count != 0 ) {
            endDataBlock();
            writeMetaBlock();
        }

    }
    
    @Override
    public void close() throws IOException {

        endBlock();

        // write out file info

        if ( lastKey != null )
            fileInfo.lastKey = lastKey.toByteArray();

        trailer.fileInfoOffset = writer.length();
        fileInfo.write( writer );

        trailer.indexOffset = writer.length();
        trailer.indexCount = dataBlocks.size();

        for ( int i = 0; i < dataBlocks.size(); ++i ) {

            // write out the data block index
            // write out the meta block index

            DataBlock db = dataBlocks.get( i );
            MetaBlock mb = metaBlocks.get( i );

            db.write( writer );
            mb.write( writer );
            
        }

        // write the trailer
        trailer.write( writer );
        
        writer.close();
        
    }
    
}