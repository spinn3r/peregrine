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

package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.util.*;

import org.jboss.netty.buffer.*;

import peregrine.util.netty.*;

/**
 * Trailer on a file denoteing metadata about that file.
 */
public class Trailer {

    //length in bytes of the trailer.  This is a fixed width data structure.
    public static final int LENGTH = (9 * Longs.LENGTH) + Integers.LENGTH;

    //magic number for the tail of the chunk stream.  All future magic should
    //be negative since the first version did not include magic and its writing
    //the number of keys in the stream.
    private static final int MAGIC = -1;
    
    private static final long VERSION_OFFSET = 200000;

    private long compressionAlgorithm = 0;
    
    private long count = 0;

    private long recordUsage = 0;

    private long indexCount = 0;
    
    private long indexOffset = -1;
    
    private long fileInfoOffset = -1;

    private long version = VERSION_OFFSET + 0;

    private long dataSectionLength = 0;

    private long dataBlockFormat = 1;

    public void setFileInfoOffset( long fileInfoOffset ) { 
        this.fileInfoOffset = fileInfoOffset;
    }

    public int getFileInfoOffset() {
        return (int)this.fileInfoOffset;
    }

    public void setIndexOffset( long indexOffset ) { 
        this.indexOffset = indexOffset;
    }

    public long getIndexOffset() { 
        return this.indexOffset;
    }

    /**
     * The number of index entries.
     */
    public long getIndexCount() { 
        return this.indexCount;
    }

    public void setIndexCount( long indexCount ) { 
        this.indexCount = indexCount;
    }

    /**
     * Size, in bytes, of the number of bytes of both keys and values.
     */
    public long getRecordUsage() { 
        return this.recordUsage;
    }

    public void setRecordUsage( long recordUsage ) { 
        this.recordUsage = recordUsage;
    }

    public void incrRecordUsage( long incr ) {
        recordUsage += incr;
    }

    /**
     * The number of records in this SSTable.
     */
    public int getCount() {
        return (int)this.count;
    }

    public void incrCount() {
        ++count;
    }

    public void setCount( int count ) {

        if ( count < 0 )
            throw new IllegalArgumentException( "count=" + count );
        
        this.count = count;

    }

    /**
     * Get the compression algorithm we are using. 
     */
    public long getCompressionAlgorithm() { 
        return this.compressionAlgorithm;
    }

    public void setCompressionAlgorithm( long compressionAlgorithm ) { 
        this.compressionAlgorithm = compressionAlgorithm;
    }

    /**
     * The version number (and magic number) for the file.  All versions should
     * start off with a specific offset so that we can use this portion as the
     * magic number.  This still gives us plenty of versions to play with since
     * we have 2^64 and doesn't require another magic field.
     */
    public long getVersion() {
        return this.version;
    }

    public void setVersion( long version ) { 
        this.version = version;
    }

    public void setDataSectionLength( long dataSectionLength ) {
        this.dataSectionLength = dataSectionLength;
    }

    /**
     * Number of bytes in the data section.  We can read ALL the raw data just by
     * reading from 0 to dataSectionLength bytes.
     */
    public int getDataSectionLength() {
        return (int)dataSectionLength;
    }

    public long getDataBlockFormat() {
        return dataBlockFormat;
    }

    public void setDataBlockFormat(long dataBlockFormat) {
        this.dataBlockFormat = dataBlockFormat;
    }

    public void read( ChannelBuffer buff ) {

        // duplicate the buffer so the global readerIndex isn't updated.
        buff = buff.duplicate();
        
        //seek to the trailer position
        int offset = buff.writerIndex() - LENGTH;

        buff.readerIndex( offset );
        
        StructReader sr = new StructReader( buff );

        setCompressionAlgorithm( sr.readLong() );
        setCount( (int)sr.readLong() );
        setRecordUsage( sr.readLong() );
        setIndexOffset( sr.readLong() );
        setIndexCount( sr.readLong() );
        setFileInfoOffset( sr.readLong() );
        setDataSectionLength( sr.readLong() );
        setDataBlockFormat( sr.readLong() );
        setVersion( sr.readLong() );

        int magic = sr.readInt();

        if ( magic != -1 )
            throw new RuntimeException();
        
    }
    
    public void write( ChannelBufferWritable writer ) throws IOException {

        StructWriter sw = new StructWriter( LENGTH );

        sw.writeLong( compressionAlgorithm );
        sw.writeLong( count );
        sw.writeLong( recordUsage );
        sw.writeLong( indexOffset );
        sw.writeLong( indexCount );
        sw.writeLong( fileInfoOffset );
        sw.writeLong( dataSectionLength );
        sw.writeLong( version );
        sw.writeLong( dataBlockFormat );
        sw.writeInt( MAGIC );
        
        writer.write( sw.getChannelBuffer() );
        
    }

    @Override
    public String toString() {
        
        return String.format( "version=%s, compressionAlgorithm=%s, count=%s, recordUsage=%s, indexOffset=%s, indexCount=%s, fileInfoOffset=%s", 
                              version,
                              compressionAlgorithm,
                              count,
                              recordUsage,
                              indexOffset,
                              indexCount,
                              fileInfoOffset );
        
    }
    
}