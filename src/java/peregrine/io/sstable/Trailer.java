package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;

import org.jboss.netty.buffer.*;

import peregrine.util.netty.*;

/**
 * Trailer on a file denoteing metadata about that file.
 */
public class Trailer {

    //length in bytes of the trailer.  This is a fixed width data structure.
    public static final int LENGTH = (8 * Longs.LENGTH) + Integers.LENGTH;

    //magic number for the tail of the chunk stream.
    public static final int MAGIC = -1;
    
    public static final long VERSION_OFFSET = 200000;

    public long compressionAlgorithm = 0;
    
    private long count = 0;

    public long recordUsage = 0;

    public long indexCount = 0;
    
    public long indexOffset = -1;
    
    public long fileInfoOffset = -1;

    // the version number (and magic number) for the file.  All versions should
    // start off with a specific offset so that we can use this portion as the
    // magic number.  This still gives us plenty of versions to play with since
    // we have 2^64 and doesn't require another magic field.
    public long version = VERSION_OFFSET + 0;

    // number of bytes in the data section.  We can read ALL the data just by
    // reading from 0 to dataSectionLength bytes.
    public long dataSectionLength = 0;
    
    public void setFileInfoOffset( long fileInfoOffset ) { 
        this.fileInfoOffset = fileInfoOffset;
    }

    public long getFileInfoOffset() { 
        return this.fileInfoOffset;
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

    public long getVersion() { 
        return this.version;
    }

    public void setVersion( long version ) { 
        this.version = version;
    }

    public void setDataSectionLength( long dataSectionLength ) {
        this.dataSectionLength = dataSectionLength;
    }
    
    public long getDataSectionLength() {
        return dataSectionLength;
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