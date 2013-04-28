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
    public static final int LENGTH = 7 * Longs.LENGTH ;

    public static final long VERSION_OFFSET = 200000;
    
    // the version number (and magic number) for the file.  All versions should
    // start off with a specific offset so that we can use this portion as the
    // magic number.  This still gives us plenty of versions to play with since
    // we have 2^64 and doesn't require another magic field.
    public long version = VERSION_OFFSET + 0;

    public long compressionAlgorithm = 0;
    
    public long count = 0;

    public long recordUsage = 0;

    public long indexCount = 0;
    
    public long indexOffset = -1;
    
    public long fileInfoOffset = -1;

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
    public long getCount() { 
        return this.count;
    }

    public void setCount( long count ) { 

        if ( count < 0 )
            throw new IllegalArgumentException( "count" );
        
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

    public void read( ChannelBuffer buff ) {

        // duplicate the buffer so the global readerIndex isn't updated.
        buff = buff.duplicate();
        
        //seek to the trailer position
        int offset = buff.writerIndex() - LENGTH;

        buff.readerIndex( offset );
        
        StructReader sr = new StructReader( buff );

        setCompressionAlgorithm( sr.readLong() );
        setCount( sr.readLong() );
        setRecordUsage( sr.readLong() );
        setIndexOffset( sr.readLong() );
        setIndexCount( sr.readLong() );
        setFileInfoOffset( sr.readLong() );
        setVersion( sr.readLong() );
        
    }
    
    public void write( ChannelBufferWritable writer ) throws IOException {

        StructWriter sw = new StructWriter( 100 );

        sw.writeLong( compressionAlgorithm );
        sw.writeLong( count );
        sw.writeLong( recordUsage );
        sw.writeLong( indexOffset );
        sw.writeLong( indexCount );
        sw.writeLong( fileInfoOffset );
        sw.writeLong( version );

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