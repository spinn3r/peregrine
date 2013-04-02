package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;

import org.jboss.netty.buffer.*;

public class Trailer {

    protected static int LENGTH = 7 * Longs.LENGTH ;
    
    protected long version = 1;

    protected long compressionAlgorithm = 0;
    
    protected long count = 0;
    
    protected long size = 0;
    
    protected long indexCount = 0;
    
    protected long indexOffset = -1;
    
    protected long fileInfoOffset = -1;

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

    public long getIndexCount() { 
        return this.indexCount;
    }

    public void setIndexCount( long indexCount ) { 
        this.indexCount = indexCount;
    }

    /**
     * Size, in bytes, of the length of keys + values as raw byte.
     */
    public long getSize() { 
        return this.size;
    }

    public void setSize( long size ) { 
        this.size = size;
    }

    public long getCount() { 
        return this.count;
    }

    public void setCount( long count ) { 
        this.count = count;
    }

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

    public void read( ChannelBuffer buff ) throws IOException {

        //TODO: seek to the trailer position

        int offset = buff.writerIndex() - LENGTH;

        System.out.printf( "FIXE: %s\n", offset );
        
        buff.readerIndex( offset );
        
        StructReader sr = new StructReader( buff );

        version = sr.readLong();
        compressionAlgorithm = sr.readLong();
        count = sr.readLong();
        size = sr.readLong();
        indexOffset = sr.readLong();
        indexCount = sr.readLong();
        fileInfoOffset = sr.readLong();

    }
    
    public void write( MappedFileWriter writer ) throws IOException {

        StructWriter sw = new StructWriter( 100 );

        sw.writeLong( version );
        sw.writeLong( compressionAlgorithm );
        sw.writeLong( count );
        sw.writeLong( size );
        sw.writeLong( indexOffset );
        sw.writeLong( indexCount );
        sw.writeLong( fileInfoOffset );

        writer.write( sw.getChannelBuffer() );
        
    }

    @Override
    public String toString() {
        
        return String.format( "version=%s, compressionAlgorithm=%s, count=%s, size=%s, indexOffset=%s, indexCount=%s, fileInfoOffset=%s", 
                              version,
                              compressionAlgorithm,
                              count,
                              size,
                              indexOffset,
                              indexCount,
                              fileInfoOffset );
        
    }
    
}