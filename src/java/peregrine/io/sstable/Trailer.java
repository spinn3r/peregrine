package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;

public class Trailer {
    
    protected int version = 1;

    protected byte[] compressionAlgorithm = "none".getBytes();
    
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

    public byte[] getCompressionAlgorithm() { 
        return this.compressionAlgorithm;
    }

    public void setCompressionAlgorithm( byte[] compressionAlgorithm ) { 
        this.compressionAlgorithm = compressionAlgorithm;
    }

    public int getVersion() { 
        return this.version;
    }

    public void setVersion( int version ) { 
        this.version = version;
    }

    public void write( MappedFileWriter writer ) throws IOException {

        StructWriter sw = new StructWriter( 100 );

        sw.writeInt( version );
        sw.writeBytes( compressionAlgorithm );
        sw.writeLong( count );
        sw.writeLong( size );
        sw.writeLong( indexOffset );
        sw.writeLong( indexCount );
        sw.writeLong( fileInfoOffset );

        writer.write( sw.getChannelBuffer() );
        
    }
    
}