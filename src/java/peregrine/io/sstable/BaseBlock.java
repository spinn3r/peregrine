package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;

import org.jboss.netty.buffer.*;

public abstract class BaseBlock {

    protected long length = -1;
    
    protected long offset = -1;

    protected long count = 0;


    public long getLength() { 
        return this.length;
    }

    public void setLength( long length ) { 
        this.length = length;
    }

    public long getOffset() { 
        return this.offset;
    }

    public void setOffset( long offset ) { 
        this.offset = offset;
    }

    public long getCount() { 
        return this.count;
    }

    public void setCount( long count ) { 
        this.count = count;
    }

    public void read( ChannelBuffer buff ) throws IOException {

        StructReader sr = new StructReader( buff );

        length = sr.readLong();
        offset = sr.readLong();
        count = sr.readLong();
        
    }

    public void write( MappedFileWriter writer ) throws IOException {

        StructWriter sw = new StructWriter( 100 );
        sw.writeLong( length );
        sw.writeLong( offset );
        sw.writeLong( count );

        writer.write( sw.getChannelBuffer() );

    }

    @Override
    public String toString() {
        return String.format( "length=%s, offset=%s, count=%s", length, offset, count );
    }

}