package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;

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

    public void write( MappedFileWriter writer ) throws IOException {

        StructWriter sw = new StructWriter( 100 );
        sw.writeLong( length );
        sw.writeLong( offset );
        sw.writeLong( count );

        writer.write( sw.getChannelBuffer() );

    }

}