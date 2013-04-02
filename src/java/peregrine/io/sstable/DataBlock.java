package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;

public class DataBlock extends BaseBlock {
    
    private byte[] firstKey = null;

    public DataBlock( byte[] firstKey ) {
        this.firstKey = firstKey;
    }

    public void setFirstKey( byte[] firstKey ) { 
        this.firstKey = firstKey;
    }

    public byte[] getFirstKey() { 
        return this.firstKey;
    }

    @Override
    public void write( MappedFileWriter writer ) throws IOException {
        super.write( writer );

        //write the first key
        writer.write( StructReaders.wrap( firstKey ).getChannelBuffer() );

    }
    
}