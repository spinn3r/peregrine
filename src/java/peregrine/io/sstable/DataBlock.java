package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;

import org.jboss.netty.buffer.*;

public class DataBlock extends BaseBlock {
    
    private byte[] firstKey = null;

    public DataBlock() { }

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
    public void read( ChannelBuffer buff ) throws IOException {

        super.read( buff );

        StructReader sr = new StructReader( buff );

        firstKey = sr.readBytes();
        
    }
        
    @Override
    public void write( MappedFileWriter writer ) throws IOException {
        super.write( writer );

        //write the first key
        writer.write( StructReaders.wrap( firstKey ).getChannelBuffer() );

    }

    @Override
    public String toString() {
        return String.format( "%s, firstKey=%s", super.toString(), Hex.encode( firstKey ) );
    }

}