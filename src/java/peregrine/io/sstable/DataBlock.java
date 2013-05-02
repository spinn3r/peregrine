package peregrine.io.sstable;

import java.io.*;

import peregrine.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;

import org.jboss.netty.buffer.*;

public class DataBlock extends BaseBlock {
    
    public byte[] firstKey = null;

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
    public void read( ChannelBuffer buff ) {

        super.read( buff );

        StructReader sr = new StructReader( buff );

        firstKey = sr.readBytes();
        
    }

    @Override
    public void write( ChannelBufferWritable writer ) throws IOException {

        super.write( writer );

        StructWriter sw = new StructWriter( firstKey.length + 4 );
        sw.writeBytes( firstKey );
        
        //write the first key
        writer.write( sw.getChannelBuffer() );

    }

    @Override
    public String toString() {
        return String.format( "%s, firstKey=%s", super.toString(), Hex.encode( firstKey ) );
    }

}