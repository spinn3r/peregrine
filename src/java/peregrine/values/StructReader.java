package peregrine.values;

import java.io.*;

import org.jboss.netty.buffer.*;

import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * 
 */
public class StructReader {

    private VarintReader varintReader;
    private ChannelBuffer buff;
    
    public StructReader( byte[] data ) {
        this( ChannelBuffers.wrappedBuffer( data ) );
    }

    public StructReader( ChannelBuffer buff ) {
    	this.buff = buff;
        this.varintReader = new VarintReader( buff );
    }

    public int readVarint() {
        return varintReader.read();
    }

    public double readDouble() {
        return buff.readDouble();
    }

    public int readInt() {
        return buff.readInt();
    }

    public byte[] read( byte[] data ) {
        buff.readBytes( data );
        return data;
    }
    
    public byte[] readHashcode() {
        return read(new byte[Hashcode.HASH_WIDTH]);
    }

}

