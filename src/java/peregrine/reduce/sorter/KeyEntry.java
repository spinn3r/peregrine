package peregrine.reduce.sorter;

import org.jboss.netty.buffer.*;

import peregrine.util.primitive.*;

public class KeyEntry {

	public byte buffer;
	public int offset;
	
	public ChannelBuffer backing; 
	
	public KeyEntry( byte buffer, int offset ) {
		this.buffer = buffer;
		this.offset = offset;
	}
	
    public byte[] read() {
        byte[] data = new byte[LongBytes.LENGTH];
        backing.getBytes( offset, data );
        return data;
    }

    /**
     * Read a byte from the entry at the given position within the key. 
     * 
     */
    public byte read( int pos ) {    	
        return backing.getByte( offset + pos );
    }
    
}
