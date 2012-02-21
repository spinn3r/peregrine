package peregrine.reduce.sorter;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.util.*;

/**
 * Read key/value pairs from a KeyLookup similar to a ChunkReader.
 */
public class KeyLookupReader {

	private KeyLookup lookup = null;
	
    private StructReader key = null;
    
    private StructReader value = null;
	
	public KeyLookupReader(KeyLookup lookup) {
		this.lookup = lookup;
	}

	public boolean hasNext() {
		return lookup.hasNext();
	}
	
	public void next() {

		lookup.next();
		
        KeyEntry current = lookup.get();

        ChannelBuffer backing = current.backing;
        
        VarintReader varintReader = new VarintReader( backing );
        
        int start = current.offset - 1;
        backing.readerIndex( start );

        key   = new StructReader( backing.readSlice( varintReader.read() ) );
        value = new StructReader( backing.readSlice( varintReader.read() ) );
		
	}
	
	public StructReader key() {
		return key;
	}
	
	public StructReader value() {
		return value;
	}
	
}
