
package peregrine.reduce.merger;

import java.io.*;

import peregrine.io.chunk.*;
import peregrine.values.*;

public class MergeQueueEntry {
    
	public byte[] keyAsByteArray;
	
    public StructReader key;
    public StructReader value;
    
    protected MergerPriorityQueue queue = null;

    protected ChunkReader reader = null;
    
    public MergeQueueEntry( ChunkReader reader ) throws IOException {
    	this.reader = reader;
    	this.key = reader.key();
    	this.value = reader.value();
    	this.keyAsByteArray = key.toByteArray();
    
    }

}

