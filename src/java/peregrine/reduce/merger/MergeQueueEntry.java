
package peregrine.reduce.merger;

import java.io.*;

import peregrine.io.chunk.*;
import peregrine.values.*;

public class MergeQueueEntry {
    
	public byte[] keyAsByteArray;
    public StructReader key;
    public StructReader value;

    protected ChunkReader reader = null;

    protected MergeQueueEntry() {}

    public MergeQueueEntry( ChunkReader reader ) throws IOException {

        this( reader.key(), reader.value() );
        
        this.reader = reader;

    }

    public MergeQueueEntry( StructReader key, StructReader value ) {
        setKey( key );
        setValue( value );
    }

    public void setKey( StructReader key ) {
    	this.keyAsByteArray = key.toByteArray();
    	this.key = key;

    }

    public void setValue( StructReader value ) {
        this.value = value;
    }

    public MergeQueueEntry copy() {

        MergeQueueEntry copy = new MergeQueueEntry();

        copy.keyAsByteArray = keyAsByteArray;
        copy.key = key;
        copy.value = value;
        copy.reader = reader;
        
        return copy;
        
    }
    
}

