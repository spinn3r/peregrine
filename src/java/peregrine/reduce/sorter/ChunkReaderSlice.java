
package peregrine.reduce.sorter;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.reduce.merger.*;

import org.jboss.netty.buffer.*;

public class ChunkReaderSlice implements ChunkReader {

    private int size = 0;

    private int idx = 0;

    // approx length in bytes of this slice.
    protected int length = 0;
    
    private ChunkReader delegate;
    
    public ChunkReaderSlice( ChunkReader delegate , int size ) {
        this.delegate = delegate;
        this.size = size;
    }

    @Override
    public boolean hasNext() throws IOException {
        return idx < size;
    }

    @Override
    public byte[] key() throws IOException {
        byte[] result = delegate.key();

        if ( result == null )
            throw new NullPointerException( "FIXME: delegate returned null: " + delegate.getClass().getName() );
        
        length += result.length + IntBytes.LENGTH;
        return result;
    }

    @Override
    public byte[] value() throws IOException {

        // bump up pointer.
        ++idx;

        byte[] result = delegate.value();
        length += result.length + IntBytes.LENGTH;
        return result;

    }

    @Override
    public int size() throws IOException {
        return size;
    }

    @Override
    public void close() throws IOException {
        // noop
    }

}
