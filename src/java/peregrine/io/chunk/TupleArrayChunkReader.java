package peregrine.io.chunk;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

public class TupleArrayChunkReader implements ChunkReader {

    private Tuple[] data = null;

    private int idx = 0;
    
    public TupleArrayChunkReader( Tuple[] data ) {
        this.data = data;
    }

    public boolean hasNext() throws IOException {
        return idx < data.length;
    }

    public byte[] key() throws IOException {
        return data[idx].key;
    }

    public byte[] value() throws IOException {
        byte[] result = data[idx].value;
        ++idx;
        return result;
    }

    public int size() throws IOException {
        return data.length;
    }

    public void close() throws IOException {
        // noop...
    }

}

