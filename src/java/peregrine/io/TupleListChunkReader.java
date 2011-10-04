package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

public class TupleListChunkReader implements ChunkReader {

    private List<Tuple> list = null;

    private int idx = 0;
    
    public TupleListChunkReader( List<Tuple> list ) {
        this.list = list;
    }

    public boolean hasNext() throws IOException {

        return idx < list.size();

    }

    public byte[] key() throws IOException {
        return list.get( idx ).key;
    }

    public byte[] value() throws IOException {
        byte[] result = list.get( idx ).value;
        ++idx;
        return result;
    }

    public int size() throws IOException {
        return list.size();
    }

    public void close() throws IOException {
        // noop...
    }

}

