package maprunner.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

public class TupleListChunkReader implements ChunkReader {

    private List<Tuple> list = null;

    private int idx = 0;
    
    public TupleListChunkReader( List<Tuple> list ) {
        this.list = list;
    }

    public Tuple read() throws IOException {

        if ( list.size() == idx )
            return null;
        
        return list.get( idx++ );
    }

    public int size() throws IOException {
        return list.size();
    }

    public void close() throws IOException {
        // noop...
    }

}

