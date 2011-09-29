package maprunner.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

public class TupleArrayChunkReader implements ChunkReader {

    private Tuple[] data = null;

    private int idx = 0;
    
    public TupleArrayChunkReader( Tuple[] data ) {
        this.data = data;
    }

    public Tuple read() throws IOException {

        if ( data.length == idx )
            return null;
        
        return data[ idx++ ];
    }

    public int size() throws IOException {
        return data.length;
    }

    public void close() throws IOException {
        // noop...
    }

}

