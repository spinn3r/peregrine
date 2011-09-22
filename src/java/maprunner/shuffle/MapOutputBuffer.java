package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class MapOutputBuffer {

    //TODO: implementing our own ArrayList which has expand capabilities but
    //which allows you to access the underlying array directly would be nice.
    private List<Tuple> tuples = new ArrayList(); 
    
    private long chunk_id = 0;

    public MapOutputBuffer( long chunk_id ) {
        this.chunk_id = chunk_id;
    }
    
    public void accept( byte[] key,
                        byte[] value ) {

        //TODO: I wonder if it would be faster to append to two arrays or to
        //store these in one byte array.
        tuples.add( new Tuple( key, value ) );
        
    }

    public int size() {
        return tuples.size();
    }

    public Tuple[] toArray() {

        Tuple[] result = new Tuple[ tuples.size() ];
        
        return tuples.toArray( result );
    }
    
}
