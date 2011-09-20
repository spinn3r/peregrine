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

    //FIXME: What is a reasonable buffer size? 
    List<Tuple> tuples = new ArrayList();
    
    private long chunk_id = 0;

    public MapOutputBuffer( long chunk_id ) {
        this.chunk_id = chunk_id;
    }
    
    public void accept( byte[] key,
                        byte[] value ) {

        tuples.add( new Tuple( key, value ) );
        
    }

    public void cleanup() {
        
    }
    
}
