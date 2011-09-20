package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class MapOutputIndex {

    public static ConcurrentHashMap<Long,MapOutputBuffer> bufferMap
        = new ConcurrentHashMap();

    protected int partition = -1;
    
    public MapOutputIndex( int partition ) {
        this.partition = partition;
    }
    
    public static void accept( long chunk_id, 
                               byte[] key,
                               byte[] value ) {

        MapOutputBuffer buffer = getBuffer( chunk_id );

        buffer.accept( key, value );

    }

    private static MapOutputBuffer getBuffer( long chunk_id ) {

        MapOutputBuffer buffer = bufferMap.get( chunk_id );

        if ( buffer == null ) {
            
            buffer = new MapOutputBuffer( chunk_id );
            bufferMap.putIfAbsent( chunk_id, buffer );
            buffer = bufferMap.get( chunk_id );
            
        }
        
        return buffer;
        
    }

    public Collection<MapOutputBuffer> getMapOutput() {
        return bufferMap.values();
    }
    
}
