package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;

public class ShuffleManager {

    public static ConcurrentHashMap<Partition,MapOutputIndex> bufferMap = null;

    public static void reset() {
        bufferMap = new ConcurrentHashMap();
    }
    
    public static MapOutputIndex getMapOutputIndex( Partition target_partition ) {

        MapOutputIndex buffer = bufferMap.get( target_partition );

        if ( buffer == null ) {
            
            buffer = new MapOutputIndex( target_partition );
            bufferMap.putIfAbsent( target_partition, buffer );
            buffer = bufferMap.get( target_partition );
            
        }
        
        return buffer;
        
    }

    public static Collection<MapOutputIndex> getMapOutput() {
        return bufferMap.values();
    }
    
}
