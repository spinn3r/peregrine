package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class ShuffleManager {

    public static ConcurrentHashMap<Partition,MapOutputIndex> bufferMap
        = new ConcurrentHashMap();

    public static MapOutputIndex getMapOutputIndex( Partition partition ) {

        MapOutputIndex buffer = bufferMap.get( partition );

        if ( buffer == null ) {
            
            buffer = new MapOutputIndex( partition );
            bufferMap.putIfAbsent( partition, buffer );
            buffer = bufferMap.get( partition );
            
        }
        
        return buffer;
        
    }

    public static Collection<MapOutputIndex> getMapOutput() {
        return bufferMap.values();
    }
    
}
