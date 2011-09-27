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

    public ConcurrentHashMap<Long,MapOutputBuffer> bufferMap
        = new ConcurrentHashMap();

    public Partition partition;
    
    public MapOutputIndex( Partition partition ) {
        this.partition = partition;
    }
    
    public void accept( long chunk_id, 
                        byte[] key,
                        byte[] value ) {

        try {
            MapOutputBuffer buffer = getBuffer( chunk_id );
            
            buffer.accept( key, value );
        } catch ( IOException e ) {
            throw new RuntimeException( "FIXME: while I just try to get this shit working" );
        }

    }
    
    private MapOutputBuffer getBuffer( long chunk_id ) throws IOException {

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
