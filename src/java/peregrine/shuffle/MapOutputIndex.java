package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;

public class MapOutputIndex {

    public Map<Long,MapOutputBuffer> bufferMap
        = new Hashtable();
    
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

            // note that we must hide IOException from the Mapper interface.
            // There is no condition where the Mappers can recover from an
            // IOException.  While there may not be any other networked replicas
            // to talk to, we could still have a local failure which means we
            // have to stop working.  The controller will handle this failure by
            // re-routing a map request to another node.
            
            throw new RuntimeException( "Failed to process chunk.", e );
        }

    }
    
    private MapOutputBuffer getBuffer( long chunk_id ) throws IOException {

        MapOutputBuffer buffer = bufferMap.get( chunk_id );

        if ( buffer == null ) {

            synchronized( bufferMap ) {

                buffer = bufferMap.get( chunk_id );

                if ( buffer == null ) {
                    buffer = new MapOutputBuffer( chunk_id );
                    bufferMap.put( chunk_id, buffer );
                }

            }

        }
        
        return buffer;
        
    }

    public Collection<MapOutputBuffer> getMapOutput() {
        return bufferMap.values();
    }
    
}
