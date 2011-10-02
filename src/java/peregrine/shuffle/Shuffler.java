package peregrine.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;

public class Shuffler {

    public ConcurrentHashMap<Partition,MapOutputIndex> bufferMap = null;

    public Shuffler() {
        reset();
    }

    public void reset() {
        bufferMap = new ConcurrentHashMap();
    }
    
    public MapOutputIndex getMapOutputIndex( Partition target_partition ) {

        MapOutputIndex buffer = bufferMap.get( target_partition );

        if ( buffer == null ) {
            
            buffer = new MapOutputIndex( target_partition );
            bufferMap.putIfAbsent( target_partition, buffer );
            buffer = bufferMap.get( target_partition );
            
        }
        
        return buffer;
        
    }

    public Collection<MapOutputIndex> getMapOutput() {
        return bufferMap.values();
    }

    public static ConcurrentHashMap<String,Shuffler> shufflers = new ConcurrentHashMap();

    public static Shuffler getInstance() {
        return getInstance( "default" );
    }

    public static Shuffler getInstance( String name ) {

        Shuffler instance = shufflers.get( name );

        if ( instance == null ) {
            instance = new Shuffler();
            shufflers.put( name, instance );
        }

        return instance;

    }

}
