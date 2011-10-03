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

    public Map<Partition,MapOutputIndex> bufferMap = null;

    public Shuffler() {
        reset();
    }

    public void reset() {
        bufferMap = new Hashtable();
    }
    
    public MapOutputIndex getMapOutputIndex( Partition target_partition ) {

        MapOutputIndex buffer = bufferMap.get( target_partition );

        if ( buffer == null ) {

            synchronized( bufferMap ) {

                buffer = bufferMap.get( target_partition );

                if ( buffer == null ) {
                    buffer = new MapOutputIndex( target_partition );
                    bufferMap.put( target_partition, buffer );
                }

            }

        }
        
        return buffer;
        
    }

    public Collection<MapOutputIndex> getMapOutput() {
        return bufferMap.values();
    }

    public static Map<String,Shuffler> shufflers = new Hashtable();

    public static Shuffler getInstance() {
        return getInstance( "default" );
    }

    public static Shuffler getInstance( String name ) {

        Shuffler instance = shufflers.get( name );

        if ( instance == null ) {

            synchronized( shufflers ) {

                instance = shufflers.get( name );

                if ( instance == null ) {
                    instance = new Shuffler();
                    shufflers.put( name, instance );
                }
                
            }

        }

        return instance;

    }

}
