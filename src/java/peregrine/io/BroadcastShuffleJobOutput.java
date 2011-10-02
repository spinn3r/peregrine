package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.map.*;
import peregrine.io.*;
import peregrine.shuffle.*;

public class BroadcastShuffleJobOutput extends ShuffleJobOutput {

    public BroadcastShuffleJobOutput() {
        this( "default" );
    }
        
    public BroadcastShuffleJobOutput( String name ) {

        super( name );

        this.shuffler = Shuffler.getInstance( name );
        
    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();

        for ( Partition target : partitionMembership.keySet() ) {

            MapOutputIndex mapOutputIndex = shuffler.getMapOutputIndex( target );

            mapOutputIndex.accept( chunkRef.global, key, value );

        }

    }

}

