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

    public BroadcastShuffleJobOutput( Config config ) {
        this( config, "default" );
    }
        
    public BroadcastShuffleJobOutput( Config config, String name ) {

        super( config, name );

    }
    
    @Override
    public void emit( byte[] key , byte[] value ) {

        Membership membership = config.getMembership();

        for ( Partition target : membership.getPartitions() ) {
            emit( target.getId() , key, value );
        }

    }

    @Override
    public String toString() {
        return String.format( "%s:%s@%s", getClass().getName(), name, Integer.toHexString(hashCode()) );
    }
    
}

