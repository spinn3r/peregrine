package peregrine.io;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.partition.*;

public final class BroadcastInput {

    byte[] value ;

    public BroadcastInput( Config config,
                           Partition part,
                           String path ) throws IOException {
        
        LocalPartitionReader reader = new LocalPartitionReader( config, part, path );

        if ( reader.hasNext() == false )
            throw new IOException( "No broadcast values found at: " + reader );

        byte[] key   = reader.key();
        byte[] value = reader.value();

        if ( reader.hasNext() )
            throw new IOException( "Too many broadcast values for: " + path );

        this.value = value;

    }
    
    public BroadcastInput( byte[] value ) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }
    
}

