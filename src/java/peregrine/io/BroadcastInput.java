package peregrine.io;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.io.partition.*;

public final class BroadcastInput {

    byte[] value ;

    public BroadcastInput( Config config,
                           Partition part,
                           Host host,
                           String path ) throws IOException {
        
        LocalPartitionReader reader = new LocalPartitionReader( config, part, host, path );

        if ( reader.hasNext() == false )
            throw new IOException( "No broadcast file for: " + path );

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

