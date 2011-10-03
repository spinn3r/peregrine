package peregrine.io;

import java.util.*;
import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class BroadcastInput {

    byte[] value ;

    public BroadcastInput( Partition part, Host host, String path ) throws IOException {
        
        LocalPartitionReader reader = new LocalPartitionReader( part, host, path );

        Tuple t = reader.read();

        if ( t == null )
            throw new IOException( "No broadcast file for: " + path );

        if ( reader.read() != null )
            throw new IOException( "Too many broadcast values for: " + path );

        this.value = t.value;

    }
    
    public BroadcastInput( byte[] value ) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }
    
}

