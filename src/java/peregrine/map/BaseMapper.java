package peregrine.map;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;

public abstract class BaseMapper {

    public int partitions = 0;

    private JobOutput stdout = null;

    private List<BroadcastInput> broadcastInput = new ArrayList();

    public void init( JobOutput... output ) {

        if ( output.length > 0 )
            this.stdout = output[0];

    }

    public final void emit( byte[] key,
                            byte[] value ) {

        if ( stdout == null )
            throw new RuntimeException( "stdout not defined." );
        
        stdout.emit( key, value );
        
    }

    public void cleanup() { }

    public List<BroadcastInput> getBroadcastInput() { 
        return this.broadcastInput;
    }

    public void setBroadcastInput( List<BroadcastInput> broadcastInput ) { 
        this.broadcastInput = broadcastInput;
    }

}

