package peregrine.map;

import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.shuffle.sender.*;

public abstract class BaseMapper {

    public int partitions = 0;

    private JobOutput stdout = null;

    private List<BroadcastInput> broadcastInput = new ArrayList();

    public void init( List<JobOutput> output ) {

        if ( output.size() > 0 )
            this.stdout = output.get(0);

        if ( this.stdout instanceof BroadcastJobOutput ) {
            throw new RuntimeException( "Standard out may not be a broadcast reference: " + this.stdout );
        }
        
    }

    public final void emit( StructReader key,
                            StructReader value ) {

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

