package peregrine;

import java.util.*;
import peregrine.io.*;

/**
 * Take a key and list of values, and reduce them and emit result.
 */
public class Reducer {

    private JobOutput stdout = null;

    private List<BroadcastInput> broadcastInput = new ArrayList();
    
    public void init( List<JobOutput> output ) {
        this.stdout = output.get(0);
    }

    public void cleanup() {}
    
    public void reduce( StructReader key, List<StructReader> values ) {

        emit( key, StructReaders.wrap( values ) );

    }
        
    public void emit( StructReader key, StructReader value ) {
        stdout.emit( key, value );
    }

    public List<BroadcastInput> getBroadcastInput() { 
        return this.broadcastInput;
    }

    public void setBroadcastInput( List<BroadcastInput> broadcastInput ) { 
        this.broadcastInput = broadcastInput;
    }

}
