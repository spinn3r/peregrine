package peregrine;

import java.util.*;
import peregrine.values.*;
import peregrine.io.*;
import peregrine.values.*;

public class Reducer {

    private JobOutput stdout = null;

    private List<BroadcastInput> broadcastInput = new ArrayList();
    
    public void init( JobOutput... output ) {
        this.stdout = output[0];
    }

    public void cleanup() {}
    
    public void reduce( StructReader key, List<StructReader> values ) {

        Struct struct = new Struct();

        for( StructReader val : values ) {
            struct.write( val );
        }

        emit( key, new StructReader( struct.toChannelBuffer() ) );

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
