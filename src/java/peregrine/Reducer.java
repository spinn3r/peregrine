package peregrine;

import java.util.*;
import peregrine.values.*;
import peregrine.io.*;

public class Reducer {

    private JobOutput stdout = null;

    private List<BroadcastInput> broadcastInput = new ArrayList();
    
    public void init( List<JobOutput> output ) {
        this.stdout = output.get(0);
    }

    public void cleanup() {}
    
    public void reduce( byte[] key, List<byte[]> values ) {

        Struct struct = new Struct();

        for( byte[] val : values ) {
            struct.write( val );
        }

        emit( key, struct.toBytes() );

    }
        
    public void emit( byte[] key, byte[] value ) {
        stdout.emit( key, value );
    }

    public List<BroadcastInput> getBroadcastInput() { 
        return this.broadcastInput;
    }

    public void setBroadcastInput( List<BroadcastInput> broadcastInput ) { 
        this.broadcastInput = broadcastInput;
    }

}
