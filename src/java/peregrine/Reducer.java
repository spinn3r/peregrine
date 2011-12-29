package peregrine;

import java.util.*;
import peregrine.values.*;
import peregrine.io.*;
import peregrine.values.*;

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

        if( values.size() == 1 ) {

            emit( key, values.get( 0 ) );

        } else if ( values.size() > 1 ) {
            
            Struct struct = new Struct();
            
            for( StructReader val : values ) {
                struct.write( val );
            }
            
            emit( key, new StructReader( struct.toChannelBuffer() ) );

        }

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
