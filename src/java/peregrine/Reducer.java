package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.io.*;

public class Reducer {
    
    private Output output = null;

    private ReducerOutput stdout = null;

    public void init( ReducerOutput... output ) {
        this.stdout = output[0];
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

    public void setOutput( Output output ) { 
        this.output = output;
    }

    public Output getOutput() { 
        return this.output;
    }

}
