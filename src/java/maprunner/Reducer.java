package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;
import maprunner.io.*;

public class Reducer {
    
    private Output output = null;

    private PartitionWriter writer = null;
    
    // tell the reducer which partition its running on as well as the host.

    public void init( Partition partition, String path ) throws IOException {
        
        writer = new PartitionWriter( partition, path );

    }
    
    public void reduce( byte[] key, List<byte[]> values ) throws Exception {

        // FIXME: I am not sure that get(0) is the right approach
        emit( key, values.get( 0 ) );

    }
        
    public void emit( byte[] key, byte[] value ) throws Exception {

        writer.write( key, value );
        
    }

    public void cleanup() throws Exception {
        writer.close();
    }

    public void setOutput( Output output ) { 
        this.output = output;
    }

    public Output getOutput() { 
        return this.output;
    }

}
