package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;

public class Reducer {

    private PartitionWriter writer = null;
    
    // tell the reducer which partition its running on as well as the host.

    public void init( Partition partition ) {

        try {

            writer = new PartitionWriter( partition, "/tmp/reduce.out" );

        } catch ( IOException e ) {
            throw new RuntimeException( "FIXME:" , e );
        }

    }
    
    public void reduce( byte[] key, List<byte[]> values ) { }
        
    public void emit( byte[] key, byte[] value ) {

        try {
            writer.write( key, value );
        } catch ( IOException e ) {
            throw new RuntimeException( "FIXME:" , e );
        }
        
    }

    public void cleanup() {

        try {
            
            writer.close();

        } catch ( IOException e ) {
            throw new RuntimeException( "FIXME:" , e );
        }

    }

}
