
package maprunner.shuffle;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.map.*;

public class MapOutputMergeCallable implements Callable {

    private MapOutputIndex mapOutputIndex = null;
    
    public MapOutputMergeCallable( MapOutputIndex mapOutputIndex ) {
        this.mapOutputIndex = mapOutputIndex;
    }

    public Object call() throws Exception {

        //FIXME: this implements the DEFAULT sort everything approach not the
        //hinted pre-sorted approach.
        
        Collection<MapOutputBuffer> mapOutput = mapOutputIndex.getMapOutput();

        int size = 0;
        
        for ( MapOutputBuffer mapOutputBuffer : mapOutput ) {
            size += mapOutputBuffer.size();
        }

        Tuple[] data = new Tuple[size];

        int offset = 0;
        
        for ( MapOutputBuffer mapOutputBuffer : mapOutput ) {

            Tuple[] copy = mapOutputBuffer.toArray();

            System.arraycopy( copy, 0, data, offset, copy.length );

            offset += copy.length;
            
        }

        System.out.printf( "Sorting %,d entries\n", data.length );
        
        Arrays.sort( data );
        
        return null;

    }
    
}