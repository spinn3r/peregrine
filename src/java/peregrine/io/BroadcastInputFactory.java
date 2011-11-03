package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class BroadcastInputFactory {

    public static List<BroadcastInput> getBroadcastInput( Config config,
                                                          Input input,
                                                          Partition part ) throws IOException {

        List<BroadcastInput> result = new ArrayList();
        
        for ( InputReference in : input.getReferences() ) {

            if ( in instanceof BroadcastInputReference ) {
                
                BroadcastInputReference bir = (BroadcastInputReference) in;
                
                BroadcastInput bi = new BroadcastInput( config, part, bir.getName() );
                
                result.add( bi );
                
            }
            
        }

        return result;

    }

}