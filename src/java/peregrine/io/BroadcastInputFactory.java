package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class BroadcastInputFactory {

    public static List<BroadcastInput> getBroadcastInput( Input input, Partition part, Host host ) throws IOException {

        List<BroadcastInput> result = new ArrayList();
        
        for ( InputReference in : input.getReferences() ) {

            if ( in instanceof BroadcastInputReference ) {
                
                BroadcastInputReference bir = (BroadcastInputReference) in;
                
                BroadcastInput bi = new BroadcastInput( part, host, bir.getPath() );
                
                result.add( bi );
                
            }
            
        }

        return result;

    }

}