/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.driver.broadcast.*;

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
