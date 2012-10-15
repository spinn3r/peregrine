/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.config;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Maintains an index of where keys are stored to analyze placement.
 */
public class PartitionRouteHistograph {

    private int total = 0;

    private Config config = null;

    private AtomicInteger[] data;
    
    public PartitionRouteHistograph( Config config ) {

        this.config = config;

        data = new AtomicInteger[ config.getMembership().getPartitions().size() ];
        
        for( int i = 0; i < data.length; ++i ) {
            data[i] = new AtomicInteger();
        }

    }

    public void incr( Partition part ) {

        ++total;
        data[ part.getId() ].getAndIncrement();
        
     }
    
    public String toString() {

        StringBuilder hist = new StringBuilder();

        for( Partition part : config.getMembership().getPartitions() ) {

            if ( hist.length() > 0 )
                hist.append( ", " );

            hist.append( String.format( "%s=%s", part.getId(), data[ part.getId() ] ) );
            
        }
        
        return String.format( "total: %,d: %s", total, hist.toString() );

    }

} 
