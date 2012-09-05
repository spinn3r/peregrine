/*
 * Copyright 2011-2012 Kevin A. Burton
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
package peregrine.sort;

import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import peregrine.util.primitive.*;

import com.spinn3r.log5j.*;

/**
 * Compares KeyValuePairs by key.
 */
public class DefaultSortComparator implements SortComparator {

    private static final Logger log = Logger.getLogger();

    /**
     */
    @Override
    public int compare( KeyValuePair pair0 , KeyValuePair pair1 ) {

        return compare( getSortKey( pair0.getKey(), pair0.getValue() ),
                        getSortKey( pair1.getKey(), pair1.getValue() ) );
        
    }

    protected int compare( StructReader sr0, StructReader sr1 ) {

        int diff = 0;

        //TODO: right now we assume that the 
        int len = sr0.length();

        //TODO is it faster to make these byte arrays in one method call or call
        //getByte() for each byte?
        for( int offset = 0; offset < len; ++offset ) {

            diff = sr0.getByte( offset ) - sr1.getByte( offset );

            if ( diff != 0 )
                return diff;
            
        }

        return diff;

    }

    public StructReader getSortKey( StructReader key, StructReader value ) {
        return key.slice();
    }
    
}
