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
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.chunk.*;
import peregrine.util.primitive.*;

import com.spinn3r.log5j.*;

/**
 * 
 */
public class DefaultSortComparator implements SortComparator, Comparator<StructReader> {

    private static final Logger log = Logger.getLogger();

    private FastStructReaderComparator delegate = new FastStructReaderComparator();
    
    /**
     */
    @Override
    public int compare( KeyValuePair pair0 , KeyValuePair pair1 ) {

        return delegate.compare( getSortKey( pair0.getKey(), pair0.getValue() ),
                                 getSortKey( pair1.getKey(), pair1.getValue() ) );
        
    }

    public int compare( StructReader sr0 , StructReader sr1 ) {
        return delegate.compare( sr0, sr1 );
    }


    @Override
    public StructReader getSortKey( StructReader key, StructReader value ) {
        return key.slice();
    }
    
}
