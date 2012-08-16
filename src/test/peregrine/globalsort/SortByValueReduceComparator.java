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
package peregrine.globalsort;

import java.io.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.chunk.*;

import peregrine.util.primitive.*;

/**
 * Compares KeyValuePairs by key.
 */
public class SortByValueReduceComparator extends DefaultReduceComparator {

    /**
     */
    public int compare( KeyValuePair pair0 , 
                        KeyValuePair pair1 ) {

        return super.compare( StructReaders.join( pair0.getValue(), pair0.getKey() ),
                              StructReaders.join( pair1.getValue(), pair1.getKey() ) );
        
    }

}
