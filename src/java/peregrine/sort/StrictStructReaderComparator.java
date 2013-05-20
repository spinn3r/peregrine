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
package peregrine.sort;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.chunk.*;

import com.spinn3r.log5j.*;

/**
 * Comparator for StructReaders that maintains exactly byte order comparison
 * but is slightly slower.
 */
public class StrictStructReaderComparator implements Comparator<StructReader> {

    private static final Logger log = Logger.getLogger();

    @Override
    public int compare( StructReader sr0, StructReader sr1 ) {

        int diff = 0;

        int len = Math.min(sr0.length(), sr1.length());

        for( int offset = 0; offset < len; ++offset ) {

            //this will be SLOW for normal key comparison (so don't use it
            //when we are in that mode).

            diff = (sr0.getByte( offset ) & 0xFF) - (sr1.getByte( offset ) & 0xFF);

            if ( diff != 0 )
                return diff;
            
        }

        return sr0.length() - sr1.length();
        
    }
    
}
