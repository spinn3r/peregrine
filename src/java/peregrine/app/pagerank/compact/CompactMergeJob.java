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
package peregrine.app.pagerank.compact;

import java.util.*;
import peregrine.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * 
 */
public class CompactMergeJob {

    private static final Logger log = Logger.getLogger();

    public static class Merge extends Merger {

        @Override
        public void merge( StructReader key, List<StructReader> values ) {

            StructReader minor   = values.get( 0 );
            StructReader major   = values.get( 1 );

            StructReader value = minor;

            if ( major != null )
                value = major;

            // we have both major and minor outbound links so they need to be merged.
            if ( minor != null && major != null ) {

                Set<StructReader> set = new TreeSet();

                set.addAll( StructReaders.split( minor, Hashcode.HASH_WIDTH ) );
                set.addAll( StructReaders.split( major, Hashcode.HASH_WIDTH ) );

                value = StructReaders.join( set );
                
            }

            emit( key, value );
            
        }

    }

}
