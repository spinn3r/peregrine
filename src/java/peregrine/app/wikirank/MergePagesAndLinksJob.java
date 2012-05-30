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
package peregrine.app.wikirank;

import java.util.*;
import peregrine.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * Join the wikipedia pages which have an ID to their nodes, which have an ID.
 * This is a merge job because we need two imputs.
 */
public class MergePagesAndLinksJob {

    private static final Logger log = Logger.getLogger();

    public static class Merge extends Merger {

        @Override
        public void merge( StructReader key,
                           List<StructReader> values ) {

            StructReader node = values.get( 0 );

            if ( node != null ) { 
                
                String page = node.readString();

                key = StructReaders.hashcode( page );
                StructReader value = values.get( 1 );

                if ( value == null )
                    return;
                
                List<StructReader> outlinks = StructReaders.unwrap( value );

                StructWriter hashcodes = new StructWriter( outlinks.size() * Hashcode.HASH_WIDTH );

                for( StructReader out : outlinks ) {
                    hashcodes.writeHashcode( out.readString() );
                }
                
                if ( value != null ) {
                    emit( key , hashcodes.toStructReader() );
                }

            }

        }

    }

}