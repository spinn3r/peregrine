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
package peregrine.app.flow;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.io.*;

import com.spinn3r.log5j.Logger;

/**
 * Terminate the job by joining against the graph and writing all graph data,
 * then sorting it in the reducer.
 */
public class FlowTermJob {

    private static final Logger log = Logger.getLogger();

    public static class Merge extends Merger {
        
        @Override
        public void merge( StructReader key, List<StructReader> values ) {

        	StructReader left   = values.get( 0 );
        	StructReader right  = values.get( 1 );

            if ( left == null || right == null )
                return;

            emit( key, StructReaders.join( right ) );

        }

    }

}
