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
package peregrine;

import java.util.*;

import peregrine.io.*;
import peregrine.task.*;
import peregrine.combine.*;

/**
 * <p>
 * Take a key and list of values, and reduce/combine them and emit result.
 * 
 * <p>
 * Combiners are used to boost the performance of shuffling by reducing 
 * values before they are sent over the wire.
 * 
 * <p>
 * This is VERY similar to a Reducer but does not support multiple output 
 * streams.
 */
public class Combiner extends BaseCombiner {

    public void combine( StructReader key, List<StructReader> values ) {
        emit( key, StructReaders.wrap( values ) );
    }

}
