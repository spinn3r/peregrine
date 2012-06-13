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

/**
 */
public class FlattenLinksJob {

    /**
     * Identify mapper so that we emit the key , value that we are working on so
     * that we can shuffle and sort it. The default entity mapper is just fine.
     */
    public static class Map extends Mapper {

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            //FIXME: emit( key, StructReaders.wrap( values ) );
            
        }
        
    }

}