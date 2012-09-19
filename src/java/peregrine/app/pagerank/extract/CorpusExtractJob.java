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
package peregrine.app.pagerank;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.util.primitive.IntBytes;
import peregrine.io.*;

/**
 * A job which takes an input file, parses the splits, then emits them.
 */
public class CorpusExtractJob {

    public static class Map extends Mapper {

        @Override
        public void init( Job job, List<JobOutput> output ) {

            String path = job.getParameters().getString( "path" );

            
            
        }

        @Override
        public void map( StructReader key, StructReader value ) {
            //noop for now.
        }

    }

    public static class Reduce extends Reducer {

    }

}
