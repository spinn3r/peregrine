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
import peregrine.io.*;

import com.spinn3r.log5j.*;

/**
 * Merge the node names, node metadata, and node rank information.
 *
 * The resulting table will have a key which is the hashcode of the node, then
 * the node name, rank, indegree, and outdegree
 * 
 */
public class CreateNodeLookupJob {

    private static final Logger log = Logger.getLogger();

    public static class Map extends Mapper {

        JobOutput nodesByPrimaryKeyOutput         = null;
        JobOutput nodesByHashcodeOutput         = null;
        
        @Override
        public void init( List<JobOutput> output ) {
            nodesByPrimaryKeyOutput           = output.get(0);
            nodesByHashcodeOutput             = output.get(1);
        }

        @Override
        public void map( StructReader key,
                         StructReader value ) {

            nodesByPrimaryKeyOutput.emit( key, value );

            String name = value.readString();
            
            nodesByHashcodeOutput.emit( StructReaders.hashcode( name ),
                                        StructReaders.wrap( name ) );
            
        }

    }

}