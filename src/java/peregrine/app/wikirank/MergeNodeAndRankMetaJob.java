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
 * Merge the node names, node metadata, and node rank information.
 *
 * The resulting table will have a key which is the hashcode of the node, then
 * the node name, rank, indegree, and outdegree
 * 
 */
public class MergeNodeAndRankMetaJob {

    private static final Logger log = Logger.getLogger();

    public static class Merge extends Merger {

        @Override
        public void merge( StructReader key,
                           List<StructReader> values ) {

            StructReader node_metadata   = values.get( 0 );
            StructReader rank_vector     = values.get( 1 );
            StructReader nodesByHashcode = values.get( 2 );

            double rank = 0.0; // default rank

            int indegree  = 0;
            int outdegree = 0;

            String name = "";
            
            if ( node_metadata != null ) {
                indegree  = node_metadata.readInt();
                outdegree = node_metadata.readInt();
            }
            
            if ( rank_vector != null ) {
                rank = rank_vector.readDouble();
            }

            if ( nodesByHashcode != null ) {
                name = nodesByHashcode.readString();
            }

            //TODO: we need a way to statically allocate memory for
            //StructWriters for variable length aggregate fields.
            
            StructWriter writer = new StructWriter( 256 );
            
            writer.writeInt( indegree );
            writer.writeInt( outdegree );
            writer.writeDouble( rank );
            writer.writeString( name );

            emit( key, writer.toStructReader() );
            
        }

    }

}