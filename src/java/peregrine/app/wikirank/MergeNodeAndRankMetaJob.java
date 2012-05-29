package peregrine.app.pagerank;

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