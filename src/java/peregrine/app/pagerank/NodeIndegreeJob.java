package peregrine.app.pagerank;

import java.util.*;
import peregrine.*;
import peregrine.util.*;

public class NodeIndegreeJob {

    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
                         StructReader value) {

            while( value.isReadable() ) {
                StructReader target = value.readSlice( Hashcode.HASH_WIDTH );
                emit( target, key );
            }
            
        }

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {
            
            int indegree = values.size();

            emit( key, new StructWriter()
            		       .writeInt( indegree )
            		       .toStructReader() );
            
        }
        
    }

}