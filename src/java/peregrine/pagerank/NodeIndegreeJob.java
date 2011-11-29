package peregrine.pagerank;

import java.util.*;
import peregrine.*;
import peregrine.values.*;

public class NodeIndegreeJob {

    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
                         StructReader value) {

            HashSetValue hashSetValue = new HashSetValue();
            hashSetValue.fromChannelBuffer( value.getChannelBuffer() );
            
            for( StructReader target : hashSetValue.getValues() ) {
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