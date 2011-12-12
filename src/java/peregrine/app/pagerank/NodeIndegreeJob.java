package peregrine.pagerank;

import java.util.*;
import peregrine.*;
import peregrine.values.*;

public class NodeIndegreeJob {

    public static class Map extends Mapper {

        @Override
        public void map( byte[] key_data,
                         byte[] value_data ) {

            HashSetValue value = new HashSetValue();
            value.fromBytes( value_data );
            
            byte[] source = key_data;

            for( byte[] target : value.getValues() ) {
                emit( target, source );
            }
            
        }

    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( byte[] key, List<byte[]> values ) {
            
            int indegree = values.size();

            emit( key, new IntValue( indegree ).toBytes() );
            
        }
        
    }

}