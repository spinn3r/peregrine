package peregrine.app.pagerank;

import java.util.*;
import peregrine.*;
import peregrine.util.*;

public class NodeIndegreeJob {

    public static class Map extends Mapper {

        @Override
        public void map( StructReader key,
                         StructReader value) {

            List<StructReader> targets = StructReaders.split( value, Hashcode.HASH_WIDTH );
            
            for( StructReader target : targets ) {
                emit( target, StructReaders.TRUE );
            }
            
        }

    }

    public static class Combine extends Reducer {

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            emit( key, StructReaders.wrap( values.size() ) );

        }
        
    }

    public static class Reduce extends Reducer {

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            int sum = 0;
            
            for ( StructReader value : values ) {
                sum += value.readInt();
            }
            
            emit( key, StructReaders.wrap( sum ) );
            
        }
        
    }

}