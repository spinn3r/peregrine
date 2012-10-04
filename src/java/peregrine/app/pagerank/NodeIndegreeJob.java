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
                emit( target, StructReaders.wrap( true ) );
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