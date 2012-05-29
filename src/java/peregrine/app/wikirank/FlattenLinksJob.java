package peregrine.app.pagerank;

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

            emit( key, StructReaders.wrap( values ) );
            
        }
        
    }

}