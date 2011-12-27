package peregrine.app.pagerank;

import java.util.*;

import peregrine.*;
import peregrine.io.*;
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

        JobOutput nodeIndegreeOutput         = null;
        JobOutput graphBySourceOutput        = null;

        @Override
        public void init( List<JobOutput> output ) {
            nodeIndegreeOutput   = output.get(0);
            graphBySourceOutput  = output.get(1);
        }

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {
            
            int indegree = values.size();

            nodeIndegreeOutput.emit( key, StructReaders.wrap( indegree ) );

            //now emit these to graphBySource so that we have a graph which is
            //sorted (and we are 100% certain it is sorted).
            graphBySourceOutput.emit( key, StructReaders.wrap( values ) );

        }
        
    }

}