package maprunner.pagerank;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.*;
import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

public class NodeIndegreeJob {

    public static class Map extends Mapper {

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
        
        public void reduce( byte[] key, List<byte[]> values ) {

            System.out.printf( "FIXME: %s\n", Hex.encode( key ) );
            
            int indegree = values.size();
            emit( key, new IntValue( indegree ).toBytes() );
            
        }
        
    }

}