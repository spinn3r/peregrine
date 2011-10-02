package peregrine.pagerank;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.io.*;

public class NodeMetadataJob {

    public static class Map extends Merger {

        @Override
        public void map( byte[] key,
                         byte[]... values ) {

            // left should be node_indegree , right should be the graph... 

            int indegree  = 0;
            int outdegree = 0;
            
            if ( values[0] != null ) {
                indegree = new IntValue( values[0] ).value;
            }

            if ( values[1] != null ) {

                HashSetValue set = new HashSetValue();
                set.fromBytes( values[1] );

                outdegree = set.size();

            }

            // now emit key, [indegree, outdegree]

            emit( key, new Struct()
                           .write( indegree )
                           .write( outdegree )
                           .toBytes() );
            
        }

    }

    public static class Reduce extends Reducer {

        JobOutput nodeMetadataOutput  = null;
        JobOutput danglingOutput      = null;
        JobOutput nonlinkedOutput     = null;

        @Override
        public void init( JobOutput... output ) {
            nodeMetadataOutput  = output[0];
            danglingOutput      = output[1];
            nonlinkedOutput     = output[2];
        }

        @Override
        public void reduce( byte[] key, List<byte[]> values ) {

            if ( values.size() != 1 )
                throw new RuntimeException( "Too many values.  Error in computation: " + values.size() );

            byte[] value = values.get( 0 );
            
            Struct struct = new Struct( value );

            int indegree  = struct.readInt();
            int outdegree = struct.readInt();

            System.out.printf( "indegree=%,d outdegree=%,d\n", indegree, outdegree );

            // NOTE that since this is being done in the reduce phase we don't
            // need to actually sort the output so we can write to local files
            // directly (sweet). We still have to use PartitionWriter though
            // because even though the files are able to be written to that
            // partition they need to be replicated.  I also need to find a way
            // to swap them in once the task is correctly executed.
            
            if ( indegree == 0 ) {
                
                //emit to dangling ...
                System.out.printf( "dangling\n" );

                // TODO would be NICE to support a sequence file where the
                // values are optional for better storage.
                danglingOutput.emit( key, BooleanValue.TRUE );
                
            }

            if ( outdegree == 0 ) {
                nonlinkedOutput.emit( key, BooleanValue.TRUE );
            }

            // value already has indegree and outdegree so we are done.
            nodeMetadataOutput.emit( key, value );
            
        }

    }

}