/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine;

import java.io.*;
import java.util.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.driver.shuffle.*;
import peregrine.app.pagerank.*;
import peregrine.reduce.sorter.*;
import peregrine.shuffle.*;
import peregrine.reduce.merger.*;

public class TestCombinerEfficiency extends peregrine.BaseTestWithMultipleConfigs {

    public static class Reduce extends Reducer {

        @Override
        public void reduce( StructReader key, List<StructReader> values ) {

            // emit these flat 
            for( StructReader value : values ) {
                emit( key, value );
            }

        }
        
    }

    @Override
    public void doTest() throws Exception {

        doTest( 5000 * getFactor() , 100 ); 

    }

    private void doTest( int nr_nodes,
                         int max_edges_per_node ) throws Exception {

        String path = "/pr/test.graph";

        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder.buildRandomGraph( writer, nr_nodes , max_edges_per_node );
        
        writer.close();

        Controller controller = new Controller( config );

        try {

            // TODO: We can elide this and the next step by reading the input
            // once and writing two two destinations.  this would read from
            // 'path' and then wrote to node_indegree and graph_by_source at the
            // same time.
            
            controller.map( NodeIndegreeJob.Map.class,
                            new Input( path ),
                            new Output( "shuffle:default" ) );

            controller.flushAllShufflers();
            
            controller.reduce( Reduce.class,
                               new Input( "shuffle:default" ),
                               new Output( "/pr/tmp/node_indegree" ) );

        } finally {
            controller.shutdown();
        }

        // now attempt to open the main shuffle file... 

        /*
        combine( "/tmp/peregrine-fs/localhost/11112/0/pr/tmp/shuffled_out/chunk000000.dat" );
        combine( "/tmp/peregrine-fs/localhost/11112/tmp/shuffle/default/0000000003.tmp" );
        combine( "/tmp/peregrine-fs/localhost/11112/tmp/shuffle/default/0000000007.tmp" );
        */

        combine2( "/tmp/peregrine-fs/localhost/11112/0/pr/tmp/node_indegree/" );
        
    }

    private void combine( String path ) throws Exception {

        if ( ! new File( path ).exists() )
            return;
        
        Config config = configs.get( 0 );
        
        //ChunkSorter sorter = new ChunkSorter 

        ShuffleInputReference shuffleInput = new ShuffleInputReference( "default" );

        Partition partition = new Partition( 0 );

        File sorted_chunk = new File( "/tmp/sorted.chunk" );

        List<JobOutput> jobOutput = new ArrayList();

        List<ShuffleInputChunkReader> work = new ArrayList();

        work.add( new ShuffleInputChunkReader( config, partition, path ) );

        ChunkSorter sorter = new ChunkSorter( config , partition, shuffleInput );

        ChunkReader sorted = sorter.sort( work, sorted_chunk, jobOutput );

        // now merge it ...

        //ChunkMerger 

        List<ChunkReader> mergeInput = new ArrayList();
        mergeInput.add( sorted );

        File combined = new File( "/tmp/combined.chunk" );
        
        DefaultChunkWriter writer = new DefaultChunkWriter( config, combined );

        ChunkMerger merger = new ChunkMerger( null, partition, jobOutput );
        merger.merge( mergeInput, writer );

        writer.close();

        double efficiency = (combined.length() / (double)sorted_chunk.length()) * 100;

        System.out.printf( "%s efficiency: %f\n", path, efficiency );
        
    }
    
    private void combine2( String path ) throws Exception {

        File input = new File( path );

        if( input.isDirectory() ) {

            File[] files = input.listFiles();

            for( File file : files ) {
                combine2( file.getPath() );
            }

            return;
            
        }
        
        if ( ! input.exists() )
            return;

        Config config = configs.get( 0 );

        Partition partition = new Partition( 0 );

        List<JobOutput> jobOutput = new ArrayList();

        //ChunkSorter sorter = new ChunkSorter 

        DefaultChunkReader sorted = new DefaultChunkReader( config, input );
        
        List<ChunkReader> mergeInput = new ArrayList();
        mergeInput.add( sorted );

        File combined = new File( "/tmp/combined.chunk" );
        
        DefaultChunkWriter writer = new DefaultChunkWriter( config, combined );

        ChunkMerger merger = new ChunkMerger( null, partition, jobOutput );
        merger.merge( mergeInput, writer );

        writer.close();

        double efficiency = (combined.length() / (double)input.length()) * 100;

        System.out.printf( "%s efficiency: %f\n", path, efficiency );
        
    }

    public static void main( String[] args ) throws Exception {
        //System.setProperty( "peregrine.test.config", "04:01:32" ); 
        //System.setProperty( "peregrine.test.config", "01:01:1" ); 
        //System.setProperty( "peregrine.test.config", "8:1:32" );
        //System.setProperty( "peregrine.test.config", "2:1:3" ); 
        //System.setProperty( "peregrine.test.config", "2:1:3" ); 

        // FIXME: test with larger numbers of files....... FUCK.... so my tests
        // are TOTALLY wrong because the shuffle output os the RECEIVED output
        // not that which we're sending... :-( 

        //System.setProperty( "peregrine.test.factor", "200" ); 
        System.setProperty( "peregrine.test.config", "1:1:1" ); 

        runTests();
        
    }

}
