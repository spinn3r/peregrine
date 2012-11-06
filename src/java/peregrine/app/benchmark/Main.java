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
package peregrine.app.benchmark;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.worker.*;

import com.spinn3r.log5j.*;

public class Main {

    private static final Logger log = Logger.getLogger();

    public static int DEFAULT_WIDTH   = 1024;
    public static int DEFAULT_MAX     = 10000;

    public static void main( String[] args ) throws Exception {

        Getopt getopt = new Getopt( args );

        if ( getopt.getBoolean( "help" ) ) {

            System.out.printf( "Options: \n\n" );
            
            System.out.printf( "   --width=WIDTH       Width of the values to write (in bytes).\n" );
            System.out.printf( "   --max=MAX           Max number of writes.\n" );
            System.out.printf( "   --emit=true|false   When true, emit() the key, value.  Default: true\n" );
            System.out.printf( "   --out=              File to write output (or blackhole:)\n" );
            System.out.printf( "   --embed=true|false  Embed the daemon in this proc.\n" );
            System.out.printf( "   --stage=            \n\n" );
            System.out.printf( "        May be one of:  \n" );

            System.out.printf( "        all (default)\n" );
            System.out.printf( "        extract\n" );
            System.out.printf( "        map\n" );
            System.out.printf( "        reduce\n" );
            
            System.out.printf( "\n" );
            
            System.exit( 1 );
            
        }

        String input         = getopt.getString( "input",  "/test/benchmark.in" );
        String output        = getopt.getString( "output", "/test/benchmark.out" );

        // 10MB by default.
        int width            = getopt.getInt( "width", DEFAULT_WIDTH );
        int max              = getopt.getInt( "max", DEFAULT_MAX ); 

        boolean emit         = getopt.getBoolean( "emit", true );
        String stage         = getopt.getString( "stage", "all" );

        boolean embed        = getopt.getBoolean( "embed" );

        EmbeddedDaemon embedded = null;

        if ( embed ) {

            System.out.printf( "Starting embedded daemon...\n" );
            
            embedded = new EmbeddedDaemon( args );
            embedded.start();
            
        }
        
        long size = width * (long) max;

        System.out.printf( "Writing %,d total bytes with width=%,d , max=%,d and emit=%s\n",
                           size, width, max, emit );

        // start our job... 
        
        Config config = ConfigParser.parse( args );
        new Initializer( config ).basic( Main.class );

        Batch batch = new Batch( Main.class );

        if ( "all".equals( stage ) || "extract".equals( stage ) ) {

            batch.map( new Job().setDelegate( Benchmark.Extract.class )
                                .setInput( "blackhole:" )
                                .setOutput( input )
                                .setParameters( "max", max,
                                                "width", width ) );

        }
        
        if ( "all".equals( stage ) || "map".equals( stage ) ) {
            batch.map( Benchmark.Map.class,
                       new Input( input ),
                       new Output( "shuffle:default" ) );
        }
        
        if ( "all".equals( stage ) || "reduce".equals( stage ) ) {
            batch.reduce( Benchmark.Reduce.class,
                          new Input( "shuffle:default" ),
                          new Output( output ) );
        }

        batch.init( args );

        if ( batch.getJobs().size() > 0 ) {
            ControllerClient.submit( config, batch );
        }

    }

}
