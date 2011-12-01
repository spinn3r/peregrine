package peregrine.app.benchmark;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.values.*;

import org.apache.log4j.xml.DOMConfigurator;

import com.spinn3r.log5j.*;

public class Main {

    private static final Logger log = Logger.getLogger();

    public static void main( String[] args ) throws Exception {

        long max = 10000;

        if ( args.length >= 1 ) {
            max = Long.parseLong( args[0] );
        }

        if ( args.length >= 2 ) {
            Benchmark.Map.EMIT = args[1].equals( "true" );
        }
            
        DOMConfigurator.configure( "conf/log4j.xml" );
        Config config = ConfigParser.parse( args );

        String in = "/test/benchmark.in";
        String out = "/test/benchmark.in";

        log.info( "Testing with %,d records." , max );
        
        System.gc();

        ExtractWriter writer = new ExtractWriter( config, in );

        for( long i = 0; i < max; ++i ) {

            StructReader key  = StructReaders.hashcode( i );
            StructReader value = StructReaders.create( i );

            writer.write( key, value );
        }

        writer.close();

        new Benchmark( config ).exec( in, out );
        
    }

}