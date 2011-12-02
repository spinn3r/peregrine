package peregrine.app.benchmark;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.util.primitive.*;
import peregrine.util.*;

import org.apache.log4j.xml.DOMConfigurator;

import com.spinn3r.log5j.*;

public class Main {

    private static final Logger log = Logger.getLogger();

    public static void main( String[] args ) throws Exception {

        long max = 10000;

        // the width of writes we should use.
        int width = 32;
        
        if ( args.length >= 1 ) {
            max = Long.parseLong( args[0] );
        }

        if ( args.length >= 2 ) {
            width = Integer.parseInt( args[0] );
        }

        if ( args.length >= 3 ) {
            Benchmark.Map.EMIT = args[2].equals( "true" );
        }

        DOMConfigurator.configure( "conf/log4j.xml" );
        Config config = ConfigParser.parse( args );

        String in = "/test/benchmark.in";
        String out = "/test/benchmark.in";

        log.info( "Testing with %,d records." , max );

        ExtractWriter writer = new ExtractWriter( config, in );

        byte[] value = new byte[ width ];
        
        for( long i = 0; i < max; ++i ) {

            byte[] key = MD5.encode( "" + i );

            writer.write( key, value );

        }

        writer.close();

        new Benchmark( config ).exec( in, out );
        
    }

}