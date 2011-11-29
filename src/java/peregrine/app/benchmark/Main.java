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

        DOMConfigurator.configure( "conf/log4j.xml" );
        Config config = ConfigParser.parse( args );

        String in = "/test/benchmark.in";
        String out = "/test/benchmark.in";

        int max = 10000;
        
        log.info( "Testing with %,d records." , max );
        
        System.gc();

        ExtractWriter writer = new ExtractWriter( config, in );

        for( int i = 0; i < max; ++i ) {

            byte[] key = MD5.encode( "" + i );
            byte[] value = IntBytes.toByteArray( i );

            writer.write( key, value );
        }

        writer.close();

        new Benchmark( config ).exec( in, out );
        
    }

}