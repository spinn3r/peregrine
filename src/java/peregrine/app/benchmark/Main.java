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

    static String in = "/test/benchmark.in";
    static String out = "/test/benchmark.out";

    public static void extract( Config config, int max, int width ) throws Exception {

        log.info( "Testing with %,d records." , max );

        ExtractWriter writer = new ExtractWriter( config, in );

        byte[] value = new byte[ width ];
        
        for( long i = 0; i < max; ++i ) {

            byte[] key = MD5.encode( "" + i );

            writer.write( key, value );

        }

        writer.close();

        System.out.printf( "Wrote %,d bytes to extract writer.\n", writer.length() );

    }
    
    public static void main( String[] args ) throws Exception {

        Getopt getopt = new Getopt( args );

        if ( getopt.getBoolean( "help" ) ) {

            System.out.printf( "Options: \n\n" );
            
            System.out.printf( "   --width=WIDTH       Width of the values to write (in bytes).\n" );
            System.out.printf( "   --max=MAX           Max number of writes.\n" );
            System.out.printf( "   --emit=true|false   When true, emit() the key, value.  Default: true\n" );
            System.out.printf( "   --stage=            \n\n" );
            System.out.printf( "        May be one of:  \n" );

            System.out.printf( "        all (default)\n" );
            System.out.printf( "        extract\n" );
            System.out.printf( "        map\n" );
            System.out.printf( "        reduce\n" );
            
            System.out.printf( "\n" );
            
            System.exit( 1 );
            
        }
        
        int width            = getopt.getInt( "width", 32 );
        int max              = getopt.getInt( "max", 10000 );
        Benchmark.Map.EMIT   = getopt.getBoolean( "emit", true );
        String stage         = getopt.getString( "stage", "all" );

        long size = width * (long) max;

        System.out.printf( "Writing %,d total bytes with width=%,d , max=%,d and emit=%s\n",
                           size, width, max, Benchmark.Map.EMIT );
        
        DOMConfigurator.configure( "conf/log4j.xml" );
        Config config = ConfigParser.parse( args );

        if ( "all".equals( stage ) || "extract".equals( stage ) )
            extract( config, max, width );

        Controller controller = new Controller( config );

        try {

            if ( "all".equals( stage ) || "map".equals( stage ) ) {
                controller.map( Benchmark.Map.class, in );
            }
            
            if ( "all".equals( stage ) || "reduce".equals( stage ) ) {
                controller.reduce( Benchmark.Reduce.class, new Input(), new Output( out ) );
            }

        } finally {
            controller.shutdown();
        }

    }

}