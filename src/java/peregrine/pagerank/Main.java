package peregrine.pagerank;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;

import org.apache.log4j.xml.DOMConfigurator;

public class Main {

    public static void main( String[] args ) throws Exception {

        //FIXME: make sure this machine is either the controller box or in the
        //list of hosts configured.  Otherwise it's pointless to run.
        
        DOMConfigurator.configure( "conf/log4j.xml" );
        Config config = Config.parse( "conf/peregrine.conf", "conf/peregrine.hosts" );

        String path = "/pr/test.graph";
        
        ExtractWriter writer = new ExtractWriter( config, path );

        GraphBuilder.buildRandomGraph( writer, 50000 , 100 );
        
        writer.close();

        new Pagerank( config ).exec( path );
        
    }

}