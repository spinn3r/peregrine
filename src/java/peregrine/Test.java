package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.*;
import java.nio.channels.*;
import java.lang.reflect.*;

import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.primitive.*;
import peregrine.app.pagerank.*;
import peregrine.config.*;
import peregrine.worker.*;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.controller.*;
import peregrine.console.controller.*;

import org.jboss.netty.buffer.*;

import com.sun.jna.Pointer;

import com.spinn3r.log5j.Logger;

import java.nio.charset.Charset;

import peregrine.controller.rpcd.delegate.*;

public class Test {

    private static final Logger log = Logger.getLogger();

    public static void main( String[] args ) throws Exception {

        // Batch batch = new Batch();
        // batch.add( new Job() );
        // batch.add( new Job() );
        
        // String result = Status.toStatus( batch );

        // System.out.printf( "%s\n", result );
        
        //System.out.printf( "%s\n", JobState.SUBMITTED );
        //System.out.printf( "%s\n", JobState.SUBMITTED.getClass().newInstance() );

        Job job = new Job();

        Batch batch = new Batch();
        batch.add( job );
        batch.setName( "pagerank" );

        System.out.printf( "FIXME3: %s\n", batch.toMessage().toString() );
        
        Controller controller = new Controller();

        controller.setExecuting( batch );

        ControllerStatusResponse input  = new ControllerStatusResponse( controller );

        ControllerStatusResponse output = new ControllerStatusResponse();

        System.out.printf( "FIXME4: %s\n", input.getExecuting().getJobs() );
        
        output.fromMessage( input.toMessage() );
        
        System.out.printf( "FIXME5: %s\n", output.getExecuting().getJobs() );

        // byte[] b0 = new byte[] { (byte)0x00 , (byte)0x00 , (byte)0x00 , (byte)0x00 , (byte)0x00 , (byte)0x01 , (byte)0x85 , (byte)0x84 , (byte)0x6d , (byte)0x40 , (byte)0x27 , (byte)0x64 , (byte)0xd5 , (byte)0x3c , (byte)0x61 , (byte)0xe2 };

        // byte[] b1 = new byte[] { (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f , (byte)0x7f };

        // List<StructReader> list = new ArrayList();

        // list.add( StructReaders.wrap( b0 ) );
        // list.add( StructReaders.wrap( b1 ) );

        // Collections.sort( list, new StrictSortDescendingComparator() );
        
        // for( StructReader current : list ) {
        //     System.out.printf( "%s\n", Hex.encode( current ) );
        // }
        
    }

}
