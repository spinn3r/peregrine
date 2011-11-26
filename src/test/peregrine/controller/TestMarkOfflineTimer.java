package peregrine.controller;

import java.net.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.http.*;
import peregrine.pfsd.*;

public class TestMarkOfflineTimer extends peregrine.BaseTestWithTwoDaemons {

    public void test() throws Exception {

        HeartbeatTimer.ONLINE_SLEEP_INTERVAL = 5000L;

        MarkOfflineTimer.FAILED_INTERVAL  = 10000L;

        MarkOfflineTimer.SCHEDULE_DELAY = 1000L;
        
        Controller controller = null;
        
        try {

            controller = new Controller( config );

            System.out.printf( "sleeping...\n" );

            Thread.sleep( 1000L );

            System.out.printf( "done\n" );

            shutdown();

            Thread.sleep( 11000L );

            // make sure our hosts are marked offline.

            Offline offline = controller.clusterState.getOffline();
            
            for( Host host : config.getHosts() ) {

                if ( ! offline.contains( host ) )
                    throw new RuntimeException( "host not offline: " + host );
                
            }

            System.out.printf( "WIN.  All hosts have been marked offline.\n" );
            
        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}