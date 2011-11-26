package peregrine.controller;

import java.net.*;

import peregrine.*;
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

            Thread.sleep( 60000L );

            // 
            
        } finally {
            controller.shutdown();
        }

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}