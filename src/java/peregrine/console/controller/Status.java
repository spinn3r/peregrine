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
package peregrine.console.controller;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

import com.spinn3r.log5j.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.controller.rpcd.delegate.*;
import peregrine.io.*;
import peregrine.os.*;
import peregrine.rpc.*;
import peregrine.util.*;
import peregrine.worker.*;

/**
 * Obtain and print the status of the controller.
 */
public class Status {

    public static final int PROGRESS_WIDTH = 40;

    protected Exception cause = null;
    
    protected ControllerStatusResponse response = new ControllerStatusResponse();

    protected Config config = null;

    protected int y_pos = 0;

    protected Mode mode = Mode.AUTO;

    /**
     * True after quit is called so we don't double quit.
     */
    protected static boolean quit = false;
    
    public static String toStatus( Batch batch ) {

        StringBuilder buff = new StringBuilder();
        
        for ( Job job : batch.getJobs() ) {

            buff.append( toStatus( job ) );

        }

        return buff.toString();
        
    }

    /**
     * Draw a progress meter in ASCII with the given width and percentage
     * filled.
     */
    public static String progress( double perc, int width ) {

        StringBuilder buff = new StringBuilder();

        buff.append( "[" );
        
        int cutoff = (int)(width * ( perc / (double)100 ));
        
        for( int i = 0; i < width; ++i ) {

            if ( i < cutoff ) {
                buff.append( "*" );
            } else {
                buff.append( " " );
            }
            
        }

        buff.append( "]" );

        buff.append( String.format( " %4.1f%%", perc ) );
        
        return buff.toString();
        
    }

    /**
     * Pretty print the given object so we can represent it on the console.
     */
    public static String toStatus( Object obj ) {

        StringBuilder buff = new StringBuilder();

        try {

            buff.append( String.format( "%s:\n", obj.getClass().getSimpleName() ) );
            
            Field[] fields = obj.getClass().getDeclaredFields();

            for( Field f : fields ) {

                if ( Modifier.isStatic( f.getModifiers() ) )
                    continue;

                f.setAccessible( true );
                Object value = f.get( obj );

                if ( value != null ) {
                    
                    if ( f.getType().equals( Class.class ) ) {
                        value = ((Class)value).getName();
                    }

                }

                buff.append( "    " );
                buff.append( f.getName() );
                buff.append( "=" );
                buff.append( value );
                buff.append( "\n" );
            }

            return buff.toString();

        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
            
    }

    public void doBatchOverviewHeaders() {

        curses.mvaddstr( y_pos++, 4, String.format( "%-40s %-20s %-10s %-8s %-8s %-12s %-15s %s", "name", "identifier", "state", "nr jobs", "start", "end", "duration", "cause") );
        curses.mvaddstr( y_pos++, 4, String.format( "%-40s %-20s %-10s %-8s %-8s %-12s %-15s %s", "----", "----------", "-----", "-------", "-----", "---", "--------", "-----" ) );
        
    }
    
    public void doBatchOverview( Batch batch ) {
        curses.mvaddstr( y_pos++, 4, String.format( "%-40s %-20s %-10s %-8s %-8s %-12s %-15s %s",
                                                    batch.getName(), batch.getIdentifier(), batch.getState(),
                                                    batch.getJobs().size(), batch.getStart(), batch.getEnd(),
                                                    getDuration( batch.getStarted(), batch.getDuration() ),
                                                    getFirstLine( batch.getCause(), "" ) ) );
    }

    public void doBatches( List<Batch> batches ) {

        doBatchOverviewHeaders();

        // cap the number of entries here.  
        for( Batch batch : batches ) {
            doBatchOverview( batch );
        }

    }

    public void doPending() {

        curses.mvaddstr( y_pos++, 0, "Pending:" );

        ++y_pos;

        doBatches( response.getPending() );

    }

    public void doHistory() {

        curses.mvaddstr( y_pos++, 0, "History:" );

        ++y_pos;

        doBatches( response.getHistory() );

        /*
        if ( response.getHistory().size() > 0 ) {

            Batch last = response.getHistory().get( 0 );
            
            // print the cause of the last failure.
            if ( ! Strings.empty( last.getCause() ) ) {
                
                ++y_pos;
                
                String[] frames = last.getCause().split( "\n" );
                
                for( String frame : frames ) {
                    curses.mvaddstr( y_pos++, 4, frame );
                }
                
            }

        }

        */
        
    }

    private String getFirstLine( String data, String _default ) {

        if ( Strings.empty( data ) )
            return _default;
        
        String[] split = data.trim().split( "\n" );

        if ( split.length == 0 )
            return _default;
        
        return split[0];
        
    }

    private String getDuration( long started, long duration ) {

        if ( duration <= 0 )
            duration = System.currentTimeMillis() - started;

        return new Duration( duration ).toString();
        
    }
    
    public void doJobsHeaders() {

        curses.mvaddstr( y_pos++, 4, String.format( "%-20s %-15s %-10s %-12s %s", "name", "state", "operation", "duration", "delegate" ) );
        curses.mvaddstr( y_pos++, 4, String.format( "%-20s %-15s %-10s %-12s %s", "----", "-----", "---------", "--------", "--------" ) );

    }
    
    public void doJobs( Batch batch ) {

        for ( Job job : batch.getJobs() ) {

            String formatted = String.format( "%-20s %-15s %-10s %-12s %s",
                                              job.getName(),
                                              job.getState(),
                                              job.getOperation(),
                                              getDuration( job.getStarted(), job.getDuration() ),
                                              job.getDelegate().getName() );

            curses.mvaddstr( y_pos++, 4, String.format( "%s" , formatted ) );

        }

    }

    public void doExecuting() {

        if ( response == null )
            return;
        
        Batch batch = response.getExecuting();

        if ( batch == null ) {
            curses.mvaddstr( y_pos++, 0, "No executing batch jobs." );
            return;
        }

        int nr_jobs = batch.getJobs().size();
        int nr_complete = 0;
        
        for( Job job : batch.getJobs() ) {
            
            if ( job.getState().equals( JobState.COMPLETED ) )
                ++nr_complete;
            
        }

        double perc_complete = 100 * (nr_complete / (double)nr_jobs);

        curses.mvaddstr( y_pos++, 0, "Currently executing batch:" );

        ++y_pos;

        //TODO: add the description for this batch.

        doBatchOverviewHeaders();
        doBatchOverview( batch );

        ++y_pos;

        curses.mvaddstr( y_pos++, 4, String.format( "nr complete jobs:     %,d" ,     nr_complete ) );
        curses.mvaddstr( y_pos++, 4, String.format( "batch perc complete:  %s" ,      progress( perc_complete, PROGRESS_WIDTH ) ) );

        //add job perc complete which comes from the scheduler.

        if ( response.getSchedulerStatusResponse() != null ) {
            curses.mvaddstr( y_pos++, 4, String.format( "job perc complete:    %s" ,
                                                        progress( response.getSchedulerStatusResponse().getProgress(), PROGRESS_WIDTH ) ) );
        }

        ++y_pos;

        Job executingJob = batch.getExecutingJob();

        doJobsHeaders();
        doJobs( batch );

        if ( executingJob != null ) {

            ++y_pos;

            curses.mvaddstr( y_pos++, 0, String.format( "Currently executing job '%s':", executingJob.getName() ) );

            ++y_pos;

            curses.mvaddstr( y_pos++, 4, String.format( "Operation: %s", executingJob.getOperation() ) );
            curses.mvaddstr( y_pos++, 4, String.format( "Delegate:  %s", executingJob.getDelegate().getName() ) );

            curses.mvaddstr( y_pos++, 4, String.format( "Input:" ) );

            for( InputReference inputRef : executingJob.getInput().getReferences() ) {
                curses.mvaddstr( y_pos++, 8, String.format( "%s", inputRef ) );
            }

            curses.mvaddstr( y_pos++, 4, String.format( "Output:" ) );

            for( OutputReference outputRef : executingJob.getOutput().getReferences() ) {
                curses.mvaddstr( y_pos++, 8, String.format( "%s", outputRef ) );
            }
            
            //TODO parameters, cause, comparator, maxChunks, partitioner, combiner, 
            
        }

    }

    public static void help() {

        System.out.printf( "Controller command line status interface.\n" );
        System.out.printf( "\n" );
        System.out.printf( "Shows information about the current controller status.\n" );
        System.out.printf( "\n" );
        System.out.printf( "--executing    Show currently executing batches.\n" );
        System.out.printf( "--history      Show historical batches.\n" );
        System.out.printf( "--pending      Show pending batches.\n" );
        System.out.printf( "\n" );
        System.out.printf( "Both arguments accept a level argument for the amount of detail to report.\n" );
        System.out.printf( "\n" );
        System.out.printf( "  0:  Just basic stats.\n" );
        System.out.printf( "  1:  Brief report on each job.\n" );
        System.out.printf( "  2:  Full report on each job.\n" );

    }

    /**
     * Read the status messgae from the controller and update global variables.
     */
    private void readStatusFromController() {

        cause = null;
        response = new ControllerStatusResponse();

        try {
        
            Client client = new Client( config );
            
            Message message = new Message();
            message.put( "action", "status" );
            
            Message result = client.invoke( config.getController(), "controller", message );
            
            response.fromMessage( result );

        } catch ( Exception e ) {
            cause = e;
        }

    }

    public void doModeAuto() {

        if ( response.getExecuting() != null ) {

            doExecuting();
            
        } else if ( response.getHistory().size() > 0 ) {

            doHistory();

        }

    }

    public void doModeExecuting() {
        doExecuting();
    }

    public void doModePending() {
        doPending();
    }

    public void doModeHistory() {
        doHistory();
    }

    public static void quit() {

        if ( quit )
            return;
        
        curses.term();
        quit=true;
        
    }
    
    public void exec() {

        while( true ) {

            if ( quit )
                break;
            
            curses.clear();

            readStatusFromController();
            
            //TODO: include scheduler cluster state.

            try {

                String state = "ONLINE";

                if ( cause != null )
                    state = "OFFLINE: " + cause.getMessage();

                curses.mvaddstr( y_pos++, 0, String.format( "%s", "Controller:" ) );
                curses.mvaddstr( y_pos++, 4, String.format( "%-20s %s", "Host:", config.getController() ) );
                curses.mvaddstr( y_pos++, 4, String.format( "%-20s %s", "State:", state ) );
                curses.mvaddstr( y_pos++, 4, String.format( "%-20s %s", "Historical batches:", response.getHistory().size() ) );
                curses.mvaddstr( y_pos++, 4, String.format( "%-20s %s", "Pending batches:", response.getPending().size() ) );

                ++y_pos;

                curses.mvaddstr( y_pos++, 0, String.format( "%s %s",    "Last refresh:", new Date() ) );

                if ( cause != null )
                    continue;

                ++y_pos;
                
                // TODO compute some stats on the history ... # failed, #
                // completed.  Failure rate.

                // TODO: the table should have 3 options:
                //
                //  - (E) show what is currently executing 
                //  - (P) show pending batches
                //  - (H) show what is in history
                //
                //  - (X) they should ALL have the option to show extended metadata 

                if ( mode.equals( Mode.AUTO ) ) {
                    doModeAuto();
                } else if ( mode.equals( Mode.HISTORY ) ) {
                    doModeHistory();
                } else if ( mode.equals( Mode.PENDING ) ) {
                    doModePending();
                } else if ( mode.equals( Mode.EXECUTING ) ) {
                    doModeExecuting();
                } 

            } finally {

                // write one more line so the cursur is in a sane place.
                ++y_pos;
                curses.mvaddstr( y_pos++, 0, String.format( "Mode: %s (h=history, x=executing, a=automatic, p=pending q=quit)", mode ) );
                curses.mvaddstr( y_pos++, 0, "" );
                curses.refresh();

                y_pos = 0;
                
                switch( (char)curses.getch() ) {

                    case 'a':
                        mode = Mode.AUTO;
                        break;

                    case 'x':
                        mode = Mode.EXECUTING;
                        break;

                    case 'h':
                        mode = Mode.HISTORY;
                        break;

                    case 'p':
                        mode = Mode.PENDING;
                        break;

                    case 'q':
                        quit();
                        System.exit( 0 );
                        break; /* not needed */
                        
                    default:
                        break;
                        
                }

            }
            
        }

    }

    public static void main( String[] args ) throws Exception {

        // TODO:
        //
        //  q      should quit

        new Initializer().logger( new File( "conf/log4j-silent.xml" ) );

        curses.init();

        Runtime.getRuntime().addShutdownHook( new Thread() {

                public void run() {
                    quit();
                }
                
            } );

        Getopt getopt = new Getopt( args );

        Status status = new Status();
        
        status.config    = ConfigParser.parse( args );

        status.exec();

    }
    
}

enum Mode {
    AUTO, EXECUTING, HISTORY, PENDING;
}