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
package peregrine.console.swoop;

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
public class Main {

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

        if ( duration == 0 && started != 0 )
            duration = System.currentTimeMillis() - started;
        
        if ( duration <= 0 )
            return "";

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

    /**
     * Show full detail on a specific batch job.
     */
    public void doBatch( Batch batch ) {
        
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

        Job job = batch.getExecutingJob();

        doJobsHeaders();
        doJobs( batch );

        if ( job != null ) {

            ++y_pos;

            curses.mvaddstr( y_pos++, 0, String.format( "Currently executing job '%s':", job.getName() ) );

            ++y_pos;

            curses.mvaddstr( y_pos++, 4, String.format( "Operation: %s", job.getOperation() ) );
            curses.mvaddstr( y_pos++, 4, String.format( "Delegate:  %s", job.getDelegate().getName() ) );

            if ( job.getCombiner() != null ) {
                curses.mvaddstr( y_pos++, 4, String.format( "Combiner:  %s", job.getCombiner().getName() ) );
            }
            
            curses.mvaddstr( y_pos++, 4, String.format( "Input:" ) );

            for( InputReference inputRef : job.getInput().getReferences() ) {
                curses.mvaddstr( y_pos++, 8, String.format( "%s", inputRef ) );
            }

            curses.mvaddstr( y_pos++, 4, String.format( "Output:" ) );

            for( OutputReference outputRef : job.getOutput().getReferences() ) {
                curses.mvaddstr( y_pos++, 8, String.format( "%s", outputRef ) );
            }

            curses.mvaddstr( y_pos++, 4, String.format( "Parameters:" ) );

            for( String key : job.getParameters().getKeys() ) {
                curses.mvaddstr( y_pos++, 8, String.format( "%-20s = %s", key, job.getParameters().getString( key ) ) );
            }
            
            //TODO cause, comparator, maxChunks, partitioner, combiner, 
            
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

        doBatch( batch );

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

    public void doAuto() {

        if ( response.getExecuting() != null ) {

            doExecuting();
            
        } else if ( response.getHistory().size() > 0 ) {

            doHistory();

        }

    }

    /**
     * Display the last executing job.
     */
    public void doLast() {

        if ( response == null )
            return;

        if ( response.getHistory().size() == 0 ) {
            curses.mvaddstr( y_pos++, 0, "No historical batch jobs." );
            return;
        }

        List<Batch> hist = response.getHistory();
        
        Batch batch = hist.get( hist.size() - 1 );

        doBatch( batch );

    }
    
    public static void quit() {

        if ( quit )
            return;
        
        curses.term();
        quit=true;
        
    }

    public String getModeline() {

        return String.format( "Mode: %s (h=history, x=executing, a=automatic, p=pending, l=last q=quit)", mode );
        
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

                switch( mode ) {
                    case AUTO:
                        doAuto();
                        break;
                    case HISTORY:
                        doHistory();
                        break;
                    case PENDING:
                        doPending();
                        break;
                    case EXECUTING:
                        doExecuting();
                        break;
                    case LAST:
                        doLast();
                        break;
                }

            } finally {

                // write one more line so the cursur is in a sane place.
                ++y_pos;
                curses.mvaddstr( y_pos++, 0, getModeline() );
                curses.mvaddstr( y_pos++, 0, "" );
                curses.refresh();

                y_pos = 0;

                char read = (char)curses.getch();

                if ( read > -1 )
                    read = Character.toLowerCase( read );
                
                switch( read ) {

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

                    case 'l':
                        mode = Mode.LAST;
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

        Main swoop = new Main();
        swoop.config = ConfigParser.parse( args );

        swoop.exec();

    }
    
}

enum Mode {
    AUTO, EXECUTING, HISTORY, PENDING, LAST;
}