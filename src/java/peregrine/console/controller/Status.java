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
import peregrine.rpc.*;
import peregrine.util.*;
import peregrine.worker.*;

/**
 * Obtain and print the status of the controller.
 */
public class Status {

    public static String toStatus( Batch batch ) {

        StringBuilder buff = new StringBuilder();
        
        for ( Job job : batch.getJobs() ) {

            buff.append( toStatus( job ) );

        }

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

    public static String toBrief( Job job ) {

        return String.format( "%20s %15s %10s    %s",
                              job.getName(),
                              job.getState(),
                              job.getOperation(),
                              job.getDelegate().getName() );
        
    }
    
    public static void help() {

        System.out.printf( "Controller command line status interface.\n" );
        System.out.printf( "\n" );
        System.out.printf( "Shows information about the current controller status.\n" );
        System.out.printf( "\n" );
        System.out.printf( "--executing    Show currently executing batches.\n" );
        System.out.printf( "--history      Show job history.\n" );
        System.out.printf( "\n" );
        System.out.printf( "Both arguments accept a level argument for the amount of detail to report.\n" );
        System.out.printf( "\n" );
        System.out.printf( "  0:  Just basic stats.\n" );
        System.out.printf( "  1:  Brief report on each job.\n" );
        System.out.printf( "  2:  Full report on each job.\n" );

    }

    public static void main( String[] args ) throws Exception {

        new Initializer().logger( new File( "conf/log4j-silent.xml" ) );
        
        Getopt getopt = new Getopt( args );

        int executing = getopt.getInt( "executing",  0 );
        int history   = getopt.getInt( "history",   -1 );
        
        Config config = ConfigParser.parse( args );

        Client client = new Client( config );
        
        Message message = new Message();
        message.put( "action", "status" );
        
        Message result = client.invoke( config.getController(), "controller", message );

        ControllerStatusResponse response = new ControllerStatusResponse();
        response.fromMessage( result );

        //TODO: include scheduler cluster state.
        
        if ( response.getExecuting() != null ) {

            Batch batch = response.getExecuting();

            int nr_jobs = batch.getJobs().size();
            int nr_complete = 0;
            
            for( Job job : batch.getJobs() ) {
                
                if ( job.getState().equals( JobState.COMPLETED ) )
                    ++nr_complete;
                
            }

            if ( executing >= 0 ) {
            
                double perc_complete = 100 * (nr_complete / (double)nr_jobs);
                
                System.out.printf( "Currently executing batch %s: \n", batch.getName() );
                System.out.printf( "\n" );
                
                System.out.printf( "  nr jobs:            %,d\n" ,     nr_jobs );
                System.out.printf( "  nr complete jobs:   %,d\n" ,     nr_complete );
                System.out.printf( "  perc complete:      %%%4.4f\n" , perc_complete );

            }

            if ( executing <= 1 ) {

                System.out.printf( "\n" );
                
                for ( Job job : batch.getJobs() ) {
                    System.out.printf( "  %s\n", toBrief( job ) );
                }
                
            }

            if ( executing >= 2 ) {
                System.out.printf( "\n" );

                System.out.printf( "%s\n", toStatus( batch ) );
            }
            
        }

        System.out.printf( "Executed %,d batch historical jobs.\n" , response.getHistory().size() );

        //for ( List 
        
    }
    
}