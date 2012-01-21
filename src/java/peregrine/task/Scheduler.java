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
package peregrine.task;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.rpc.*;

import com.spinn3r.log5j.*;

/**
 * <p>
 * Handles work scheduling in the cluster.
 * 
 * <h2>Overview</h2>
 * 
 * <p>
 * Handles work scheduling in the cluster.  RPC endpoints deliver messages
 * to the scheduler, periodically it wakes up and looks for empty slots on
 * machines which can do work and then schedules jobs when necessary.
 *
 * <h2>Speculative Execution</h2>
 * 
 * <p> Speculative execution works by first executing partitions that have a
 * higher priority.  Once we have spare hosts which have completed all their
 * primary work, we do speculative execution on them by running partition which
 * may be in flight on other hosts.
 *
 * <p> The system works by first sorting all potential replicas on the host by
 * looking at the number of currently executing jobs and sorting them ascending
 * so that jobs which have LESS speculative executions get bumped up higher.
 *
 * <p> Once one of the hosts reports a partition as complete, we mark it
 * complete internally and then add requests into a prey queue to send RPC
 * messages to these hosts to kill the extra tasks.
 *
 * <p> When the tasks die they send off an RPC message saying that they were
 * failed and marked killed at which make these hosts available for more work.
 * 
 * <h2> Speculative Sorting</h2>
 *
 * <p>When shuffle data is sent from map tasks, and there is a shuffle target,
 * we can preemptively sort these files when machines are idle. They have to be
 * sorted ANYWAY and we might as well do this while we have idle CPU time.
 */
public class Scheduler {

    private static final Logger log = Logger.getLogger();

    protected Config config = null;

    protected Membership membership = null;

    /**
     * The list of partitions that are completed.  When a new MapperTask needs
     * more work we verify that we aren't scheduling work form completed
     * partitions.
     */
    protected MarkSet<Partition> completed = new MarkSet();

    /**
     * The list of work that has not yet been completed but is still pending.
     */
    protected MarkSet<Partition> pending = new MarkSet();    

    /**
     * Keep track of which hosts are performing work on which partitions.  This
     * is used so that we can enable speculative execution as we need to
     * terminate work hosts which have active work but another host already
     * completed it.
     */
    protected MapSet<Partition,Host> executing = new MapSet();
    
    /**
     * Hosts which are available for additional work.  These are stored in a 
     * queue so we can just keep popping items until it is empty. 
     */
    protected SimpleBlockingQueue<Host> available = new SimpleBlockingQueue();

    /**
     * Hosts available for additional work for speculative execution.
     */
    protected MarkSet<Host> spare = new MarkSet();

    /**
     * The concurrency of each host as it is executing.
     */
    protected IncrMap<Host> concurrency;

    /**
     * Failure conditions in the scheduler so that we can fail easily.
     */
    protected MarkSet<Fail> failure = new MarkSet();

    /**
     * When a host has has been marked offline, we also need to keep track of
     * the partitions it hosted.  When number of offline hosts for a partition
     * is equal to the replica count the system has to fail as we have lost
     * data (ouch).
     */
    protected IncrMap<Partition> offlinePartitions;

    /**
     * Hosts which should be sent kill requests because a job completed which is
     * also being speculatively executed on other hosts so we need to send a
     * kill command to the host.
     */
    protected SimpleBlockingQueue<Replica> prey = new SimpleBlockingQueue();

    protected ChangedMessage changedMessage = new ChangedMessage();

    private ClusterState clusterState;

    private Job job;

    private String operation;

    protected Scheduler() {}; /* for testing */
    
    public Scheduler( final String operation,
                      final Job job,
                      final Config config,
                      final ClusterState clusterState ) {

        this.operation = operation;
        this.job = job;
        this.config = config;
        this.membership = config.getMembership();
        this.clusterState = clusterState;

        // create a concurrency from all the currently known hosts.
        concurrency = new IncrMap( config.getHosts() );

        offlinePartitions = new IncrMap( config.getMembership().getPartitions() );
        
        // import the current list of online hosts and pay attention to new
        // updates in the future.
        clusterState.getOnline().addListenerWithSnapshot( new MarkListener<Host>() {

                public void updated( Host host, MarkListener.Status status ) {

                    if ( status == MarkListener.Status.MARKED ) {
                        log.info( "Host now available: %s", host );
                        available.putWhenMissing( host );
                    }
                    
                }

            } );

        clusterState.getOffline().addListenerWithSnapshot( new MarkListener<Host>() {

                public void updated( Host host, MarkListener.Status status ) {

                    // for every partition when it is marked offline, go through
                    // and mark every partition offline.  if a partition has NO
                    // online replicas then we must abort the job.
                    List<Partition> partitions = config.getMembership().getPartitions( host );

                    for( Partition part : partitions ) {

                        offlinePartitions.incr( part );

                        if( offlinePartitions.get( part ) == config.getReplicas() ) {
                            markFailed( host, part , false, "*NO TRACE*" );
                            break;
                        }
                                                  
                    }
                    
                }

            } );
        
    }

    /**
     * The operation in progress.  Can be map reduce or merge.
     */
    public String getOperation() {
        return operation;
    }
    
    protected void schedule( Host host ) throws Exception {

        List<Replica> replicas = getReplicasForExecutionByImportance( host );
        
        for( Replica replica : replicas ) {

            Partition part = replica.getPartition();
            
            if ( completed.contains( part ) )
                continue;

            if ( config.getSpeculativeExecutionEnabled() ) {

                // verify that this host isn't ALREADY executing this partition
                // which would be wrong.
                if ( executing.contains( part ) && executing.get( part ).contains( host ) ) {
                    continue;
                }
                
            } else {

                if ( replica.getPriority() > 0 ) {
                    continue;
                }
                
                if ( pending.contains( part ) ) {
                    // skip speculatively executing this partition now.
                    continue;
                }

            }

            // NOTE that this needs to be in the for loop for replica selection
            // because we want to keep filling this host with jobs until we
            // reach the desired concurrency.
            
            if ( concurrency.get( host ) >= config.getConcurrency() ) {
                return;
            }
            
            log.info( "Scheduling %s on %s with current concurrency: %,d of %,d",
                      part, host, concurrency.get( host ), config.getConcurrency() );
            
            invoke( host, part );

            // mark this host as pending so that work doesn't get executed again
            // until we want to do speculative execution

            pending.mark( part );

            concurrency.incr( host );

            executing.put( part, host );
            
            continue;

        }

        spare.mark( host );
        
    }

    /**
     * For a given host, return the replicas that it should process, ordered by
     * number of hosts currently running a job on that partition and then the
     * priority.
     */
    protected List<Replica> getReplicasForExecutionByImportance( Host host ) 
        throws Exception {

        List<Replica> replicas = membership.getReplicas( host );

        if ( replicas == null )
            throw new Exception( "No replicas defined for host: " + host );

        return getReplicasForExecutionByImportance( replicas );
        
    }

    /**
     * For a given host, return the replicas that it should process, ordered by
     * number of hosts currently running a job on that partition and then the
     * priority.
     */
    protected List<Replica> getReplicasForExecutionByImportance( List<Replica> replicas )
        throws Exception {

        final IncrMap<Partition> parallelism = new IncrMap();

        final List<Replica> result = new ArrayList();
        
        // add all these partitions to the mix.
        for( Replica replica : replicas ) {

            Partition part = replica.getPartition();
            
            parallelism.init( part );

            if ( executing.contains( part ) )
                parallelism.set( part, executing.get( part ).size() );

            result.add( replica );
            
        }

        // now sort the result correctly.
        Collections.sort( result, new Comparator<Replica>() {

                public int compare( Replica r0, Replica r1 ) {

                    Partition p0 = r0.getPartition();
                    Partition p1 = r1.getPartition();

                    int diff = parallelism.get( p0 ) - parallelism.get( p1 );

                    if ( diff != 0 )
                        return diff;
                    
                    // now order it by priority
                    return r0.getPriority() - r1.getPriority();

                }
                
            } );
        
        return result;
        
    }
    
    /**
     * Must be implemented by schedulers to hand out work correctly.
     */
    public void invoke( Host host, Partition part ) throws Exception {

        // we could make this an abstract class but this means that we can't
        // test it as easily.

        throw new RuntimeException( "not implemented" );
        
    }
    
    /**
     * Mark a job as complete.  The RPC service calls this method when we are
     * List<Partition> partitions = config..getMembership()done with a job.
     */
    public void markComplete( Host host, Partition partition ) {

        log.info( "Marking partition %s complete from host %s", partition, host );

        // mark this partition as complete.
        completed.mark( partition );

        markInactive( host, partition );

        // for each one of the hosts that are executing.. add them to the prey
        // queue so they can be killed.

        if ( executing.contains( partition ) ) {
            for( Host current : executing.get( partition ) ) {
                prey.put( new Replica( current, partition ) );
            }
        }

    }

    /**
     * Mark a job as failed.
     */
    public void markFailed( Host host,
                            Partition partition,
                            boolean killed,
                            String stacktrace ) {

        log.error( "Host %s has failed on %s with trace: \n %s", host, partition, stacktrace );

        markInactive( host, partition );
        
        // this isn't really a failure because another host finished this job.
        if ( completed.contains( partition ) )
            return;
        
        failure.mark( new Fail( host, partition, stacktrace ) );
        
    }

    /**
     * Mark a given 
     */
    protected void markInactive( Host host, Partition partition ) {

        // now remove this host from the list of actively executing jobs.
        executing.remove( partition, host );

        // TODO: if this partition has other hosts running this job, terminate
        // the jobs.  This is needed for speculative execution.
        
        // clear the pending status for this partition. Note that we need to do
        // this AFTER marking it complete because if we don't then it may be
        // possible to read both completed and pending at the same time and both
        // would be clear.

        pending.clear( partition );

        // add this to the list of available hosts so that we can schedule 
        // additional work.
        available.putWhenMissing( host );

        concurrency.decr( host );

    }
    
    public void markOnline( Host host ) {

    	if ( ! clusterState.getOnline().contains( host ) ) {    		
    		log.info( "Host is now online: %s", host );
    		available.putWhenMissing( host );
    	}

    }

    /**
     * Wait for all jobs to be complete.  This also performs scheduling in this
     * thread until we are done executing.
     */
    public void waitForCompletion() throws Exception {

        log.info( "Waiting on completion %s" , job );
        
        while( true ) {

            // test if we're complete.
            if ( completed.size() == membership.size() ) {
                break;
            }

            // Right now , if ANYTHING has failed, we can not continue.
            if ( failure.size() > 0 ) {

                // log all root causes.
                for( Fail fail : failure.values() ) {
                    log.error( "Failed to handle task: %s \n %s" , fail , fail.stacktrace );
                }

                // throw the current position.
                throw new Exception( String.format( "Failed job %s due to %s", job, failure ) );
                
            }

            // try to drain the prey queue killing each of the entries TODO. we
            // should probably use parallel dispatch on these when there are
            // multiple entries for performance reasons.
            while( prey.size() > 0 ) {

                Replica victim = prey.take();
                sendKill( operation, victim.getHost(), victim.getPartition() );
                
            }

            // TODO: make this a constant.
            Host availableHost = available.poll( 500, TimeUnit.MILLISECONDS );
            
            if ( availableHost != null ) {
                
                log.info( "Scheduling work on: %s", availableHost );

                try {
                    schedule( availableHost );
                } catch ( Exception e ) {
                    //FIXME: we need to gossip about this.
                    log.error( "Unable to schedule work on host: " + availableHost, e );
                }

            }

            String status = status();
            
            if ( changedMessage.hasChanged( status ) ) {
                log.info( "%s", status );
            }

            changedMessage.update( status );

        }
            
    }

    /**
     * Send a kill command to a given host to kill a job on a given partition.
     */
    protected void sendKill( String service,
                             Host host,
                             Partition partition ) {

        while( true ) {

            try {

                if ( clusterState.getOffline().contains( host ) ) {
                    
                    log.info( "Not sending kill.  Host is offline." );
                    return;
                    
                }
                
                Message message = new Message();
                
                message.put( "action" ,   "kill" );
                message.put( "partition", partition.getId() );
                
                log.info( "Sending kill message to host %s: %s", host, message );
                
                new Client().invoke( host, service, message );
                
                break;

            } catch ( IOException e ) {
                
                log.error( String.format( "Unable to kill %s on %s for %s", service, host, partition ), e );
                Threads.coma( 1500L );
                
            }
            
        } 
            
    }

    private String status() {

        StringBuilder buff = new StringBuilder();

        buff.append( String.format( "-- progress for %s %s: --\n", operation, job ) );
        
        buff.append( String.format( "  pending:    %s\n" + 
                                    "  completed:  %s\n" +
                                    "  available:  %s\n" +
                                    "  spare:      %s\n" +
                                    "  online:     %s\n",
                                    format( pending ), format( completed ), available, spare, clusterState.getOnline() ) );

        long perc = (long)(100 * (completed.size() / (double)membership.getPartitions().size()));
        
        buff.append( String.format( "  Perc complete: %,d %% \n", perc ) );

        buff.setLength( buff.length() - 1 ); // trim the trailing \n
        
        return buff.toString();

    }

    private String format( MarkSet<Partition> set ) {

        StringBuilder buff = new StringBuilder();

        buff.append( "[" );
        
        for( Partition part : set.values() ) {

            if ( buff.length() > 1 )
                buff.append( ", " );

            buff.append( part.getId() );
            
        }

        buff.append( "]" );
        
        return buff.toString();
        
    }
    
}

class Fail {

    protected Host host;
    protected Partition partition;

    protected String stacktrace;
    
    public Fail( Host host,
                 Partition partition,
                 String stacktrace ) {
        
        this.host = host;
        this.partition = partition;
        this.stacktrace = stacktrace;
        
    }
    
    public int hashCode() {
        return host.hashCode() + partition.hashCode();
    }
    
    public boolean equals( Object o ) {

    	if ( o != null && o instanceof Fail ) {
            Fail f = (Fail)o;            
            return host.equals( f.host ) && partition.equals( f.partition );            
    	}
    	
    	return false;

    }

    public String toString() {
        return String.format( "%s:%s", host, partition );
    }
    
}

/**
 * A message which returns true if it is different from the previous message.
 */
class ChangedMessage {

    public String last = null;

    public boolean hasChanged( String message ) {
        return ! message.equals( last );
    }

    public void update( String message ) {
        last = message;
    }

}

