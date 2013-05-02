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
package peregrine.config;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import peregrine.config.partitioner.*;
import peregrine.util.primitive.*;
import peregrine.util.*;
import peregrine.os.*;

/**
 * Holds all the main properties for a Config including getters and setters.
 * 
 */
public class BaseConfig {
    
    /**
     * The default params we have been started with.  These are loaded from the
     * .conf which we store in the .jar.
     */
    public static StructMap DEFAULTS = null;
    
    /**
     * The root for storing data.
     */
    public String root = null;

    public String basedir = null;

    /**
     * Partition membership.
     */
    protected Membership membership = null;

    /**
     * The current 'host' that we are running on.  This is used so that we can
     * determine whether we should local or remote readers/writers.  This should
     * be set correctly or we won't be able to tell that we are on the local
     * machine and would have performance issues though we should still perform
     * correctly.
     */
    protected Host host = null;

    /**
     * The controller coordinating job tasks in the cluster.  
     */
    protected Host controller = null;

    /**
     * Unique index of hosts. 
     */
    protected SortedSet<Host> hosts = new TreeSet<Host>();

    /**
     * The number of replicas per file we are configured for.  Usually 2 or 3.
     */
    protected int replicas;

    /**
     * The concurrency on a per host basis.  How many mappers and reducers each
     * can run.
     */
    protected int concurrency;

    protected long shuffleBufferSize;

    protected Partitioner partitioner;

    protected int shuffleSegmentMergeParallelism;

    protected long fallocateExtentSize;

    protected boolean fadviseDontNeedEnabled;;

    protected long chunkSize;

    protected long sortBufferSize;

    protected boolean purgeShuffleData;

    protected int port;

    // We keep this around so that the command line can see which fields are set
    // and then set them directly and override the struct and re-configure the
    // system.
    
    protected StructMap struct = null;

    protected String partitionerDelegate = null;

    protected long syncWriteSize = 0;

    protected int maxOpenFileHandles = 0;

    protected boolean speculativeExecutionEnabled = false;

    protected String hostsFile = null;

    protected boolean shuffleMapLockEnabled = false;

    protected String user = null;

    protected boolean enableFailureDuringMemLockError = false;

    protected boolean traceNetworkTraffic = false;

    protected long requireFreeDiskSpaceSize = -1;

    protected long httpMaxChunkSize = -1;

    protected long maxClientShuffleOutputBufferSize = -1;

    protected boolean shieldMappedFileAccess = false;

    protected long maxMemory = -1;

    protected int netSoLinger = -1;

    protected boolean netReuseAddress = false;
    
    protected boolean netTcpNodelay = false;

    protected int netConnectTimeout = -1;
    
    protected int netWriteTimeout = -1;

    protected long sstableBlockSize = -1;

    public void init( StructMap struct ) {

        this.struct = struct;

        setBasedir( struct.get( "basedir" ) );
        setController( Host.parse( struct.get( "controller" ) ) );

        // TODO: consider using reflection to allow these to be set for any
        // directive without updating the mapping.
        
        setPort( struct.getInt( "port" ) );
        setReplicas( struct.getInt( "replicas" ) );
        setConcurrency( struct.getInt( "concurrency" ) );
        setShuffleBufferSize( struct.getSize( "shuffleBufferSize" ) );
        setShuffleSegmentMergeParallelism( struct.getInt( "shuffleSegmentMergeParallelism" ) );
        setFallocateExtentSize( struct.getSize( "fallocateExtentSize" ) );
        setFadviseDontNeedEnabled( struct.getBoolean( "fadviseDontNeedEnabled" ) );
        setChunkSize( struct.getSize( "chunkSize" ) );
        setSortBufferSize( struct.getSize( "sortBufferSize" ) );
        setPartitionerDelegate( struct.getString( "partitionerDelegate" ) );
        setPurgeShuffleData( struct.getBoolean( "purgeShuffleData" ) );
        setSyncWriteSize( struct.getSize( "syncWriteSize" ) );
        setMaxOpenFileHandles( struct.getInt( "maxOpenFileHandles" ) );
        setSpeculativeExecutionEnabled( struct.getBoolean( "speculativeExecutionEnabled" ) );
        setHostsFile( struct.getString( "hostsFile" ) );
        setShuffleMapLockEnabled( struct.getBoolean( "shuffleMapLockEnabled" ) );
        setUser( struct.getString( "user" ) );
        setEnableFailureDuringMemLockError( struct.getBoolean( "enableFailureDuringMemLockError" ) );
        setTraceNetworkTraffic( struct.getBoolean( "traceNetworkTraffic" ) );
        setRequireFreeDiskSpaceSize( struct.getSize( "requireFreeDiskSpaceSize" ) );
        setHttpMaxChunkSize( struct.getSize( "httpMaxChunkSize" ) );
        setMaxClientShuffleOutputBufferSize( struct.getSize( "maxClientShuffleOutputBufferSize" ) );
        setShieldMappedFileAccess( struct.getBoolean( "shieldMappedFileAccess" ) );
        setMaxMemory( struct.getSize( "maxMemory" ) );
        setNetSoLinger( struct.getInt( "netSoLinger" ) );
        setNetReuseAddress( struct.getBoolean( "netReuseAddress" ) );
        setNetTcpNodelay( struct.getBoolean( "netTcpNodelay" ) );
        setNetWriteTimeout( struct.getInt( "netWriteTimeout" ) );
        setNetConnectTimeout( struct.getInt( "netConnectTimeout" ) );
        setSSTableBlockSize( struct.getLong( "sstableBlockSize" ) );

        if ( struct.containsKey( "host" ) )
            setHost( Host.parse( struct.getString( "host" ) ) );

    }

    /**
     * Get the raw underlying StructMap for use with reading the raw params
     * specified in the config (by name).
     */
    public StructMap getStructMap() {
        return struct;
    }
    
    public Membership getMembership() {
        return membership;
    }

    /**
     */
    public Host getHost() {
        return host;
    }

    public void setHost( String host ) {
        setHost( Host.parse( host ) );
    }
    
    public void setHost( Host host ) {
        struct.put( "host", host.toString() );
        this.host = host;
    }

    public Host getController() {
        return controller;
    }

    public void setController( Host controller ) {
        this.controller = controller;
    }

    public boolean isController() {
        return this.controller.getName().equals( getHost().getName() );
    }
    
    public SortedSet<Host> getHosts() {
        return hosts;
    }

    public void setHosts( SortedSet<Host> hosts ) {
        this.hosts = hosts;
    }
    
    public String getRoot() {
        return this.root;
    }

    public void setRoot( String root ) {
        this.root = root;
    }

    public void setRoot( Host host ) {
        setRoot( String.format( "%s/%s/%s", basedir, host.getName(), host.getPort() ) );
    }
    
    public void setBasedir( String basedir ) {
        this.basedir = basedir;
    }

    public String getBasedir() {
        return this.basedir;
    }

    public int getPartitionsPerHost() {
        return this.replicas * this.concurrency;
    }

    public int getReplicas() { 
        return this.replicas;
    }

    public void setReplicas( int replicas ) { 
        this.replicas = replicas;
    }

    public int getConcurrency() { 
        return this.concurrency;
    }

    public void setConcurrency( int concurrency ) { 
        this.concurrency = concurrency;
    }

    public long getShuffleBufferSize() { 
        return this.shuffleBufferSize;
    }

    public void setShuffleBufferSize( long shuffleBufferSize ) { 
        this.shuffleBufferSize = shuffleBufferSize;
    }

    public int getShuffleSegmentMergeParallelism() { 
        return this.shuffleSegmentMergeParallelism;
    }

    public long getFallocateExtentSize() { 
        return this.fallocateExtentSize;
    }

    public void setFallocateExtentSize( long fallocateExtentSize ) { 
        this.fallocateExtentSize = fallocateExtentSize;
    }

    public void setFadviseDontNeedEnabled( boolean fadviseDontNeedEnabled ) { 
        this.fadviseDontNeedEnabled = fadviseDontNeedEnabled;
    }

    public boolean getFadviseDontNeedEnabled() { 
        return this.fadviseDontNeedEnabled;
    }

    public void setShuffleSegmentMergeParallelism( int shuffleSegmentMergeParallelism ) { 
        this.shuffleSegmentMergeParallelism = shuffleSegmentMergeParallelism;
    }

    public long getChunkSize() { 
        return this.chunkSize;
    }

    public void setChunkSize( long chunkSize ) { 
        this.chunkSize = chunkSize;
    }

    public long getSortBufferSize() { 
        return this.sortBufferSize;
    }

    public void setSortBufferSize( long sortBufferSize ) { 
        this.sortBufferSize = sortBufferSize;
    }

    public boolean getPurgeShuffleData() { 
        return this.purgeShuffleData;
    }

    public void setPurgeShuffleData( boolean purgeShuffleData ) { 
        this.purgeShuffleData = purgeShuffleData;
    }

    public int getPort() { 
        return this.port;
    }

    public void setPort( int port ) { 
        this.port = port;
    }

    public String getPartitionerDelegate() { 
        return this.partitionerDelegate;
    }

    public void setPartitionerDelegate( String partitionerDelegate ) { 
        this.partitionerDelegate = partitionerDelegate;
    }

    public void setSyncWriteSize( long syncWriteSize ) { 
        this.syncWriteSize = syncWriteSize;
    }

    public long getSyncWriteSize() { 
        return this.syncWriteSize;
    }

    public int getMaxOpenFileHandles() { 
        return this.maxOpenFileHandles;
    }

    public void setMaxOpenFileHandles( int maxOpenFileHandles ) { 
        this.maxOpenFileHandles = maxOpenFileHandles;
    }

    public void setSpeculativeExecutionEnabled( boolean speculativeExecutionEnabled ) { 
        this.speculativeExecutionEnabled = speculativeExecutionEnabled;
    }

    public boolean getSpeculativeExecutionEnabled() { 
        return this.speculativeExecutionEnabled;
    }

    public void setHostsFile( String hostsFile ) { 
        this.hostsFile = hostsFile;
    }

    public String getHostsFile() { 
        return this.hostsFile;
    }

    public boolean getShuffleMapLockEnabled() { 
        return this.shuffleMapLockEnabled;
    }

    public void setShuffleMapLockEnabled( boolean shuffleMapLockEnabled ) { 
        this.shuffleMapLockEnabled = shuffleMapLockEnabled;
    }

    public String getUser() { 
        return this.user;
    }

    public void setUser( String user ) { 
        this.user = user;
    }

    public Map<String,String> toDict() {
        return struct.toDict();
    }

    public boolean getEnableFailureDuringMemLockError() { 
        return this.enableFailureDuringMemLockError;
    }

    public void setEnableFailureDuringMemLockError( boolean enableFailureDuringMemLockError ) { 
        this.enableFailureDuringMemLockError = enableFailureDuringMemLockError;
    }

    public boolean getTraceNetworkTraffic() { 
        return this.traceNetworkTraffic;
    }

    public void setTraceNetworkTraffic( boolean traceNetworkTraffic ) { 
        this.traceNetworkTraffic = traceNetworkTraffic;
    }

    public long getRequireFreeDiskSpaceSize() { 
        return this.requireFreeDiskSpaceSize;
    }

    public void setRequireFreeDiskSpaceSize( long requireFreeDiskSpaceSize ) { 
        this.requireFreeDiskSpaceSize = requireFreeDiskSpaceSize;
    }

    public long getHttpMaxChunkSize() { 
        return this.httpMaxChunkSize;
    }

    public void setHttpMaxChunkSize( long httpMaxChunkSize ) { 
        this.httpMaxChunkSize = httpMaxChunkSize;
    }

    public long getMaxClientShuffleOutputBufferSize() {
        return this.maxClientShuffleOutputBufferSize;
    }

    public void setMaxClientShuffleOutputBufferSize( long maxClientShuffleOutputBufferSize ) {
        this.maxClientShuffleOutputBufferSize = maxClientShuffleOutputBufferSize;
    }

    public boolean getShieldMappedFileAccess() { 
        return this.shieldMappedFileAccess;
    }

    public void setShieldMappedFileAccess( boolean shieldMappedFileAccess ) { 
        this.shieldMappedFileAccess = shieldMappedFileAccess;
    }

    public long getMaxMemory() { 
        return this.maxMemory;
    }

    public void setMaxMemory( long maxMemory ) { 
        this.maxMemory = maxMemory;
    }

    public int getNetSoLinger() { 
        return this.netSoLinger;
    }

    public void setNetSoLinger( int netSoLinger ) { 
        this.netSoLinger = netSoLinger;
    }

    public boolean getNetReuseAddress() { 
        return this.netReuseAddress;
    }

    public void setNetReuseAddress( boolean netReuseAddress ) { 
        this.netReuseAddress = netReuseAddress;
    }

    public boolean getNetTcpNodelay() { 
        return this.netTcpNodelay;
    }

    public void setNetTcpNodelay( boolean netTcpNodelay ) { 
        this.netTcpNodelay = netTcpNodelay;
    }

    public int getNetWriteTimeout() { 
        return this.netWriteTimeout;
    }

    public void setNetWriteTimeout( int netWriteTimeout ) { 
        this.netWriteTimeout = netWriteTimeout;
    }

    public int getNetConnectTimeout() { 
        return this.netConnectTimeout;
    }

    public void setNetConnectTimeout( int netConnectTimeout ) { 
        this.netConnectTimeout = netConnectTimeout;
    }

    public long getSSTableBlockSize() { 
        return this.sstableBlockSize;
    }

    public void setSSTableBlockSize( long sstableBlockSize ) { 
        this.sstableBlockSize = sstableBlockSize;
    }

    static {

        try {

            // Load the default configuration on startup.  This is required for
            // definition of all default values in the system.  This is actually
            // the peregrine.conf copied to default.conf so we can use that as
            // the defaults.
            
            InputStream is = ConfigParser.class.getResourceAsStream( "/default.conf" );
            DEFAULTS = new StructMap( is );
            
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
        
    }

}
