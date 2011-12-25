package peregrine.config;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import peregrine.config.partitioner.*;
import peregrine.util.primitive.*;
import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

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
    protected Set<Host> hosts = new HashSet<Host>();

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
        
        if ( struct.containsKey( "host" ) )
            setHost( Host.parse( struct.getString( "host" ) ) );

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
    
    public Set<Host> getHosts() {
        return hosts;
    }

    public void setHosts( Set<Host> hosts ) {
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

    static {

        try {

            // Load the default configuration on startup.  This is required for
            // definition of all default values in the system.
            
            InputStream is = ConfigParser.class.getResourceAsStream( "/default.conf" );
            DEFAULTS = new StructMap( is );
            
        } catch ( Throwable t ) {
            throw new RuntimeException( t );
        }
        
    }

}