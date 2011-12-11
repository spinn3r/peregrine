package peregrine.config;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import peregrine.config.router.*;
import peregrine.util.primitive.*;
import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 * 
 */
public class BaseConfig {
    
    private String routerDelegate = null;

    public static int DEFAULT_PORT = 11112;

    public static String DEFAULT_BASEDIR = "/tmp/peregrine-fs";
    
    /**
     * The root for storing data.
     */
    public String root = DEFAULT_BASEDIR;

    public String basedir = DEFAULT_BASEDIR;
    
    /**
     * Partition membership.
     */
    protected Membership membership = new Membership();

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

    protected PartitionRouter router;

    protected int mergeFactor;

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
    
    public void init( StructMap struct ) {

        this.struct = struct;

        setBasedir( struct.get( "basedir" ) );
        setController( Host.parse( struct.get( "controller" ) ) );

        // TODO: consider using reflection to allow these to be set for any
        // directive without updating the mapping.
        
        setReplicas( struct.getInt( "replicas" ) );
        setConcurrency( struct.getInt( "concurrency" ) );
        setShuffleBufferSize( struct.getSize( "shuffleBufferSize" ) );
        setMergeFactor( struct.getInt( "mergeFactor" ) );
        setFallocateExtentSize( struct.getSize( "fallocateExtentSize" ) );
        setFadviseDontNeedEnabled( struct.getBoolean( "fadviseDontNeedEnabled" ) );
        setChunkSize( struct.getSize( "chunkSize" ) );
        setSortBufferSize( struct.getSize( "sortBufferSize" ) );
        setRouterDelegate( struct.getString( "routerDelegate" ) );
        setPurgeShuffleData( struct.getBoolean( "purgeShuffleData" ) );
    }

    public Membership getMembership() {
        return membership;
    }

    /**
     */
    public Host getHost() {
        return host;
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

    public int getMergeFactor() { 
        return this.mergeFactor;
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

    public void setMergeFactor( int mergeFactor ) { 
        this.mergeFactor = mergeFactor;
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

    public String getRouterDelegate() { 
        return this.routerDelegate;
    }

    public void setRouterDelegate( String routerDelegate ) { 
        this.routerDelegate = routerDelegate;
    }

}