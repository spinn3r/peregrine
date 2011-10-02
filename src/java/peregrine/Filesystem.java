package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.io.*;

public class Filesystem {

    public static void syntax() {

        System.out.printf( "SYNTAX: \n" );
        System.out.printf( "   info path       Print information about a file in the DFS.\n" );
        
    }

    private static void info_file( final String path ) throws Exception {

        final AtomicLong size = new AtomicLong();
        final AtomicInteger nr_chunks = new AtomicInteger();
        
        forPartitions( new PartitionListener() {

                public boolean onPartition( Partition part, Host host ) throws Exception {

                    // FIXME: unify chunk iteration...
                    
                    for( int i = 0; i < Integer.MAX_VALUE; ++i ) {

                        String name = LocalPartition.getFilenameForChunkID( i );
                        String dir = Config.getDFSPath( part, host, path );
                        File chunk = new File( dir, name );

                        if ( chunk.exists() ) {
                            size.set( size.get() + chunk.length() );
                            nr_chunks.set( nr_chunks.get() + 1 );
                        } else {
                            break;
                        }
                            
                    }
                    
                    return true;
                }

            } );

        System.out.printf( "%,d bytes across %,d chunks\n", size.get(), nr_chunks.get() );
        
    }

    private static void cat( final String path,
                             final String key_class,
                             final String value_class ) throws Exception {

        forChunks( path, new FileChunkListener() {

                public boolean onChunk( File file ) throws Exception {

                    ChunkReader reader = new DefaultChunkReader( file );

                    while( true ) {

                        Tuple t = reader.read();

                        if ( t == null )
                            break;
                        
                        Key key = (Key)Class.forName( key_class ).newInstance();
                        Value value = (Value)Class.forName( value_class ).newInstance();

                        key.fromBytes( t.key );
                        value.fromBytes( t.value );

                        System.out.printf( "%s: %s\n", key, value );
                        
                    }

                    return true;
                }
                
            } );

    }

    public static void info( final String path ) throws Exception {

        //TODO using posix symbols and then using perror() would be nice instead
        //of making up my own error messages
        
        System.out.printf( "%s", path );

        forPartitions( new PartitionListener() {

                public boolean onPartition( Partition part, Host host ) throws Exception {

                    File file = new File( Config.getDFSPath( part, host, path ) );

                    if ( file.exists() ) {
                        
                        File chunk = new File( file, LocalPartition.getFilenameForChunkID( 0 ) );
                        
                        if ( chunk.exists() ) {
                            System.out.printf( " is file\n" );
                            info_file( path );
                            return false;
                        } else {
                            System.out.printf( " is directory\n" );
                            return false;
                        }

                    } else {
                        System.out.printf( "Path does not exist.\n" );
                        return false;
                    }

                }

            } );
            
    }

    public static void forChunks( final String path, final FileChunkListener listener )
        throws Exception {

        forPartitions( new PartitionListener() {

                //FIXME: migrate this to PartitionReader I think... 
                // FIXME: unify chunk iteration...
            
                public boolean onPartition( Partition part, Host host ) throws Exception {

                    for( int i = 0; i < Integer.MAX_VALUE; ++i ) {

                        String name = LocalPartition.getFilenameForChunkID( i );
                        String dir = Config.getDFSPath( part, host, path );
                        File chunk = new File( dir, name );

                        if ( chunk.exists() ) {
                            
                            if ( listener.onChunk( chunk ) == false )
                                break;
                            
                        } else {
                            break;
                        }
                            
                    }
                    
                    return true;
                }

            } );

    }
    
    public static void forPartitions( PartitionListener listener )
        throws Exception {

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();

        if ( partitionMembership.size() == 0 ) {
            throw new Exception( "No partition information configured.\n" );
        }
        
        for( Partition part : partitionMembership.keySet() ) {
            
            List<Host> hosts = partitionMembership.get( part );

            Host host = hosts.get( 0 );

            if ( listener.onPartition( part, host ) == false )
                break;

        }

    }

    public static void main( String[] args ) throws Exception {

        //FIXME: this needs to be removed ...
        // TRY with three partitions... 
        Config.addPartitionMembership( 0, "cpu0", "cpu1" );
        Config.addPartitionMembership( 1, "cpu0", "cpu1" );

        if ( args.length < 1 ) {
            syntax();
            System.exit( 1 );
        }

        String cmd = args[0];

        if ( "info".equals( cmd ) ) {
            info( args[1] );
            return;
        } else if ( "cat".equals( cmd ) ) {
            cat( args[1], args[2], args[3] );
            return;
        } else {
            syntax();
            System.exit( 1 );
        }
        
    }
    
}

class PartitionListener {

    public boolean onPartition( Partition part, Host host ) throws Exception {
        return true;
    }
    
}

class FileChunkListener {

    public boolean onChunk( File file ) throws Exception {

        return true;
    }
    
}