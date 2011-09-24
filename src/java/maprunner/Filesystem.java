package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;

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

                    for( int i = 0; i < Integer.MAX_VALUE; ++i ) {

                        String name = LocalPartitionWriter.getFilenameForChunk( i );
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
    
    public static void info( final String path ) throws Exception {

        //TODO using posix symbols and then using perror() would be nice instead
        //of making up my own error messages
        
        System.out.printf( "%s", path );

        forPartitions( new PartitionListener() {

                public boolean onPartition( Partition part, Host host ) throws Exception {

                    File file = new File( Config.getDFSPath( part, host, path ) );

                    if ( file.exists() ) {
                        
                        File chunk = new File( file, LocalPartitionWriter.getFilenameForChunk( 0 ) );
                        
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

    public static void forPartitions( PartitionListener listener ) throws Exception {

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