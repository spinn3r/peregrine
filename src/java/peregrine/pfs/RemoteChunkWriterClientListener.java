package peregrine.pfs;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.util.*;

public class RemoteChunkWriterClientListener implements ChannelFutureListener {

    public static final int LIMIT = 10;
    
    protected boolean clear = true;

    protected boolean closed = false;
    
    protected BlockingQueue<byte[]> queue = new LinkedBlockingDeque( LIMIT );

    // stores the result of this IO operation.  Boolean.TRUE if it was success
    // or Throwable it it was a failure.

    protected BlockingQueue<Object> result = new LinkedBlockingDeque( 1 );

    protected Throwable cause = null;
    
    public void operationComplete( ChannelFuture future ) 
        throws Exception {

        Channel channel = future.getChannel();
        
        if ( ! channel.isOpen() ) {

            closed = true;
            
            if ( future.getCause() != null ) {
                setCause( future.getCause() );
            }
            
            return;

        }

        if ( queue.peek() != null ) {

            byte[] data = queue.take();

            ChannelBuffer cbuff = RemoteChunkWriterClient.newChannelBuffer( data );
            
            future.getChannel().write( cbuff ).addListener( this );
            
            return;
            
        }

        // the queue was drained so the next packet should be sent directly

        clear = true;
        
    }

    public void setCause( Throwable throwable ) throws Exception {
        this.cause = throwable;
        this.result.put( throwable );
    }
    
    public void success() throws Exception {
        this.result.put( Boolean.TRUE );
    }

}

