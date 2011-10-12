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

    public static final int PENDING  = -1;
    public static final int CLOSED   = 0;
    public static final int OPEN     = 1;
    
    public static final int LIMIT = 10;
    
    protected boolean clear = false;

    protected int state = PENDING;

    protected BlockingQueue<ChannelBuffer> queue = new LinkedBlockingDeque( LIMIT );

    // stores the result of this IO operation.  Boolean.TRUE if it was success
    // or Throwable it it was a failure.

    protected BlockingQueue<Object> result = new LinkedBlockingDeque( 1 );

    protected Throwable cause = null;

    protected URI uri;

    protected Channel channel;

    protected RemoteChunkWriterClient client; 
    
    public RemoteChunkWriterClientListener( RemoteChunkWriterClient client ) {
        this.client = client;
    }

    public void operationComplete( ChannelFuture future ) 
        throws Exception {

        channel = future.getChannel();

        if ( state == -1 && channel.isOpen() ) {

            state = OPEN;

            channel.write( client.request );
            
            // we need to find out when we are closed now.
            channel.getCloseFuture().addListener( this );

        } else if ( ! channel.isOpen() ) {

            state = CLOSED;

            if ( future.getCause() != null ) {
                setCause( future.getCause() );
            }
            
            return;

        }

        if ( queue.peek() != null ) {

             ChannelBuffer data = queue.take();

            // NOTE that even if the last response was written here we MUST wait
            // until we get the HTTP response.

            channel.write( data ).addListener( this );

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

    public boolean isClosed() {
        return state == CLOSED;
    }

}
