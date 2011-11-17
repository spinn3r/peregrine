package peregrine.pfsd;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import com.spinn3r.log5j.*;

/**
 * Base handler for dealing with logging throughput and other issues with PUT.
 */
public class FSPutBaseHandler extends SimpleChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    /**
     * NR of bytes written.
     */
    protected long written = 0;

    /**
     * Time we started the request.
     */
    protected long started;

    /**
     * Number of chunks written.
     */
    protected long chunks = 0;

    protected FSHandler handler;
    
    public FSPutBaseHandler( FSHandler handler ) {

        this.handler = handler;

        started = System.currentTimeMillis();

    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Object message = e.getMessage();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();

                written += content.writerIndex();
                chunks = chunks + 1;

            } else {

                // log that this was written successfully including the NR of
                // bytes.

                long duration = System.currentTimeMillis() - started;

                int mean_chunk_size = 0;

                if ( chunks > 0 )
                    mean_chunk_size = (int)(written / chunks);
                
                log.info( "Wrote %,d bytes in %,d chunks (mean chunk size = %,d bytes) in %,d ms to %s",
                          written, chunks, mean_chunk_size, duration, handler.path );

            }

        }
            
    }

}

