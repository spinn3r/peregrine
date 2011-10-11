package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;
import java.util.regex.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.*;
import org.jboss.netty.handler.stream.*;
import org.jboss.netty.util.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.util.*;
import peregrine.pfsd.shuffler.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSPutShuffleHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    private static Pattern PATH_REGEX =
        Pattern.compile( "/shuffle/([a-zA-Z0-9]+)/from-partition/([0-9]+)/from-chunk/([0-9]+)/to-partition/([0-9]+)" );

    public static byte[] EOF = new byte[0];

    private OutputStream asyncOutputStream = null;

    /**
     * NR of bytes written.
     */
    private long written = 0;

    /**
     * Time we started the request.
     */
    private long started;

    /**
     * Number of chunks written.
     */
    private long chunks = 0;

    private FSHandler handler;

    private int from_partition;
    private int from_chunk;
    private int to_partition;
    private String name;

    private Shuffler shuffler = null;
    
    public FSPutShuffleHandler( FSHandler handler ) throws Exception {
        this.handler = handler;

        started = System.currentTimeMillis();

        String path = handler.request.getUri();
        
        Matcher m = PATH_REGEX.matcher( path );

        if ( ! m.find() )
            throw new IOException( "The path specified is not a shuffle URL: " + path );

        this.name           = m.group( 1 );
        this.from_partition = Integer.parseInt( m.group( 2 ) );
        this.from_chunk     = Integer.parseInt( m.group( 3 ) );
        this.to_partition   = Integer.parseInt( m.group( 4 ) );

        this.shuffler = ShufflerFactory.getInstance( this.name );
        
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Object message = e.getMessage();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();
                byte[] data = content.array();

                written += data.length;
                chunks = chunks + 1;

            } else {

            }

        }
            
    }

}

