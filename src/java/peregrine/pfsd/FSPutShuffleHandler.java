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
import peregrine.shuffle.receiver.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSPutShuffleHandler extends FSPutBaseHandler {

    private static final Logger log = Logger.getLogger();

    private static Pattern PATH_REGEX =
        Pattern.compile( "/([0-9]+)/shuffle/([a-zA-Z0-9_-]+)/from-partition/([0-9]+)/from-chunk/([0-9]+)" );

    private FSHandler handler;

    private int to_partition;
    private String name;
    private int from_partition;
    private int from_chunk;
    
    private ShuffleReceiver shuffleReceiver = null;
    
    public FSPutShuffleHandler( FSHandler handler ) throws Exception {
        super( handler );
        
        this.handler = handler;

        String path = handler.request.getUri();
        
        Matcher m = PATH_REGEX.matcher( path );

        if ( ! m.find() )
            throw new IOException( "The path specified is not a shuffle URL: " + path );

        this.to_partition   = Integer.parseInt( m.group( 1 ) );
        this.name           = m.group( 2 );
        this.from_partition = Integer.parseInt( m.group( 3 ) );
        this.from_chunk     = Integer.parseInt( m.group( 4 ) );

        this.shuffleReceiver = handler.daemon.shuffleReceiverFactory.getInstance( this.name );
        
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        super.messageReceived( ctx, e );

        Object message = e.getMessage();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();

                // the split pointer of before and after the suffix.
                int suffix_idx = content.writerIndex() - IntBytes.LENGTH;
                
                // get the last 4 bytes to parse the count.
                int count = content.getInt( suffix_idx );

                // now slice the data sans suffix.
                ChannelBuffer data = content.slice( 0, suffix_idx );

                shuffleReceiver.accept( from_partition, from_chunk, to_partition, count, data );
                
            } else {
                
                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );

                ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
                
            }

        }
            
    }

}

