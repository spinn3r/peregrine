
package peregrine.pfsd;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

public class HexPipelineEncoder implements ChannelUpstreamHandler, ChannelDownstreamHandler {

    private static final Logger log = Logger.getLogger();

    /**
     * When true dump all packets sent over the wire to stdout for debug
     * purposes.
     */
    public static boolean ENABLED = false;
    
    public void handleUpstream( ChannelHandlerContext ctx, ChannelEvent evt) throws Exception {

        try {

            handleEvent( evt );
            
        } finally {
            ctx.sendUpstream( evt );
        }

    }

    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent evt) throws Exception {

        try {

            handleEvent( evt );
            
        } finally {
            ctx.sendDownstream( evt );
        }
        
    }

    private void handleEvent( ChannelEvent evt ) throws Exception {

        if ( ! ENABLED ) return;

        if ( evt instanceof MessageEvent ) {
            
            MessageEvent e = (MessageEvent) evt;

            if ( e.getMessage() instanceof ChannelBuffer ) {
                ChannelBuffer buff = (ChannelBuffer) e.getMessage();
                log.info( "\n%s\n", Hex.pretty( buff ) );
            }
            
        }

    }
    
}
