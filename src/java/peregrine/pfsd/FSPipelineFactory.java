
package peregrine.pfsd;

import static org.jboss.netty.channel.Channels.*;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;

/**
 */
public class FSPipelineFactory implements ChannelPipelineFactory {

    public static int REQUEST_MAX_INITIAL_LINE_LENGTH    = 1024;
    public static int REQUEST_MAX_HEADER_SIZE            = 1024;

    /**
     * The memory consumption of netty partially depends on these variables.
     * 
     * If we have 100 servers in a cluster.  And each is using 8192 to buffer
     * chunks, and each has 25 partitions this would require 100*25 total
     * connections and 8192*100*25 bytes of memory (20.4MB).  At 1024 bytes this
     * would require 2.5MB.
     *
     * <pre>
     * servers    partitions    buffer    memory_per_server
     * 100        25            1024        2.5 MB
     * 100        25            8192       20.4 MB
     * 1000       25            1024       25.0 MB
     * 1000       25            8192      204.0 MB
     * </pre>
     */
    public static int REQUEST_MAX_CHUNK_SIZE             = 2048;

    public ChannelPipeline getPipeline() throws Exception {

        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        pipeline.addLast("decoder",        new HttpRequestDecoder( REQUEST_MAX_INITIAL_LINE_LENGTH ,
                                                                   REQUEST_MAX_HEADER_SIZE,
                                                                   REQUEST_MAX_CHUNK_SIZE ) );

        pipeline.addLast("encoder",        new HttpResponseEncoder() );
        pipeline.addLast("handler",        new FSHandler());
        
        return pipeline;

    }

}
