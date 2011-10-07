package peregrine.pfs;

import static org.jboss.netty.channel.Channels.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

/**
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @version $Rev: 2226 $, $Date: 2010-03-31 11:26:51 +0900 (Wed, 31 Mar 2010) $
 */
public class RemoteChunkWriterClientPipelineFactory implements ChannelPipelineFactory {

    private HttpRequest request;
    
    public RemoteChunkWriterClientPipelineFactory( HttpRequest request ) {
        this.request = request;
    }
    
    public ChannelPipeline getPipeline() throws Exception {

        ChannelPipeline pipeline = pipeline();

        //FIXME: the client codec needs a memory config too... 
        pipeline.addLast("codec",   new HttpClientCodec());
        pipeline.addLast("handler", new RemoteChunkWriterClientHandler( request ));

        return pipeline;

    }
    
}
