package peregrine.pfs;

import static org.jboss.netty.channel.Channels.*;
import static peregrine.pfsd.FSPipelineFactory.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

/**
 *
 * @version $Rev: 2226 $, $Date: 2010-03-31 11:26:51 +0900 (Wed, 31 Mar 2010) $
 */
public class RemoteChunkWriterClientPipelineFactory implements ChannelPipelineFactory {

    private RemoteChunkWriterClient client;
    
    public RemoteChunkWriterClientPipelineFactory( RemoteChunkWriterClient client ) {
        this.client = client;
    }
    
    public ChannelPipeline getPipeline() throws Exception {

        ChannelPipeline pipeline = pipeline();

        //FIXME: the client codec needs a memory config too... 
        pipeline.addLast("codec",   new HttpClientCodec( MAX_INITIAL_LINE_LENGTH ,
                                                         MAX_HEADER_SIZE,
                                                         MAX_CHUNK_SIZE ));
        
        pipeline.addLast("handler", new RemoteChunkWriterClientHandler( client ));

        return pipeline;

    }
    
}
