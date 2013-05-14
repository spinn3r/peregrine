package peregrine.http;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * Takes an HTTP chunk and encodes it.
 */
public class HttpChunkEncoder {

    protected static final byte[] CRLF = new byte[] { (byte)'\r', (byte)'\n' };

    private ChannelBuffer data;

    public HttpChunkEncoder() {
        this.data = ChannelBuffers.wrappedBuffer( new byte[0] );
    }

    public HttpChunkEncoder(ChannelBuffer data) {
        this.data = data;
    }

    public ChannelBuffer toChannelBuffer() {

        // use Netty composite buffers to avoid copying excessive data.
        String prefix = String.format( "%2x", data.writerIndex() );

        ChannelBuffer result =
                ChannelBuffers.wrappedBuffer(ChannelBuffers.wrappedBuffer(prefix.getBytes()),
                        ChannelBuffers.wrappedBuffer(CRLF),
                        data,
                        ChannelBuffers.wrappedBuffer(CRLF));

        return result;

    }

}
