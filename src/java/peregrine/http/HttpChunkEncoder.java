/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
