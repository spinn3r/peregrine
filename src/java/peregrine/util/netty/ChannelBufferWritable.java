package peregrine.util.netty;

import java.io.*;
import org.jboss.netty.buffer.*;

/**
 * Tag interface for anything that can accept the write of a channel buffer.
 * 
 */
public interface ChannelBufferWritable {

    public void write( ChannelBuffer buff ) throws IOException;

    public void shutdown() throws IOException;

    public void close() throws IOException;

}