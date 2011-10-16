package peregrine.rpc;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.pfs.*;
import peregrine.pfsd.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

public class Message extends StructMap {

    public Message() { }

    public Message( String data ) {

        Map<String,List<String>> decoded
            = new QueryStringDecoder( new String( data ) ).getParameters();

        for( String key : decoded.keySet() ) {
            delegate.put( key , decoded.get( key ).get(0) );
        }

    }

    public String toString() {

        QueryStringEncoder encoder = new QueryStringEncoder( "" );

        for( String key : keys ) {
            encoder.addParam( key, delegate.get( key ).toString() );
        }

        return encoder.toString();

    }

}