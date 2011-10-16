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

public class Message {

    public Map<String,String> delegate = new HashMap();

    public Message() {

    }

    public Message( String data ) {

        Map<String,List<String>> decoded
            = new QueryStringDecoder( new String( data ) ).getParameters();

        for( String key : decoded.keySet() ) {
            delegate.put( key , decoded.get( key ).get(0) );
        }

    }
    
    public void put( String key, String value ) {
        delegate.put( key, value );
    }

    public void put( String key, int value ) {
        delegate.put( key, ""+value );
    }

    public String get( String key ) {
        return delegate.get( key );
    }
    
    public String getString( String key ) {
        return get( key );
    }

    public int getInt( String key ) {
        return Integer.parseInt( delegate.get( key ) );
    }

    public String toString() {

        QueryStringEncoder encoder = new QueryStringEncoder( "" );

        for( String key : delegate.keySet() ) {
            encoder.addParam( key, delegate.get( key ) );
        }

        return encoder.toString();

    }

}