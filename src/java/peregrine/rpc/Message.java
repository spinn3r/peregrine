package peregrine.rpc;

import java.util.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.util.*;

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

            Object value = delegate.get( key );

            if( value != null )
                encoder.addParam( key, value.toString() );
            
        }

        return encoder.toString();

    }

}