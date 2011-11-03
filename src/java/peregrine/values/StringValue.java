package peregrine.values;

import java.nio.charset.Charset;

public class StringValue extends BaseValue {

    private static Charset UTF8 = Charset.forName( "UTF-8" );

    public StringValue() {}
    
    public StringValue( String data ) {
        super( data.getBytes( UTF8 ) );
    }

}