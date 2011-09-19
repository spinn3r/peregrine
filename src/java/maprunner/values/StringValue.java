package maprunner.values;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import maprunner.*;

public class StringValue extends BaseValue {

    private static Charset UTF8 = Charset.forName( "UTF-8" );
    
    public StringValue( String data ) {
        super( data.getBytes( UTF8 ) );
    }

}