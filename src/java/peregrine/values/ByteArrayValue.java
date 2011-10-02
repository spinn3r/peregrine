package peregrine.values;

import java.io.*;
import java.util.*;

import java.nio.charset.Charset;

import peregrine.*;

public class ByteArrayValue extends BaseValue {

    public ByteArrayValue( byte[] data ) {
        super( data );
    }

}