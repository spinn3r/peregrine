package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.reduce.*;
import peregrine.map.BaseMapper;

public class Mapper extends BaseMapper {

    public void map( byte[] key, byte[] value ) {
        emit( key, value );
    }

}
