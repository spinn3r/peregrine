package maprunner;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import maprunner.keys.*;
import maprunner.values.*;
import maprunner.util.*;
import maprunner.shuffle.*;

public class Mapper extends BaseMapper {

    public void map( byte[] key, byte[] value ) {
        emit( key, value );
    }

}
