package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.shuffle.*;
import peregrine.map.BaseMapper;

public class Merger extends BaseMapper {

    public void map( byte[] key, byte[]... merged_values ) {}

}
