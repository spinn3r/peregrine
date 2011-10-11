package peregrine.pfsd.shuffler;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.async.*;

import com.spinn3r.log5j.Logger;

/**
 * Handles accepting shuffle data and rolling over shuffle files when buffers
 * are full.
 */
public class ShufflerFactory {

    private static Map<String,Shuffler> instances = new HashMap();
    
    public static Shuffler getInstance( String name ) {

        Shuffler shuffler = instances.get( name );

        if ( shuffler == null ) {

            synchronized( instances ) {

                shuffler = instances.get( name );

                if ( shuffler == null ) {

                    shuffler = new Shuffler( name );
                    instances.put( name, shuffler );
                    
                } 

            }
            
        }

        return shuffler;
        
    }
    
}