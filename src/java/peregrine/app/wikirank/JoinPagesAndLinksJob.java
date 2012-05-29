package peregrine.app.pagerank;

import java.util.*;
import peregrine.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * Join the wikipedia pages which have an ID to their nodes, which have an ID.
 * This is a merge job because we need two imputs.
 */
public class JoinPagesAndLinksJob {

    private static final Logger log = Logger.getLogger();

    public static class Merge extends Merger {

        @Override
        public void merge( StructReader key,
                           List<StructReader> values ) {

            StructReader node = values.get( 0 );

            if ( node != null ) { 
                
                String page = node.readString();

                key = StructReaders.hashcode( page );
                StructReader value = values.get( 1 );

                if ( value == null )
                    return;
                
                List<StructReader> outlinks = StructReaders.unwrap( value );

                StructWriter hashcodes = new StructWriter( outlinks.size() * Hashcode.HASH_WIDTH );

                for( StructReader out : outlinks ) {
                    hashcodes.writeHashcode( out.readString() );
                }
                
                if ( value != null ) {
                    emit( key , hashcodes.toStructReader() );
                }

            }

        }

    }

}