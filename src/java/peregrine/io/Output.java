package peregrine.io;

import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class Output {

    private List<OutputReference> references = new ArrayList();

    public Output() { }

    public Output( String... paths ) {
        for( String path : paths ) {
            add( new FileOutputReference( path ) );
        }
    }

    public Output( OutputReference... refs ) {
        for( OutputReference ref : refs )
            add( ref );
    }
    
    public Output( OutputReference ref ) {
        add( ref );
    }

    public Output add( OutputReference ref ) {
        this.references.add( ref );
        return this;
    }

    public List<OutputReference> getReferences() {
        return references;
    }

    @Override
    public String toString() {
        return references.toString();
    }

}