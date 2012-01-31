#!/usr/bin/python

###
#
# TODO
#
# - print the branch that this changeset used in the sidebar.
#
# - CSS this bitch
#
# - take an --index option so that we can just regen the index.
#
# - 

import os
import re
import traceback
import datetime

import sys
from subprocess import *

LIMIT=5
BRANCH="default"

SCRATCH="/tmp/integration/peregrine/"
TEST_LOGS="/var/lib/integration/peregrine"

TEST_COMMAND="ant clean test"
#TEST_COMMAND="false"

REPO="https://burtonator:redapplekittycat@bitbucket.org/burtonator/peregrine"

class ReportIndex:

    file=None

    def __init__(self):
        
        self.file=open( "%s/index.html" % TEST_LOGS , "w" );

        self.file.write( "<frameset cols='30%,70%' title=''>" )
        self.file.write( "<frame src='left.html' name='left' title='all tests'>" )
        self.file.write( "<frame src='' name='right' title=''>" )
        self.file.write( "</frameset>" )
        
    def close(self):
        self.file.close()

class ReportSidebar:

    file=None

    def __init__(self):

        self.file=open( "%s/left.html" % TEST_LOGS , "w" );

        self.file.write( "<table width='100%' cellspacing='0'>" )

    def link( self, bgcolor, branch, rev ):
        """Write a link to the given URL."""

        self.file.write( "<tr bgcolor='%s'>" % bgcolor )
        self.file.write( "<td><a href='%s/test.log' target='right'>%s</a></td>" % (rev,rev) )
        self.file.write( "<td align='right'><a href='https://bitbucket.org/burtonator/peregrine/changeset/%s' target='right'>CS</a></td>" % rev )
        self.file.write( "</tr>" )
        self.file.flush()

    def close(self):
        self.file.write( "</table>" )

        # FIXME: print the current date

        now = datetime.datetime.now()

        self.file.write( "<br/><center><small>%s</small></center>" % (now.strftime("%Y-%m-%d %H:%M")) )

        self.file.close()

def read_cmd(cmd, input=None, cwd=None):
    """Run the given command and read its output"""

    pipe = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, stdin=PIPE, cwd=cwd)

    out=''
    err=''

    while True:

        (_out,_err) = pipe.communicate( input )

        out += _out
        err += _err

        result = pipe.poll()
        
        if result == 0:
            return out
        elif result >= 0:
            raise Exception( "%s exited with %s" % (cmd, result) )

def run_cmd(cmd, input=None, stdout=None, stderr=None, cwd=None, fail=True):
    """Run the given command and read its output"""

    print " # %s" % cmd

    pipe = Popen(cmd, shell=True, cwd=cwd, stdout=stdout, stderr=stderr)

    (_out,_err) = pipe.communicate( input )
    result = pipe.poll()
        
    if result == 0:
        return 0
    elif result >= 0 and fail:
        raise Exception( "%s exited with %s" % (cmd, result) )

    return result

def get_active_branches():
    """Get the currently active branches we are working with and integrate here first."""

    branches = read_cmd( "hg branches -a" )

    result={}

    for line in branches.split("\n"):
        
        branch = line.split( " " )[0]

        if branch == "":
            continue
        
        result[branch]=1

    # default would need to be added.
    result[ r"default" ]=1
        
    return result.keys()

def get_change_index():
    """Return a map from branch name to revision ID by reverse chronology"""

    os.chdir( SCRATCH )

    index={}

    output=read_cmd( "hg log --template '{rev} {branches} {date}\n'" )

    for line in output.split( "\n" ):

        changectx={}

        split=line.split( " " )

        if len( split ) != 3:
            continue

        branch=split[1]

        if branch == "":
            branch = "default"

        changectx['rev']    = split[0]
        changectx['branch'] = branch
            
        changes=index.get( branch )

        if changes == None:
            changes=[]
            index[branch]=changes

        changes.append( changectx )

    return index

def get_changedir(rev):
    """Get the directory used to contain logs."""

    changedir="%s/%s/" % (TEST_LOGS, rev)

    return changedir

def test(branch,rev):
    """Run the tests on the current branch."""

    if ( isTested( rev ) ):
        print "Skipping rev %s (already tested)." % rev
        return

    print "Testing %s on branch %s" % ( rev, branch )

    os.chdir( SCRATCH )

    run_cmd( "hg update -r %s" % rev )

    # to: $changedir/test.log

    changedir=get_changedir(rev)

    if not os.path.exists( changedir ):
        os.makedirs( changedir )

    stdout=open( "%s/test.log" % (changedir), "w" )
    stderr=open( "%s/test.err" % (changedir), "w" )

    result = run_cmd( TEST_COMMAND, stdout=stdout, stderr=stderr, fail=False )

    if ( result == 0 ):
        print "SUCCESS"
    else:
        print "FAILED"

    exit_result=open( "%s/exit.result" % (changedir), "w" )
    exit_result.write( str( result ) )
    exit_result.close()

    stderr=open( "%s/test.err" % (changedir), "w" )

def isTested(rev):
    """Return true if the given rev is already tested."""

    changedir=get_changedir(rev)

    if os.path.exists( changedir ):
        
        if os.path.exists( "%s/exit.result" % (changedir) ):
            return True
        
    return False

def run(limit=LIMIT):

    if not os.path.exists( SCRATCH ):

        os.makedirs( SCRATCH )
        os.chdir( SCRATCH )
        run_cmd( "hg clone %s %s" % (REPO,SCRATCH) )

    # change to the sratch dir and hg pull -u
    os.chdir( SCRATCH )

    run_cmd( "hg pull -u" )

    active_branches=get_active_branches()

    change_index=get_change_index()

    #print "Working with active branches: %s" % active_branches
    #print "Working with change index keys: %s" % change_index.keys() 

    for branch in active_branches:

        for i in xrange(limit):
            changectx=change_index[branch][i]
            rev=changectx['rev']

            test(branch,rev)

            # regen the index.
            index(change_index)

def get_branch(change_index, rev):

    os.chdir( SCRATCH )

    return "unknown"

def index(change_index=None):
    """Write the full index of the sidebar and index.html"""

    index   = ReportIndex()
    sidebar = ReportSidebar()

    if change_index == None:
        change_index = get_change_index()

    try:

        for file in os.listdir( TEST_LOGS ):

            path = "%s/%s" % (TEST_LOGS, file)

            if os.path.isdir( path ):

                changedir=get_changedir(file)

                exit_file="%s/exit.result" % (changedir)

                if ( os.path.exists( exit_file ) ):

                    rev=file

                    exit_result=open( exit_file, "r" )
                    result=exit_result.read()
                    exit_result.close()

                    bgcolor="green"
                    
                    if result != "0": 
                        bgcolor="red"

                    branch = get_branch( change_index, rev )

                    sidebar.link( bgcolor, branch, rev )

    finally:
        
        index.close()
        sidebar.close()

if len(sys.argv) == 2 and sys.argv[1] == "--index":
    index()
    sys.exit(1)

# test the first changeset from each branch
run(1)

# test the remaining changesets.
run()

