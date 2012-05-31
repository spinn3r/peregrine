#!/usr/bin/python

###
# 

###
#
# TODO
#
#
# - make the right page have some basic stats.
#
# - parse command line arguments
#    - timeout
#    - ignore-branches
#    - ignore-changesets

import datetime
import os
import re
import shutil
import subprocess
import sys
import time
import traceback

VERSION="1.0.2"

LIMIT=200

BRANCH="default"

SCRATCH="/tmp/integration/peregrine"
TEST_LOGS="/var/lib/integration/peregrine"

TEST_COMMAND="hg cat -r default build.xml > build.xml && export ANT_OPTS=-Xmx512M && ant clean test"
#TEST_COMMAND="false"

REPO="https://burtonator:redapplekittycat@bitbucket.org/burtonator/peregrine"

DAEMON_SLEEP_INTERVAL=120

IGNORE_BRANCHES={}

IGNORE_BRANCHES['burton-bench']=1
IGNORE_BRANCHES['burton-cassandra-support']=1

IGNORE_CHANGESETS={}
IGNORE_CHANGESETS['1852']=1

##
# Timeout for build commands.
TIMEOUT=30*60

class ReportIndex:

    def __init__(self):
        
        file=open( "%s/index.html" % TEST_LOGS , "w" );

        file.write( "<html>" )
        file.write( "<head><title>Integration report</title></head>" )
        file.write( "<frameset cols='30%,70%' title=''>" )
        file.write( "<frame src='left.html' name='left' title='all tests'>" )
        file.write( "<frame src='' name='right' title=''>" )
        file.write( "</frameset>" )
        file.write( "</html>" )

        file.close()
        
    def close(self):
        pass

class ReportSidebar:

    def __init__(self):

        file=open( "%s/left.html" % TEST_LOGS , "w" );

        file.write( "<html>" )

        file.write( "<head>" )
        file.write( "<style>" )
        file.write( "* { font-family: sans-serif; font-size: 12px; }" )
        file.write( "</style>" )
        
        file.write( "</head>" )
        file.write( "<body>" )
        file.write( "<table width='100%' cellspacing='0'>" )

        file.flush()
        file.close()

    def link( self, bgcolor, rev, report, log ):
        """Write a link to the given URL."""

        time = datetime.datetime.fromtimestamp( float( log['date'] ) )

        file=open( "%s/left.html" % TEST_LOGS , "a" );

        file.write( "<tr bgcolor='%s'>" % bgcolor )
        file.write( "<td><a href='%s/test.log' target='right'>%s</a></td>" % (rev,rev) )
        file.write( "<td>%s</td>" % log['branch'] )
        file.write( "<td>%s</td>" % strftime(time) )

        file.write( "<td align='right'><a href='https://bitbucket.org/burtonator/peregrine/changeset/%s' target='right'>CS</a></td>" % rev )

        if report != None:
            file.write( "<td align='right'><a href='%s' target='right'>report</a></td>" % report )
        else:
            file.write( "<td align='right'></td>" )

        file.write( "</tr>" )
        file.close()

    def close(self):

        file=open( "%s/left.html" % TEST_LOGS , "a" );

        now = datetime.datetime.now()

        file.write( "</table>" )
        file.write( "<br/><center><small>%s</small></center>" % (strftime(now)) )
        file.flush()

        file.close()

def strftime( ts ):

    return ts.strftime("%Y-%m-%d %H:%M")

def read_cmd(cmd, input=None, cwd=None):
    """Run the given command and read its output"""

    pipe = subprocess.Popen( cmd,
                             shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             cwd=cwd)

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

    pipe = subprocess.Popen(cmd, shell=True, cwd=cwd, stdout=stdout, stderr=stderr)

    (_out,_err) = pipe.communicate( input )
    result = pipe.poll()
        
    if result == 0:
        return 0
    elif result >= 0 and fail:
        raise Exception( "%s exited with %s" % (cmd, result) )

    return result

class Timeout(Exception):
    pass

def run_cmd2(command, input=None, stdout=None, stderr=None, cwd=None, fail=True, timeout=TIMEOUT):

    print " # %s" % command

    proc = subprocess.Popen( command,
                             bufsize=0,
                             stdout=stdout,
                             stderr=stderr,
                             cwd=cwd ,
                             shell=True )
    
    poll_seconds = .250
    deadline = time.time()+timeout
    while time.time() < deadline and proc.poll() == None:
        time.sleep(poll_seconds)

    if proc.poll() == None:
        proc.terminate()

        if fail:
            raise Timeout()
        else:
            return -1

    stdout, stderr = proc.communicate()

    result = proc.poll()

    if result > 0 and fail:
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

    return parse_hg_log(get_hg_log())

def get_change_index_flat():
    """Get the full HG log output."""

    return parse_hg_log_flat(get_hg_log())

def get_hg_log():
    """Get the output of 'hg log'""" 

    os.chdir( SCRATCH )

    output=read_cmd( "hg log --template '{rev} {branches} {date}\n'" )

    return output

def parse_hg_log(output):
    """Parse the HG log by branch."""

    index={}

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
        changectx['date']   = split[2]
            
        changes=index.get( branch )

        if changes == None:
            changes=[]
            index[branch]=changes

        changes.append( changectx )

    return index

def parse_hg_log_flat(output):
    """Parse the HG log by changeset ID"""
    
    index=[]

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
        changectx['date']   = split[2]

        index.append( changectx )

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

    if ( isIgnored( rev ) ):
        print "Skipping rev %s (ignored)." % rev
        return

    print "Testing %s on branch %s" % ( rev, branch )

    os.chdir( SCRATCH )

    run_cmd( "hg update -C -r %s" % rev )

    # to: $changedir/test.log

    changedir=get_changedir(rev)

    if not os.path.exists( changedir ):
        os.makedirs( changedir )

    _stdout=open( "%s/test.log" % (changedir), "w" )
    _stderr=open( "%s/test.err" % (changedir), "w" )

    result = run_cmd2( TEST_COMMAND, stdout=_stdout, stderr=_stderr, fail=False )

    if ( result == 0 ):
        print "SUCCESS"
    elif ( result > 0 ):
        print "FAILED"
    else:
        print "TIMEOUT"

    # TODO consider just making this a move.
    if os.path.exists( "%s/target/test-reports" % SCRATCH):

        dest = "%s/%s" % (changedir, "test-reports")

        print "Copying test-reports to %s" % dest
        
        shutil.copytree( "target/test-reports", dest )
    else:
        print "WARN: target/test-reports directory does not exist." 

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

def isIgnored(rev):

    return IGNORE_CHANGESETS.get( rev ) != None

def prioritize(list,depth=0):
    """

    For a given list, reprioritize it so that we index basically by a binary
    search so that we don't keep indexing the most recent changesets but spread
    out tests across ALL the revisions.

    for example with an input of:
    
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

    the output would look like:
    
    [0, 10, 5, 15, 3, 13, 8, 18, 2, 12, 7, 17, 4, 14, 9, 19, 1, 11, 6, 16]

    """
    if len(list) <= 1:
        return list

    mid = len(list) / 2

    head=list[0:mid]
    tail=list[mid:len(list)]

    result=[]

    if depth == 0:
        result.append( head.pop(0) )

    result.append( tail.pop(0) )

    head = prioritize( head, depth+1 )
    tail = prioritize( tail, depth+1 )

    end=max(len(head), len(tail))

    for i in xrange(end):

        if i < len(head):
            result.append( head[i] )

        if i < len(tail):
            result.append( tail[i] )

    return result

def run(limit=LIMIT):

    if not os.path.exists( SCRATCH ):

        os.makedirs( SCRATCH )
        os.chdir( SCRATCH )
        run_cmd( "hg clone %s %s" % (REPO,SCRATCH) )

    # change to the sratch dir and hg pull -u
    os.chdir( SCRATCH )

    # FIXME: if bitbucket is down this will fail and the script will abort when
    # in reality it's ok if this fails.

    try:
        run_cmd( "hg pull -u" )
    except:
        print "FAILED to hg pull"

    active_branches=get_active_branches()

    change_index=get_change_index()

    # reprioritize the revisions we should be testing.
    for branch in active_branches:
        change_index[branch] = prioritize(change_index[branch][0:LIMIT]);

    for i in xrange(limit):

        for branch in active_branches:

            if IGNORE_BRANCHES.get( branch ) != None:
                continue

            changes=change_index[branch]

            if len( changes ) <= i:
                continue
            
            changectx=changes[i]

            rev=changectx['rev']

            test(branch,rev)

            # regen the index.
            index()

def get_log(rev):
    """Run hg log and get the output""" 
    
    os.chdir( SCRATCH )

    output=read_cmd( "hg log -r %s --template '{rev} {branches} {date}\n'" % rev )

    parsed=parse_hg_log(output)

    key = parsed.keys()[0];

    return parsed[key][0]

def index():
    """Write the full index of the sidebar and index.html"""

    index   = ReportIndex()
    sidebar = ReportSidebar()

    try:

        changelog = get_change_index_flat()

        for change in changelog:
            
            rev = change['rev']

            path = "%s/%s" % (TEST_LOGS, rev)
            report = None

            if os.path.isdir( path ):

                changedir=get_changedir(rev)

                exit_file="%s/exit.result" % (changedir)

                if ( os.path.exists( exit_file ) ):

                    exit_result=open( exit_file, "r" )
                    result=exit_result.read()
                    exit_result.close()

                    bgcolor="green"
                    
                    if result != "0": 
                        bgcolor="red"

                    # see if the test report exists.

                    if ( os.path.exists( "%s/test-reports" % changedir ) ):
                        report="%s/%s" % ( rev, "test-reports" )

                    sidebar.link( bgcolor, rev, report, change )

            else:
                sidebar.link( "gray", rev, report, change )

    finally:
        
        index.close()
        sidebar.close()

print "integrate version %s" % VERSION

if len(sys.argv) == 2 and sys.argv[1] == "--index":
    index()
    sys.exit(1)

daemon=len(sys.argv) == 2 and sys.argv[1] == "--daemon"

while True:
    # test the first changeset from each branch
    run(1)
    
    # test the remaining changesets.
    run()

    if not daemon:
        break

    time.sleep( DAEMON_SLEEP_INTERVAL )

