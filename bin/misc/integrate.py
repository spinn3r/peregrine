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
#
# - if we timeout we should STILL run tast-report , etc.

import datetime
import os
import re
import shutil
import subprocess
import sys
import time
import traceback

VERSION="1.0.9"

LIMIT=50

BRANCH="default"

SCRATCH="/tmp/integration/peregrine"
TEST_LOGS="/var/lib/integration/peregrine"

TEST_COMMAND="pkill -9 -u nobody java 2> /dev/null ; hg cat -r default build.xml > build.xml && export HOSTNAME=localhost && export ANT_OPTS=-Xmx512M && ant clean jar compile.test jar && time ant javadoc jxr && time ant test"
#TEST_COMMAND="false"

###
# specify a post command to use AFTER we run our test.
POST_COMMAND="export ANT_OPTS=-Xmx512M && ant test-report"

REPO="https://burtonator:redapplekittycat@bitbucket.org/burtonator/peregrine"

DAEMON_SLEEP_INTERVAL=240

IGNORE_BRANCHES={}

IGNORE_BRANCHES['burton-bench']=1
IGNORE_BRANCHES['burton-cassandra-support']=1

IGNORE_CHANGESETS={}
IGNORE_CHANGESETS['1852']=1

##
# Timeout for build commands (in seconds)
TIMEOUT=1*60*60

OLD_AGE=3 * 7 * 24 * 60 * 60

class ReportIndex:

    def __init__(self):

        file=open( "%s/index.html" % TEST_LOGS , "w" );

        file.write( "<html>\n" )

        file.write( "<head>\n" )
        file.write( "<title>Integration report</title>\n" )
        file.write( "<meta http-equiv='cache-control' content='no-cache'>\n" )
        file.write( "</head>\n" )

        file.write( "<frameset cols='500,100%' title=''>\n" )
        file.write( "<frame src='left.html' name='left' title='all tests'>\n" )
        file.write( "<frame src='' name='right' title=''>\n" )
        file.write( "</frameset>\n" )
        file.write( "</html>\n" )

        file.close()
        
    def close(self):
        pass

class ReportSidebar:

    def __init__(self):

        path="%s/left.html" % TEST_LOGS

        print "Going to write sidebar: %s" % path

        file=open( path , "w" );

        file.write( "<html>\n" )

        file.write( "<head>\n" )
        file.write( "<style>\n" )
        file.write( "* { font-family: sans-serif; font-size: 12px; }\n" )
        file.write( "</style>\n" )

        file.write( "<meta http-equiv='cache-control' content='no-cache'>\n" )

        file.write( "</head>\n" )
        file.write( "<body>\n" )

        file.write( "<table width='100%' cellspacing='0'>\n" )

        file.flush()
        file.close()

    def link( self, bgcolor, rev, report, log, coverage ):
        """Write a link to the given URL."""

        time = datetime.datetime.fromtimestamp( float( log['date'] ) )

        file=open( "%s/left.html" % TEST_LOGS , "a" );

        file.write( "<tr bgcolor='%s'>\n" % bgcolor )
        file.write( "<td nowrap><a href='%s/test.log' target='right'>%s</a></td>\n" % (rev,rev) )
        file.write( "<td nowrap>%s</td>\n" % log['branch'] )
        file.write( "<td nowrap>%s</td>\n" % strftime(time) )

        file.write( "<td align='right'><a href='https://bitbucket.org/burtonator/peregrine/changeset/%s' target='right'>CS</a></td>\n" % rev )

        if report != None:
            file.write( "<td align='right'><a href='%s' target='right'>report</a></td>\n" % report )
        else:
            file.write( "<td align='right'></td>\n" )

        if coverage != None:
            file.write( "<td align='right'><a href='%s' target='right'>coverage</a></td>\n" % coverage )
        else:
            file.write( "<td align='right'></td>\n" )

        file.write( "</tr>\n" )
        file.close()

    def close(self):

        file=open( "%s/left.html" % TEST_LOGS , "a" );

        now = datetime.datetime.now()

        file.write( "</table>\n" )
        file.write( "<br/><center><small>%s</small></center>\n" % (strftime(now)) )
        file.write( "</body>\n" )
        file.write( "</html>\n" )

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

    # run the post command so we can get coverage report , test reports, etc.
    run_cmd2( POST_COMMAND, stdout=_stdout, stderr=_stderr, fail=False )

    # TODO consider just making this a move.
    if os.path.exists( "%s/target/test-reports" % SCRATCH):

        dest = "%s/%s" % (changedir, "test-reports")

        print "Copying test-reports to %s" % dest
        
        shutil.copytree( "target/test-reports", dest )
    else:
        print "WARN: target/test-reports directory does not exist." 

    #FIXME: make this a function with the above... 
    if os.path.exists( "%s/target/coverage" % SCRATCH):

        dest = "%s/%s" % (changedir, "coverage")

        print "Copying coverage to %s" % dest
        
        shutil.copytree( "target/coverage", dest )
    else:
        print "WARN: target/coverage directory does not exist." 

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

def isOld(date):

    return (time.time() - OLD_AGE) > float(date)

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
            date=changectx['date']

            if ( isTested( rev ) ):
                continue

            if ( isIgnored( rev ) ):
                continue

            if ( isOld( date ) ):
                print "Skipping rev %s (old)." % rev
                continue

            # regen the HTML index... do this BEFORE we run a test so that we
            # know that it has pulled the most recent version and is running.

            # TODO: we should ALSO have a flag/color indicating that something
            # is being integrated.
            
            simulate=False

            if not simulate: 
            
                index(int(rev))

                test(branch,rev)

                # regen the HTML index.
                index()
            else:
                print "%s:%s:%s" % (branch, rev, date)

def get_log(rev):
    """Run hg log and get the output""" 
    
    os.chdir( SCRATCH )

    output=read_cmd( "hg log -r %s --template '{rev} {branches} {date}\n'" % rev )

    parsed=parse_hg_log(output)

    key = parsed.keys()[0];

    return parsed[key][0]

def index(current=None):
    """Write the full index of the sidebar and index.html"""

    index   = ReportIndex()
    sidebar = ReportSidebar()

    try:

        changelog = get_change_index_flat()

        for change in changelog:
            
            rev = change['rev']

            path = "%s/%s" % (TEST_LOGS, rev)
            report = None
            coverage = None
            javadoc = None
            
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

                    if ( os.path.exists( "%s/coverage" % changedir ) ):
                        coverage="%s/%s" % ( rev, "coverage" )

                    if ( os.path.exists( "%s/javadoc" % changedir ) ):
                        javadoc="%s/%s" % ( rev, "javadoc" )

                    sidebar.link( bgcolor, rev, report, change, coverage )

            else:

                bgcolor="gray"
                
                if int(rev) == current:
                    bgcolor="lightblue"
                
                sidebar.link( bgcolor, rev, report, change, coverage )

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

    print "Sleeping for %s" % DAEMON_SLEEP_INTERVAL
    time.sleep( DAEMON_SLEEP_INTERVAL )

