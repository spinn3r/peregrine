##
#
# build simple/quick peregrine web sources like javadoc/jxr, etc.


SCRATCH="/tmp/integration/peregrine-web"

TEST_LOGS="/tmp/peregrine-web"

TEST_COMMAND="export ANT_OPTS=-Xmx512M && time ant javadoc jxr"

REPO="https://burtonator:redapplekittycat@bitbucket.org/burtonator/peregrine"

OUTPUT = { 'javadoc' : 'target/javadoc',
           'xref' : 'target/xref' }
