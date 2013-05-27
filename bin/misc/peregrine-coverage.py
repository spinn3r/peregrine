##
#
# build simple/quick peregrine web sources like javadoc/jxr, etc.

SCRATCH="/tmp/integration/coverage"

TEST_LOGS="/var/lib/integration/peregrine/coverage"

TEST_COMMAND="export ANT_OPTS=-Xmx512M && time ant coverage"

REPO="https://burtonator:redapplekittycat@bitbucket.org/burtonator/peregrine"

OUTPUT = { 'coverage' : 'target/coverage'  }
