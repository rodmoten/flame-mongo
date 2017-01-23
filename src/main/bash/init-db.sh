mongo $* << EOF
use flame
db.runCommand( { dropDatabase: 1 } );

EOF
