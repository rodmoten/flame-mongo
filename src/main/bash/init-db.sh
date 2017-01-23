mongo $* << EOF
use flame
db.runCommand( { dropDatabase: 1 } );
db.entity_attributes.createIndex({ts : 1});
db.entities.createIndex({"loc" : "2dsphere"});
db.entities.createIndex({type:1});

EOF
