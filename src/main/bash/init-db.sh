mongo $1 << EOF
use flame
db.runCommand( { dropDatabase: 1 } );
db.attribute_names.createIndex({name : 1});
db.attribute_names.createIndex({ts : 1});
db.entities.createIndex({type:1});
db.entities.createIndex({"loc" : "2dsphere"});
db.createCollection("entity_attributes");
db.createCollection("types");
db.createCollection("locks");
db.locks.insert({_id:"attribute_names", state:false});

EOF


