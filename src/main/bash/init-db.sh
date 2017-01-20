mongo $* << EOF
use flame
db.runCommand( { dropDatabase: 1 } );
db.entity_attributes.createIndex({ts : 1});
db.entity_attributes.createIndex({entity_id : 1});
db.entity_attributes.createIndex({attribute_name : 1});
db.entity_attributes.createIndex({type : 1, value : 1});
db.entity_attributes.createIndex({type : 1, text : "text"});
db.entities.createIndex({type:1});
db.entities.createIndex({"loc" : "2dsphere"});

EOF


