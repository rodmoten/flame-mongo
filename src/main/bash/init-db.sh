mongo $* << EOF
use flame
db.runCommand( { dropDatabase: 1 } );
db.attributes.createIndex({ts : 1});
db.attributes.createIndex({entity_id : 1, attribute_name : 1});
db.attributes.createIndex({attribute_name : 1});
db.attributes.createIndex({value : 1});
db.attributes.createIndex({text : "text"});

// Index references
db.references.createIndex({ts : 1});
db.references.createIndex({entity_id : 1, attribute_name : 1});
db.references.createIndex({attribute_name : 1});
db.references.createIndex({value : 1});


// Index geos
db.geos.createIndex({entity_id : 1});
db.geos.createIndex({type : 1, value : 1});

// Index entities
db.entities.createIndex({type:1});
db.entities.createIndex({"loc" : "2dsphere"});


		
EOF
