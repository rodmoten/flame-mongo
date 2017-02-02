function updateLoc(doc) { 
	db.entities.update({ _id: doc._id }, 
			{ $set: { loc: { type: 'Point', coordinates: [doc.longitude, doc.latitude] } } }, { upsert: false })
	}