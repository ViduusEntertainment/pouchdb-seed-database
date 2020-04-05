const PouchDB = require('pouchdb');
const PouchFind = require('pouchdb-find');
const PouchUpsert = require('pouchdb-upsert');
const PouchDesign = require('pouchdb-design');
const _ = require('lodash');
const { v4 } = require('uuid');
const debug = require('debug')('pouchdb-seed-database');

PouchDB.plugin(PouchFind);
PouchDB.plugin(PouchUpsert);
PouchDB.plugin(PouchDesign);

const createIndex = async (db, indexes) => {
	debug('createIndex', indexes)

	const map_fn = function (doc) {
		var every = true;
		var fields = indexes;
		for (var i=0 ; i<fields.length ; i++) {
			if (!doc.hasOwnProperty(fields[i])) {
				every = false;
			}
		}
		if (every) {
			emit(doc);
		}
	}.toString().replace('indexes', JSON.stringify(indexes));

	// create view to query categories of entries
	await db.putIfNotExists({
		_id: `_design/indexes`,
		views: {
			primary_index: {
				map: map_fn
			}
		}
	});

	// create index for find functions
	await db.createIndex({
		index: {
			fields: indexes
		}
	});
}

const upsertBulk = async (db, indexes, documents) => {
	const old_documents = await fetchAll(db);
	const old_document_map = {};

	const document_to_unique_key = (document) => {
		let key = [];
		for (let index of indexes) {
			if (!document.hasOwnProperty(index))
				return null;
			key.push(document[index]);
		}
		return key;
	}

	for (let document of old_documents) {
		const key = document_to_unique_key(document);
		if (key) {
			old_document_map[key] = document;
		}
	}

	const new_documents = documents.map(document => {
		const key = document_to_unique_key(document);
		const old_document = old_document_map[key];
		if (old_document) {
			return _.assign({}, old_document, document)
		} else {
			return _.assign({ _id: v4() }, document)
		}
	});

	await db.bulkDocs(new_documents);
}

const upsert = async (db, indexes, document) => {
	debug('upsert', document)
	if (!indexes.every(index => document.hasOwnProperty(index))) {
		throw new Error('can not perform a upsert without the entire index');
	}

	const old_document = await fetch(db, indexes, document);
	if (old_document) {
		document = _.assign({}, old_document, document);
	} else if(document._id === undefined) {
		document._id = v4();
	}

	await db.put(document);
}

const fetch = async (db, indexes, document) => {
	debug('fetch', document)
	const selector = {};
		
	for (let index of indexes) {
		if (document[index] !== undefined) {
			selector[index] = {
				$eq: document[index]
			};
		}
	}

	const res = await db.find({
		selector
	});

	return (res.docs.length === 0) ? null : res.docs[0];
}

const fetchAll = async (db) => {
	const res = await db.query(`indexes/primary_index`);
	return res.rows.map(row => row.key);
}

/**
 * 
 */
module.exports.seed = async (db_connection_info, design_doc) => {
	try {
		await Promise.all(Object.entries(design_doc).map(async ([db_name, db_design]) => {
			const db_connect = () => new PouchDB(_.assign({}, db_connection_info, {
				name: db_connection_info.name_prefix + db_name
			}));
	
			// clear old database if enabled
			if (db_design.clear) {
				await db_connect().destroy();
			}
	
			// create/connect to database
			const db = db_connect();
	
			// create index if not exists
			await createIndex(db, db_design.indexes);
	
			// add data if not exists
			await upsertBulk(db, db_design.indexes, db_design.data);
		}))
	} catch (e) {
		console.error(e);
	}
}