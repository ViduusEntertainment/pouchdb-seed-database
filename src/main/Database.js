const PouchDB = require('./PouchDB');
const _ = require('lodash');
const { v4 } = require('uuid');
const debug = require('debug')('pouchdb-seed-database');

const connect = (db_connection_info) => {
	return new PouchDB(_.assignIn({}, db_connection_info));
}

module.exports = class Database {
	db_connection_info;
	db;
	indexes;

	constructor(db_connection_info) {
		this.db_connection_info = db_connection_info;
		this.db = connect(this.db_connection_info);
	}

	async destroy() {
		await this.db.destroy();
		this.db = connect(this.db_connection_info);
	}

	async deleteAll() {
		const all_docs = await this.db.allDocs();
		if (all_docs.rows.length > 0) {
			debug('deleteAll', this.db_connection_info.name, 'deleting', all_docs.rows.length, 'documents')
			await this.db.bulkDocs(
				all_docs.rows
					.filter(row => !row.id.startsWith('_design/'))
					.map(row => _.assign({
						_id: row.id,
						_rev: row.value.rev,
						_deleted: true
					}))
			);
		}
	}

	async createIndex(indexes) {
		debug('createIndex', this.db_connection_info.name, indexes)
		this.indexes = indexes;

		const map_fn = function (doc) {
			var every = true;
			var fields = indexes;
			for (var i=0 ; i<fields.length ; i++) {
				if (!doc.hasOwnProperty(fields[i])) {
					every = false;
				}
			}
			if (every && !doc._deleted) {
				emit(doc);
			}
		}.toString().replace('indexes', JSON.stringify(this.indexes));

		// create view to query categories of entries
		await this.db.putIfNotExists({
			_id: `_design/indexes`,
			views: {
				primary_index: {
					map: map_fn
				}
			}
		});

		// create index for find functions
		await this.db.createIndex({
			index: {
				fields: this.indexes
			}
		});
	}

	async upsertBulk(documents) {
		debug('upsertBulk', this.db_connection_info.name, documents);
		const old_documents = await this.fetchAll();
		const old_document_map = {};
	
		const document_to_unique_key = (document) => {
			let key = [];
			for (let index of this.indexes) {
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

		const new_document_map = {};

		for (let document of new_documents) {
			new_document_map[document._id] = document;
		}

		const result = await this.db.bulkDocs(new_documents);
	
		return result.map(
			doc => doc.ok === true
				? _.omit(_.assignIn(doc, new_document_map[doc.id]), [ '_id' ])
				: null
		);
	}

	async upsert(document) {
		debug('upsert', this.db_connection_info.name, document);

		if (!this.indexes.every(index => document.hasOwnProperty(index))) {
			throw new Error('can not perform a upsert without the entire index');
		}
	
		const old_document = await this.fetch(document);
		if (old_document) {
			document = _.assign({}, old_document, document);
		} else if(document._id === undefined) {
			document._id = v4();
		}
	
		return await this.db.put(document);
	}

	async fetch(document) {
		debug('fetch', this.db_connection_info.name, document)
		const selector = {};
			
		for (let index of this.indexes) {
			if (document[index] !== undefined) {
				selector[index] = {
					$eq: document[index]
				};
			}
		}
	
		const res = await this.db.find({
			selector
		});
	
		return (res.docs.length === 0) ? null : res.docs[0];
	}

	async fetchAll() {
		debug('fetchAll', this.db_connection_info.name);
		const res = await this.db.query(`indexes/primary_index`);
		return res.rows.map(row => row.key);
	}
}