const debug = require('debug')('pouchdb-seed-database');
const Interface = require('./Interface');

module.exports = class Type extends Interface {
	// name;
	// indexes;

	constructor(name, db) {
		super(`${db.id}/_design/type_${name}`, db.pdb);
		this.name = name;
		this.type = name;
		this.indexes = ['_id'];
	}

	get dd_name() {
		return `type_${this.name}`;
	}

	async upsertBulk(documents) {
		debug('type - upsertBulk');
		return await super.upsertBulk(
			documents.map(doc => {
				doc.type = this.name;
				return doc;
			})
		);
	}

	async upsert(doc) {
		debug('type - upsert');
		doc.type = this.name;
		return await super.upsert(doc);
	}

}