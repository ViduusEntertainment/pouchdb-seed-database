const debug = require('debug')('pouchdb-seed-database');
const Interface = require('./Interface');

module.exports = class Type extends Interface {
	name;
	indexes;

	constructor(name, db) {
		super(`${db.id}/_design/type_${name}`, db.pdb);
		this.name = name;
	}

	get dd_name() {
		return `type_${this.name}`;
	}

	get index_dd() {
		const obj = {
			_id: `${this.full_dd_name}`,
			meta: {},
			views: {
				all_docs: {
					map: function (doc) {
							if (!doc._deleted && doc.type === type_name) {
								emit(doc._id, doc);
							}
						}.toString()
						.replace('type_name', JSON.stringify(this.name))
				}
			}
		};

		if (this.indexes) {
			obj.meta.indexes = this.indexes;
			obj.views.index = {
				map: function (doc) {
					function by_string(o, s) {
						s = s.replace(/\[(\w+)\]/g, '.$1');
						s = s.replace(/^\./, '');
						var a = s.split('.');
						for (var j = 0, n = a.length; j < n; ++j) {
							var k = a[j];
							if (k in o) {
								o = o[k];
							} else {
								return;
							}
						}
						return o;
					}

					var every = true;
					var key = [];
					var fields = indexes;
					for (var i = 0; every && i < fields.length; i++) {
						var value = by_string(doc, fields[i]);
						if (!value) {
							every = false;
						}
						key.push(value);
					}
					if (every && !doc._deleted && doc.type === type_name) {
						emit(key, doc);
					}
				}.toString()
					.replace('indexes', JSON.stringify(this.indexes))
					.replace('type_name', JSON.stringify(this.name))
			};
		}

		if (this.write_roles) {
			obj.meta.write_roles = this.write_roles;
		}

		return obj;
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