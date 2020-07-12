const PouchDB = require('./PouchDB');
const Type = require('./Type');
const _ = require('lodash');
const { v4 } = require('uuid');
const debug = require('debug')('pouchdb-seed-database');
const Interface = require('./Interface');

const connect = (db_connection_info) => {
	return new PouchDB(_.assignIn({}, db_connection_info));
}

module.exports = class Database extends Interface {
	db_connection_info;
	indexes;
	name;

	constructor(db_name, db_connection_info) {
		super(`${db_connection_info.name}`, connect(db_connection_info));
		this.db_connection_info = db_connection_info;
		this.name = db_name;
	}

	get dd_name() {
		return `global`;
	}

	get index_dd() {
		const obj = {
			_id: `${this.full_dd_name}`,
			meta: {},
			views: {
				all_docs: {
					map: function (doc) {
						if (!doc._deleted) {
							emit(doc.id, doc);
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
					var every = true;
					var key = [];
					var fields = indexes;
					for (var i = 0; i < fields.length; i++) {
						if (!doc.hasOwnProperty(fields[i])) {
							every = false;
							key.push(doc[fields[i]]);
						}
					}
					if (every && !doc._deleted) {
						emit(key, doc);
					}
				}.toString()
					.replace('indexes', JSON.stringify(this.indexes))
					.replace('type_name', JSON.stringify(this.name))
			};
		}

		return obj;
	}

	async destroy() {
		debug('database - destroy');
		await this.pdb.destroy();
		this.pdb = connect(this.db_connection_info);
	}

	async createType(type_name) {
		debug('database - createType', type_name);
		const type = new Type(type_name, this);
		await type.updateIndexDD();
		return type;
	}
}