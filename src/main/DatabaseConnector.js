const Database = require('./Database');
const _ = require('lodash');

module.exports = class DatabaseConnector {
	constructor(db_connection_info) {
		this.db_connection_info = db_connection_info;
	}

	async createDatabase(db_name, db_design) {
		const db = this.connect(db_name);

		// clear old database if enabled
		if (db_design.clear) {
			await db.deleteAll();
		}

		// create index if not exists
		if (db_design.indexes) {
			await db.createIndex(db_design.indexes);
		}

		// add data if not exists
		if (db_design.data) {
			return await db.upsertBulk(db_design.data);
		}

		return [];
	}

	connect(db_name) {
		return new Database(_.omit(_.assign({}, this.db_connection_info, {
			name: this.db_connection_info.prefix + db_name
		}), ['prefix']))
	}
}