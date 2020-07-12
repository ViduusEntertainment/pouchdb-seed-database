const Database = require('./Database');
const _ = require('lodash');

module.exports = class DatabaseConnector {
	constructor(db_connection_info) {
		this.db_connection_info = db_connection_info;
	}

	async createDatabaseWithDD(db_name, db_design) {
		const db = await this.connect(db_name);

		// clear old database if enabled
		if (db_design.clear) {
			await db.clear();
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

	async createDatabase(db_name) {
		return await this.connect(db_name);
	}

	async connect(db_name) {
		const db = new Database(db_name, _.omit(_.assign({}, this.db_connection_info, {
			name: this.db_connection_info.prefix + db_name
		}), ['prefix']));
		await db.updateIndexDD();
		return db;
	}
}