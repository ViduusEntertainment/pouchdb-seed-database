import Database, { DatabaseConnectionInfo } from "./Database";
import _ from 'lodash';

export default class DatabaseConnector {
	db_connection_info: DatabaseConnectionInfo;

	constructor(db_connection_info: DatabaseConnectionInfo) {
		this.db_connection_info = db_connection_info;
	}

	public async createDatabase(db_name: string): Promise<Database> {
		return await this.connect(db_name);
	}

	public async connect(db_name: string): Promise<Database> {
		const db: Database = new Database(db_name, _.omit(_.assign({}, this.db_connection_info, {
			name: this.db_connection_info.prefix + db_name
		}), ['prefix']));
		await db.updateIndexDD();
		return db;
	}
}