import DatabaseConnector from './src/main/DatabaseConnector';
import { DatabaseConnectionInfo } from './src/main/Database';

export async function seed(db_connection_info: DatabaseConnectionInfo, func: (connection: DatabaseConnector) => void) {
	func(new DatabaseConnector(db_connection_info));
}

export async function connect(db_connection_info: DatabaseConnectionInfo, func: (connection: DatabaseConnector) => void) {
	func(new DatabaseConnector(db_connection_info));
}