const DatabaseConnector = require('./src/main/DatabaseConnector');

module.exports.seed = async (db_connection_info, func) => {
	func(new DatabaseConnector(db_connection_info));
}

module.exports.connect = async (db_connection_info, func) => {
	func(new DatabaseConnector(db_connection_info));
}