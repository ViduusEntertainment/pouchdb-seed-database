const PouchDB = require('./PouchDB');
const Type = require('./Type');
const _ = require('lodash');
const { v4 } = require('uuid');
const debug = require('debug')('pouchdb-seed-database');
const Interface = require('./Interface');
const axios = require('axios');

const connect = (db_connection_info) => {
	return new PouchDB(_.assignIn({}, db_connection_info));
}

module.exports = class Database extends Interface {
	// db_connection_info;
	// indexes;
	// name;

	constructor(db_name, db_connection_info) {
		super(`${db_connection_info.name}`, connect(db_connection_info));
		this.db_connection_info = db_connection_info;
		this.name = db_name;
	}

	get dd_name() {
		return `global`;
	}

	async updateSecurityDD() {
		debug('database - updateSecurityDD');

		const url = new URL(this.pdb.name + '/_security');
		url.username = this.pdb.__opts.auth.username;
		url.password = this.pdb.__opts.auth.password;
		
		const doc = _.merge({
			admins: {
				names: [],
				roles: []
			},
			members: {
				names: [],
				roles: []
			}
		}, {
			admins: this.admins,
			members: this.members
		});

		if (doc.admins.roles.indexOf('_admin') === -1) {
			doc.admins.roles.push('_admin');
		}

		if (doc.members.roles.indexOf('_admin') === -1) {
			doc.members.roles.push('_admin');
		}

		await axios.put(url.href, doc);
	}

	async setReadRoles(member_roles = [], admin_roles = []) {
		debug('database - setSecurity', this.id, member_roles, admin_roles);
		this.members = {
			roles: member_roles
		};
		this.admins = {
			roles: admin_roles
		};

		await this.updateSecurityDD();
	}

	async destroy() {
		debug('database - destroy');
		await this.pdb.destroy();
		this.pdb = connect(this.db_connection_info);
		await this.updateIndexDD();
	}

	async createType(type_name) {
		debug('database - createType', type_name);
		const type = new Type(type_name, this);
		await type.updateIndexDD();
		return type;
	}
}