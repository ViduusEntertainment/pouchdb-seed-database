import DatabaseInterface from './DatabaseInterface';
import Type from './Type';
import debug from './Debug';
import axios from 'axios';
import _ from 'lodash';
import PouchDB from './PouchDB';
import { URL } from 'url';

function connect(db_connection_info: DatabaseConnectionInfo): PouchDB {
	return new PouchDB(_.assignIn({}, db_connection_info));
}

export interface DatabaseConnectionInfo {
	prefix: string,
	name: string,
}

export interface DatabaseSecurityGroup {
	members?: string[]
	roles?: string[]
}

export default class Database extends DatabaseInterface {
	db_connection_info: DatabaseConnectionInfo;
	admins: DatabaseSecurityGroup;
	members: DatabaseSecurityGroup;

	constructor(db_name: string, db_connection_info: DatabaseConnectionInfo) {
		super(`${db_connection_info.name}`, connect(db_connection_info));
		this.db_connection_info = db_connection_info;
		this.name = db_name;
	}

	public get dd_name(): string {
		return `global`;
	}

	public async updateSecurityDD() {
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

	async setReadRoles(member_roles: string[] = [], admin_roles: string[] = []) {
		debug('database - setSecurity', this.id, member_roles, admin_roles);
		this.members = {
			roles: member_roles
		};
		this.admins = {
			roles: admin_roles
		};

		await this.updateSecurityDD();
	}

	public async destroy(): Promise<void> {
		debug('database - destroy');
		await this.pdb.destroy();
		this.pdb = connect(this.db_connection_info);
		await this.updateIndexDD();
	}

	public async createType(type_name: string): Promise<Type> {
		debug('database - createType', type_name);
		const type = new Type(type_name, this);
		await type.updateIndexDD();
		return type;
	}
}