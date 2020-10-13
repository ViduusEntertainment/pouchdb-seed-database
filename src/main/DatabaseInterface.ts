import _ from 'lodash';
import { v4 } from 'uuid';
import PouchDB from 'pouchdb';
import debug from './Debug';

const emit = (... args: any) => {};

function returnOne<T>(result): T {
	return result.rows.length > 0
		? result.rows[0].value
		: null;
}

function returnMany<T>(result): T[] {
	return result.rows.map(row => row.value);
}

export type DocumentId = string;

export type DocumentIndex = (string | number | symbol)[];

export interface Document {
	_id?: DocumentId;
	_rev?: string;
	_deleted?: boolean;
	type?: string;
}

export interface DesignDocument extends Document {
	meta: any;
	views: {
		all_docs: any,
		index?: any
	};
	validate_doc_update: any;
}

export default class DatabaseInterface {
	name: string;
	id: string;
	pdb: PouchDB;
	indexes: string[];
	write_roles: string[];
	type: string;
	_push_indexes: boolean;

	constructor(id: string, pdb: PouchDB) {
		this.id = id;
		this.pdb = pdb;
	}

	public get require_type_check(): boolean {
		return this.type !== undefined;
	}

	public get dd_name(): string {
		throw new Error('implement get dd_name()')
	}

	public get full_dd_name(): string {
		return `_design/${this.dd_name}`;
	}

	protected get index_dd(): DesignDocument {
		const obj: DesignDocument = {
			_id: `${this.full_dd_name}`,
			meta: {
				indexes: this.indexes,
				write_roles: this.write_roles
			},
			views: {
				all_docs: {
					map: function(doc) {
							if (!doc._deleted && (!this.require_type_check || doc.type === this.type)) {
								emit(doc._id, doc);
							}
						}.toString()
							.replace('this.require_type_check', JSON.stringify(this.require_type_check))
							.replace('this.type', JSON.stringify(this.type))
				}
			},
			validate_doc_update: function(new_doc, old_doc, user, sec) {
				// admin edge case
				if (user.roles.indexOf('_admin') !== -1) {
					return;
				}
	
				// restrict document type changes
				if (old_doc && old_doc.type !== new_doc.type) {
					throw({ forbidden: 'You do not have permission to change document type' });
				}
	
				// type check
				if (this.require_type_check && new_doc.type !== this.type) {
					return;
				}
	
				// roles check
				var valid_roles = this.write_roles;
				if (valid_roles != undefined) {
					for (var i=0 ; i<valid_roles.length ; i++) {
						if (user.roles.indexOf(valid_roles[i]) !== -1) {
							return;
						}
					}
					throw({ forbidden: 'You do not have write permissions (dd: this.full_dd_name) (user: ' + JSON.stringify(user.roles) + ') (req: ' + valid_roles + ')' });
				}
			}.toString()
				.replace('this.write_roles', JSON.stringify(this.write_roles))
				.replace('this.require_type_check', JSON.stringify(this.require_type_check))
				.replace('this.type', JSON.stringify(this.type))
				.replace('this.full_dd_name', JSON.stringify(this.full_dd_name))
		};

		if (this.indexes) {
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
					var fields = this.indexes;
					for (var i = 0; every && i < fields.length; i++) {
						var value = by_string(doc, fields[i]);
						if (!value) {
							every = false;
						}
						key.push(value);
					}
					if (every && !doc._deleted && (!this.require_type_check || doc.type === this.type)) {
						emit(key, doc);
					}
				}.toString()
					.replace('this.indexes', JSON.stringify(this.indexes))
					.replace('this.type', JSON.stringify(this.type))
					.replace('this.require_type_check', JSON.stringify(this.require_type_check))
			};
		}

		return obj;
	}

	/**
	 * Clears all documents returned by this interface. This does not delete the document from
	 * CouchDB but merely flags the document as deleted using the _deleted field.
	 */
	public async clear(): Promise<void> {
		debug('interface - clear');
		const all_docs = await this.fetchAll();
		if (all_docs.length > 0) {
			debug('interface - clear', this.pdb.name, 'deleting', all_docs.length, 'documents')
			await this.pdb.bulkDocs(
				all_docs
					// .filter(row => !row._id.startsWith('_design/'))
					.map(row => _.assign({
						_id: row._id,
						_rev: row._rev,
						_deleted: true
					}))
			);
		}
	}

	/**
	 * This function will update the index design document for this interface. This design document
	 * is used to entire uniqueness across the interface.
	 */
	public async updateIndexDD(): Promise<void> {
		debug('interface - updateIndexDD');

		const old_index_dd: DesignDocument = await this.fetch({ _id: this.full_dd_name });
		if (!this._push_indexes && _.get(old_index_dd, 'meta.indexes')) {
			this.indexes = old_index_dd.meta.indexes;
		}
		if (!this._push_indexes && _.get(old_index_dd, 'meta.write_roles')) {
			this.write_roles = old_index_dd.meta.write_roles;
		}

		if (this._push_indexes) {
			await this.upsert(this.index_dd);
			this._push_indexes = false;
		}
	}

	public async setIndex(indexes: string[]): Promise<void> {
		debug('interface - setIndex', this.id, indexes);
		this._push_indexes = true;
		this.indexes = indexes;
		await this.updateIndexDD();
	}

	public async setWriteRoles(write_roles: string[]): Promise<void>  {
		debug('interface - setPermissions', this.id, write_roles);
		this.write_roles = write_roles;
		await this.updateIndexDD();
	}

	protected documentToKey(document: Document): DocumentIndex {
		if (!this.indexes)
			return null;

		let key: DocumentIndex = [];
		for (let index of this.indexes) {
			if (!document.hasOwnProperty(index))
				return null;
			key.push(document[index]);
		}
		return key;
	}

	protected documentToSearchKey(document: Document): [any[], any[]] {
		if (!this.indexes)
			return [[null], [{}]];

		let start_key = [];
		let end_key = [];

		for (let index of this.indexes) {
			if (document.hasOwnProperty(index)) {
				start_key.push(document[index]);
				end_key.push(document[index]);
			} else {
				start_key.push(null);
				end_key.push({});
				break;
			}
		}

		return [start_key, end_key];
	}

	public async upsertBulk(documents: Document[]): Promise<Document[]> {
		debug('interface - upsertBulk', this.id, documents);

		const old_documents: Document[] = await this.fetchAll();

		const key_to_old_document: Record<string, Document> = Object.fromEntries(old_documents
			.map((doc) : [DocumentIndex, Document] => [this.documentToKey(doc), doc])
			.filter(([key, doc]) => key != null)
			.map(([key, doc]) => [key.toString(), doc]));

		const id_to_old_document: Record<DocumentId, Document>  = Object.fromEntries(old_documents
			.map(doc => [doc._id, doc]));

		const new_documents: Document[] = documents.map(doc => {
			let old_doc;

			if (doc._id)
				old_doc = id_to_old_document[doc._id];

			if (!old_doc) {
				const key = this.documentToKey(doc);
				if (key) {
					old_doc = key_to_old_document[key.toString()];
				}
			}

			if (old_doc) {
				return _.assign({}, old_doc, doc)
			} else {
				return _.assign({ _id: v4() }, doc)
			}
		});

		const new_document_map: Record<DocumentId, Document> = Object.fromEntries(new_documents
			.map(doc => [doc._id, doc]));

		const result = await this.pdb.bulkDocs(new_documents);

		return result.map(doc => doc.ok === true
				? _.omit(_.assignIn(doc, new_document_map[doc.id], {
					_id: doc.id,
					_rev: doc.rev
				}), ['id', 'rev', 'ok'])
				: null
		);
	}

	async insert(document: Document): Promise<Document> {
		debug('interface - insert', this.id, document);

		if (document._id === undefined) {
			document._id = v4();
		}

		const result = await this.pdb.put(document);

		return await this.fetch({ _id: result.id });
	}

	async upsert(document: Document): Promise<Document> {
		debug('interface - upsert', this.id);

		const old_document = await this.fetch(document);
		if (old_document) {
			document = _.assign(old_document, document);
		}

		return await this.insert(document);
	}

	public async delete(document: Document): Promise<Document> {
		debug('interface - delete', this.id);

		document._deleted = true;

		return this.upsert(document);
	}

	public async deleteBulk(documents: Document[]): Promise<Document[]> {
		debug('interface - deleteBulk', this.type, documents.length);

		documents.forEach(document => document._deleted = true);

		return this.upsertBulk(documents);
	}

	public async fetch<T extends Document>(document: Document): Promise<T> {
		debug('interface - fetch', this.id);
		
		return document._id
			? await this.fetchById(document)
			: await this.fetchByIndex(document);
	}

	public async fetchById<T extends Document>(document: Document): Promise<T> {
		debug('interface - fetchById', this.id, document._id);

		try {
			return await this.pdb.get(document._id);
		} catch {
			return null;
		}
	}

	async fetchByIndex<T extends Document>(document: Document): Promise<T> {
		const key = this.documentToKey(document);
		debug('interface - fetchByIndex', this.id, key);

		if (!this.indexes)
			return null;
		
		try {
			return returnOne(await this.pdb.query(`${this.dd_name}/index`, {
				key: key
			}));
		} catch(e) {
			return null;
		}
	}

	async fetchAllByPartialIndex(document: Document): Promise<Document[]> {
		debug('interface - fetchAllByPartialIndex', this.id);

		try {
			const [startkey, endkey] = this.documentToSearchKey(document);
			return returnMany(await this.pdb.query(`${this.dd_name}/index`, {
				startkey,
				endkey
			}));
		} catch(e) {
			return [];
		}
	}

	async fetchAll(): Promise<Document[]> {
		debug('interface - fetchAll', this.id);

		try {
			return returnMany(await this.pdb.query(`${this.dd_name}/all_docs`));
		} catch(e) {
			return [];
		}
	}
}