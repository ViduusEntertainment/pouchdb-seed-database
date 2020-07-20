const _ = require('lodash');
const { v4 } = require('uuid');
const debug = require('debug')('pouchdb-seed-database');

const returnOne = result => {
	return result.rows.length > 0
		? result.rows[0].value
		: null;
}

const returnMany = result => {
	return result.rows.map(row => row.value);
}

module.exports = class Interface {
	id;
	pdb;
	indexes;
	write_roles;

	constructor(id, pdb) {
		this.id = id;
		this.pdb = pdb;
	}

	get dd_name() {
		throw new Error('implement get dd_name()')
	}

	get full_dd_name() {
		return `_design/${this.dd_name}`;
	}

	get index_dd() {
		throw new Error('implement get index_dd()')
	}

	async clear() {
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

	async updateIndexDD() {
		debug('interface - updateIndexDD');

		const old_index_dd = await this.fetch({ _id: this.full_dd_name });
		if (_.get(old_index_dd, 'meta.indexes')) {
			this.indexes = old_index_dd.meta.indexes;
		}
		if (_.get(old_index_dd, 'meta.write_roles')) {
			this.write_roles = old_index_dd.meta.write_roles;
		}

		await this.upsert(this.index_dd);
	}

	async setIndex(indexes) {
		debug('interface - setIndex', this.id, indexes);
		this.indexes = indexes;
		await this.updateIndexDD();
	}

	async setWriteRoles(write_roles) {
		debug('interface - setPermissions', this.id, write_roles);
		this.write_roles = write_roles;
		await this.updateIndexDD();
	}

	documentToKey(document) {
		if (!this.indexes)
			return null;

		let key = [];
		for (let index of this.indexes) {
			if (!document.hasOwnProperty(index))
				return null;
			key.push(document[index]);
		}
		return key;
	}

	async upsertBulk(documents) {
		debug('interface - upsertBulk', this.id, documents);

		const old_documents = await this.fetchAll();

		const key_to_old_document = Object.fromEntries(old_documents
			.map(doc => [this.documentToKey(doc), doc])
			.filter(([key, doc]) => key != null));

		const id_to_old_document = Object.fromEntries(old_documents
			.map(doc => [doc._id, doc]));

		const new_documents = documents.map(doc => {
			let old_doc;

			if (doc._id)
				old_doc = id_to_old_document[doc._id];

			if (!old_doc)
				old_doc = key_to_old_document[this.documentToKey(doc)];

			if (old_doc) {
				return _.assign({}, old_doc, doc)
			} else {
				return _.assign({ _id: v4() }, doc)
			}
		});

		const new_document_map = Object.fromEntries(new_documents
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

	async upsert(document) {
		debug('interface - upsert', this.id, document);

		const old_document = await this.fetch(document);
		if (old_document) {
			document = _.assign(old_document, document);
		} else if (document._id === undefined) {
			document._id = v4();
		}

		const result = await this.pdb.put(document);

		return await this.fetch({ _id: result.id });
	}

	async fetch(document) {
		debug('interface - fetch', this.id, document);
		
		return document._id
			? await this.fetchById(document)
			: await this.fetchByIndex(document);
	}

	async fetchById(document) {
		debug('interface - fetchById', this.id, document._id);

		try {
			return await this.pdb.get(document._id);
		} catch {
			return null;
		}
	}

	async fetchByIndex(document) {
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

	async fetchAll() {
		debug('interface - fetchAll', this.id);

		try {
			return returnMany(await this.pdb.query(`${this.dd_name}/all_docs`));
		} catch(e) {
			return [];
		}
	}
}