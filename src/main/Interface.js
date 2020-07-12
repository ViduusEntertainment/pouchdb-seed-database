const _ = require('lodash');
const { v4 } = require('uuid');
const debug = require('debug')('pouchdb-seed-database');

const returnOne = result => {
	return result.rows.length > 0
		? result.rows[0]
		: null;
}

module.exports = class Interface {
	id;
	pdb;
	indexes;

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
				all_docs.rows
					.filter(row => !row.id.startsWith('_design/'))
					.map(row => _.assign({
						_id: row.value._id,
						_rev: row.value.rev,
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

		await this.upsert(this.index_dd);
	}

	async setIndex(indexes) {
		debug('interface - setIndex', this.id, indexes)
		this.indexes = indexes;
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

		return result.map(
			doc => doc.ok === true
				? _.omit(_.assignIn(doc, new_document_map[doc.id]), ['_id'])
				: null
		);
	}

	async upsert(document) {
		debug('interface - upsert', this.id, document);

		let old_document = await this.fetch(document);
		if (old_document) {
			document = _.assign({}, old_document, document);
		} else if (document._id === undefined) {
			document._id = v4();
		}

		return await this.pdb.put(document);
	}

	async fetch(document) {
		debug('interface - fetch', this.id, document)
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
			return await this.pdb.query(`${this.dd_name}/index`, {
				key: key,
				include_docs: true
			})
		} catch(e) {
			console.log(e);
			return null;
		}
	}

	async fetchAll() {
		debug('interface - fetchAll', this.id);
		// const res = await this.db.query(`indexes/primary_index`);
		const res = await this.pdb.allDocs();
		return res.rows.map(row => row.key);
	}
}