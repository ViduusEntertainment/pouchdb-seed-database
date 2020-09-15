import DatabaseInterface, { Document } from './DatabaseInterface';
import debug from './Debug';

export default class Type extends DatabaseInterface {

	constructor(name, db) {
		super(`${db.id}/_design/type_${name}`, db.pdb);
		this.name = name;
		this.type = name;
		this.indexes = ['_id'];
	}

	public get dd_name(): string {
		return `type_${this.name}`;
	}

	public async upsertBulk(documents): Promise<Document[]> {
		debug('type - upsertBulk');
		return await super.upsertBulk(
			documents.map(doc => {
				doc.type = this.name;
				return doc;
			})
		);
	}

	public async upsert(doc: Document): Promise<Document> {
		debug('type - upsert');
		doc.type = this.name;
		return await super.upsert(doc);
	}

	public async insert(doc: Document): Promise<Document> {
		debug('type - insert');
		doc.type = this.name;
		return await super.insert(doc);
	}

}