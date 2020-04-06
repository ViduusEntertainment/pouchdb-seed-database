const PouchDB = require('pouchdb');
const PouchFind = require('pouchdb-find');
const PouchUpsert = require('pouchdb-upsert');
const PouchDesign = require('pouchdb-design');

PouchDB.plugin(PouchFind);
PouchDB.plugin(PouchUpsert);
PouchDB.plugin(PouchDesign);

module.exports = PouchDB;