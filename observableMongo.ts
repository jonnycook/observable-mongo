import _ from 'lodash';
import sift from 'sift';
import { ObservableDBDriver } from './ObservableDBDriver';

function firstPart(prop) {
  return prop.split('.')[0];
}

function buildMeta(documents) {
  const meta = {};
  for (const doc of documents) {
    if (doc._meta) for (const prop in doc._meta) {
      meta[`${doc._id}.${prop}`] = doc._meta[prop];
    }
  }
  return meta;
}

function metaOperations(update, client) {
  const meta = {};
  for (const prop in update) {
    if (prop.startsWith('$')) {
      if (prop == '$set') {
        for (const key in update[prop]) {
          meta[firstPart(key)] = { client, timestamp: new Date() };
        }
      }
      else if (prop == '$pull') {
        for (const key in update[prop]) {
          meta[firstPart(key)] = { client, timestamp: new Date() };
        }
      }
      else if (prop == '$push') {
        for (const key in update[prop]) {
          meta[firstPart(key)] = { client, timestamp: new Date() };
        }
      }
      else if (prop == '$unset') {
        for (const key in update[prop]) {
          meta[firstPart(key)] = { client, timestamp: new Date() };
        }
      }
    }
    else if (prop != '_id') {
      meta[firstPart(prop)] = {
        modified: { client, timestamp: new Date() }
      }
    }
  }

  return meta;
}

function convertTo$Set(meta) {
  const newMeta = {};
  for (const key in meta) {
    newMeta[`_meta.${key}`] = meta[key];
  }
  return newMeta;
}

function addMetaOperations(update, client) {
  const metaOps = convertTo$Set(metaOperations(update, client));
  if (update.$set) {
    _.merge(update.$set, metaOps);
  }
  else {
    update.$set = metaOps;
  }

  return update;
}


export async function handleClientPush(db, payload, user?) {
  const mutations = payload.mutation ? (_.isArray(payload.mutation) ? payload.mutation : [ payload.mutation ]) : payload.mutations;
  const collection = db.collection(payload.collection);

  for (const mutation of mutations) {
    try {
      let originalPath;
      let prev;
      if (mutation.path) {
        originalPath = mutation.path.clone();
        let doc = await collection.find({ _id: payload._id });
        let obj = doc;
        for (let i = 0; i < mutation.path.length; ++ i) {
          let comp = mutation.path[i];  
          if (comp[0] == '&') {
            let id = comp.substr(1);
            let index = obj.findIndex((el) => el._id == id);
            if (index == -1) {
              throw new Error(`Can't find key ${id} in ${originalPath.join('.')}`);
            }
            mutation.path[i] = index;
            prev = obj = obj[index];
          }
          else {
            prev = obj = obj?.[comp] || {};
          }
        }
      }
    
      if (mutation.type == 'set') {
        await collection.updateOne({ _id: payload._id }, {
          // $push: {
          //   ['_history.' + originalPath.join('.') + '._']: {
          //     operation: 'set',
          //     value: prev,
          //     timestamp: new Date(),
          //     user: user
          //   }
          // },
          $set: {
            [mutation.path.join('.')]: mutation.value
          }
        }, { upsert: true });
        
      }
      else if (mutation.type == 'unset') {
        await collection.updateOne({ _id: payload._id }, {
          // $push: {
          //   ['_history.' + originalPath.join('.') + '._']: {
          //     operation: 'unset',
          //     value: prev,
          //     timestamp: new Date(),
          //     user: user
          //   }
          // },
          $unset: {
            [mutation.path.join('.')]: ''
          }
        }, { upsert: true });
        
      }
      else if (mutation.type == 'remove') {
        let deleteKey = Math.random();
    
        if (mutation.key) {
          await collection.updateOne({ _id: payload._id }, {
            // $push: {
            //   ['_history.' + originalPath.concat('&' + mutation.key).join('.') + '._']: {
            //     operation: 'remove',
            //     value: prev.find((el) => el._id == mutation.key),
            //     timestamp: new Date(),
            //     user: user
            //   }
            // },
            $pull: { [mutation.path.join('.')]: { _id: mutation.key } }
          });
        }
        else {
          await collection.updateOne({ _id: payload._id }, {
            $set: { [mutation.path.concat(mutation.index).join('.')]: deleteKey }
          });
          await collection.updateOne({ _id: payload._id }, {
            $pull: { [mutation.path.join('.')]: deleteKey }
          });
        }
      }
      else if (mutation.type == 'insert') {
        let path = mutation.path.slice(0, -1);
        let index = mutation.path[mutation.path.length - 1];
        await collection.updateOne({ _id: payload._id }, {
          $push: {
            // ['_history.' + originalPath.slice(0, -1).concat('&' + mutation.el._id).join('.') + '._']: {
            //   operation: 'insert',
            //   value: null,
            //   timestamp: new Date(),
            //   user: user
            // },
            [path.join('.')]: {
              $each: [ mutation.el ],
              $position: index
            }
          }
        });
      }
      else if (mutation.type == 'create') {
        mutation.document._created = { timestamp: new Date(), user };
        await collection.insertOne(mutation.document);
        
      }
      else if (mutation.type == 'delete') {
        await collection.updateOne({ _id: payload._id }, { $set: {_deleted: { timestamp: new Date(), user }} });
      }
    
    }
    catch (e) {
      console.log('Mutation failed', mutation, e);
      throw e;
    }

  }

  return true;
}

function findObservers(collection, document): {
  observer
  tag

}[] {
  let observers = [];
  for (const key in registry) {
    const r = registry[key];
    if (r.collectionName == collection) {
      if (r.compiledQuery(document)) {
        observers = observers.concat(_observers[r.id] || [])
      }
    }
  }

  return _.uniq(observers);
}

const methods = {
  async insertOne(db, collection, document, client) {
    const observers = findObservers(collection, document);
    for (const {observer: obs} of observers) {
      obs(obs);
    }

    return db.collection(collection).insertOne({ ...document, _meta: metaOperations(document, client) });
  },
  async deleteOne(db, collection, query) {
    const beforeDocument = await db.collection(collection).findOne(query);
    await db.collection(collection).deleteOne(query);

    const observers = findObservers(collection, beforeDocument);

    for (const {observer: obs} of observers) {
      obs(obs);
    }
  },
  async updateOne(db, collection, query, update, client) {
    const beforeDocument = await db.collection(collection).findOne(query);
    await db.collection(collection).updateOne(query, addMetaOperations(update, client));
    const afterDocument = await db.collection(collection).findOne(query);

    const observers = _.uniq(findObservers(collection, beforeDocument).concat(findObservers(collection, afterDocument)));

    for (const {observer: obs} of observers) {
      obs(obs);
    }
  },
  async push(db, collection, payload, client) {
    return await handleClientPush({
      collection(collection) {
        return {
          find(query) {
            return db.collection(collection).findOne(query);
          },
          updateOne(query, update) {
            return methods.updateOne(db, collection, query, update, client);
          },
          insertOne(document) {
            return methods.insertOne(db, collection, document, client);
          },
        }
      }
    }, payload);
  },
};

const registry = {};

let _observers: {
  [id: string]: { observer, tag }[]
}

let nextId = 1;

export function createMongoDriver(db): ObservableDBDriver {
  return {
    init(observers) {
      _observers = observers;
    },
    call({ client }, args: [ string, string, any ][]) {
      return Promise.all(args.map(([ collection, method, args ]) => {
        if (!methods[method]) throw new Error(`Unknown method ${method}`);
        return methods[method](db, collection, ...args, client);
      }));
    },
    async wsCall({ visitId, meta }, collectionName, methodName, args, one, debug?) {
      if (!args) args = {};
      const key = `${collectionName}.${methodName}.${JSON.stringify(args)}`;
      let id;
      if (!registry[key]) {
        id = nextId++;
        registry[key] = {
          id,
          collectionName,
          query: args,
          compiledQuery: sift(args),
        };
      }
      else {
        id = registry[key].id;
      }
      visitId?.(id);
      
      let r = await db.collection(collectionName)[methodName]({ $and: [args, { _deleted: { $exists: false } }] });

      if (methodName == 'find') {
        r = await r.toArray();
      }


      
      return !meta ? [ id, r ] : [ id, [ r, buildMeta(_.isArray(r) ? r : [r]) ]]
    },
  }
}
