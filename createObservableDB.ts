import MongoDB from 'mongodb';
import { ObservableDB } from './ObservableDB';
import { ReadObserver } from './ReadObserver';
import { ObservableDBDriver } from './ObservableDBDriver';

export async function createObservableDB({ init, wsCall, call }: ObservableDBDriver): Promise<ObservableDB> {
  function makeDB(opts: { tag?; client?; meta?; }, visitId?): MongoDB.Db & { push(payload); batch(payload) } {
    const allOpts = { ...opts, visitId };
    return {
      collection(collectionName: string): MongoDB.Collection<any> {

        return <any>{
          async find(query) {
            const [id, r] = await wsCall(allOpts, collectionName, 'find', query, false);
            return {
              async toArray() {
                return r;
              }
            };
          },
          async findOne(filter, debug?) {
            const [id, r] = await wsCall(allOpts, collectionName, 'findOne', filter, true, debug);
            // console.log('poop', collectionName, filter, id, r)
            return r;
          },
          updateOne(filter, update, opts = {}) {
            return call(allOpts, [ [ collectionName, 'updateOne', [filter, update, opts] ] ]);
          },
          deleteOne(filter) {
            return call(allOpts, [[collectionName, 'deleteOne', [filter]]]);
          },
          async insertOne(doc) {
            const id = await call(allOpts, [[collectionName, 'insertOne', [doc]]]);
            doc._id = id;
            return true;
          }
        };
      },
      collections() {
        return null;
      },
      push(payload) {
        return call(allOpts, [[payload.collection, 'push', [payload]]]);
      },
      batch(payload) {
        return call(allOpts, payload);
      }
    } as any;
  }

  const observers: any = {};

  function makeApi(opts = {}) {
    const db = makeDB(opts);

    return Object.assign({}, db, {
      taggedDb(opts): ObservableDB {
        return makeApi(opts);
      },
      createReadObserver(t = {}): ReadObserver {
        const ids = [];

        return Object.assign(makeDB(Object.assign({}, opts, t), id => {
          ids.push(id);
        }), {
          reads(): any[] {
            return ids;
          }
        });
      },
      observe(id, observer, tag) {
        if (!observers[id]) {
          observers[id] = [];
        }
        // console.log('adding observer for', tag, 'to', id)
        observers[id].push({ observer, tag });
      },
      stopObserving(id, observer) {
        const index = observers[id].findIndex(entry => entry.observer == observer);
        if (index != -1) {
          observers[id].splice(index, 1);
        }
      },
      push(payload) {
        return db.push(payload);
      },
      batch(payload) {
        return db.batch(payload);
      }
    });
  }

  const api = makeApi();

  await init(observers);

  return api;
}
