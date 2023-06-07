import MongoDB from 'mongodb';
import { ReadObserver } from "./ReadObserver";

export interface ObservableDB extends MongoDB.Db {
  createReadObserver(opts?: { tag?; client?; meta?; }): ReadObserver;
  observe(id, observer, tag?);
  stopObserving(id, observer);
  taggedDb(opts: { tag?; client?; meta?; }): ObservableDB;
  push(payload);
  batch(payload);
}
