import MongoDB from 'mongodb';

export type ReadObserver = MongoDB.Db & { reads(): any[]; };
