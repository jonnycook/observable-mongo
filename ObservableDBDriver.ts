export interface ObservableDBDriver {
  init(observers);
  call(opts, args: [ string, string, any ][]);
  wsCall(opts: {
    visitId
    tag?
    client?
    meta?
  }, collectionName, methodName, args, one, debug?): Promise<[number, any]>;
}
