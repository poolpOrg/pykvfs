# pykvfs

#
# THIS IS A WORK IN PROGRESS PROJECT, DO NOT USE
#

pykvfs is a pure-python package providing a filesystem-backed persistent transactional key-value store.


What is a transactional key-value store ?
-----------------------------------------
A key-value store is a database allowing to store and retrieve data identified by a unique key.

A transactional key-value store provides transactions allowing atomic commit and rollback of multiple key-value records.

pykvfs provides read-committed transactions, ensuring that:
- changes within a transaction are only visible to this transaction until committed
- changes committed are available to other running transactions


How does it work ?
------------------
Basically, it consists of a filesystem store containing several directories within which the magic happens.

A global storage contains the objects store (all values) and the namespace (all keys).

The objects store contains values stored in files identified with a checksum providing deduplication.

The namespace contains keys which are stored as a hardlink to the value they map to in the objects store.


In addition, a transaction storage contains a transaction-local object store and namespace.

Within a transaction, inserts are done in the local store, lookups are done in the local store first and global store next.

Upon rollback, the transaction store is purged.

Upon commit, the transaction store is moved to a commit space and a commit phase merges the transaction to global stores.


Requires
--------
Python 3.x, no dependencies.

Pykvfs relies heavily on the filesystem having sane semantics:
- atomic link(), unlink(), rename(), symlink(), rmdir()

*I don't even want to know if it can work on NFS*

It also relies heavily on hardlink and symlinks to play games that maintain atomicity and recoverability.
Doing an rsync, a copy or essentially doing anything manually within the store is a bad idea.


Features
--------
This is a work in progress, bugs are bound to happen and ruin your data.

Currently, pykfvs supports:
- concurrent accesses to the store from multiple transactions
- `get()` in the global store
- transactional `put()`
- transactional `get()` with read-committed semantics
- atomic commit and rollback
- recoverable state in case of application crash
- storage deduplication for values
- best-effort handling of conflicting commits to same keys 


TODO
----
- add transactional `update()`
- add transactional `delete()`
- simplify and optimize commit stages
- better handle conflicts
- cleanup code


Example
-------
```python
import pykvfs

store = pykfs.Store(directory="/var/pykvfs")

# perform a transaction-less lookup
value = store.get('foobar')
if not value:
  print("not found !")

# automatically rollbacked transaction
with store.transaction() as tx:
  tx.get('foobar')                # nothing
  tx.put('foobar', b'blabla')     # map 'foobar' to b'blabla' in transaction
  tx.get('foobar')                # b'blabla'
store.get('foobar')               # nothing, we didn't commit, value was not written outside transaction

# same but with explicit rollback
with store.transaction() as tx:
  tx.get('foobar')                # nothing
  tx.put('foobar', b'blabla')     # map 'foobar' to b'blabla' in transaction
  tx.get('foobar')                # b'blabla'
  tx.rollback()
store.get('foobar')               # nothing, we didn't commit, value was not written outside transaction

# let's commit now
with store.transaction() as tx:
  tx.get('foobar')                # nothing
  tx.put('foobar', b'blabla')     # map 'foobar' to b'blabla' in transaction
  tx.get('foobar')                # b'blabla'
  tx.commit()
store.get('foobar')               # b'blabla'

# let's start another one
with store.transaction() as tx:
  tx.get('foobar')                # b'blabla', fetched from global store
  tx.put('foobar', b'bliblou')    # map 'foobar' to b'bliblou' in transaction
  tx.get('foobar')                # b'bliblou'
store.get('foobar')               # b'blabla', fetched from global store

```

