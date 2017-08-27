#
# Copyright (c) 2017 Gilles Chehade <gilles@poolp.org>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

import hashlib
import os
import os.path
import posix
import sys
import tempfile
import uuid

_PATH_OBJECTS = '__objects__'
_PATH_NAMESPACE = '__namespace__'
_PATH_TRANSACTIONS = '__transactions__'
_PATH_COMMITS = '__commits__'
_PATH_PURGE = '__purge__'

class Store(object):
    def __init__(self, directory):
        self.directory = directory

        if not os.path.exists(path=os.path.join(directory, '.inited')):
            self.__initialize()

    def __initialize(self):
        os.makedirs(name=self.directory, mode=0o700, exist_ok=True)
        subdirs = [self.path_commits(), self.path_purge(), self.path_objects(), self.path_namespace(), self.path_transactions()]
        for subdir in subdirs:
            for bucket in range(0x0, 0xff+1):
                os.makedirs(name=os.path.join(self.directory, subdir, "%02x" % bucket), mode=0o700, exist_ok=True)
        open(os.path.join(self.directory, '.inited'), "w").close()

    def key_hash(self, key):
        hasher = hashlib.sha256()
        hasher.update(key.encode())
        return hasher.hexdigest()

    def path_objects(self):
        return os.path.join(self.directory, _PATH_OBJECTS)

    def path_namespace(self):
        return os.path.join(self.directory, _PATH_NAMESPACE)

    def path_purge(self):
        return os.path.join(self.directory, _PATH_PURGE)

    def path_commits(self):
        return os.path.join(self.directory, _PATH_COMMITS)

    def path_transactions(self):
        return os.path.join(self.directory, _PATH_TRANSACTIONS)

    def transaction(self):
        return Transaction(self)

    def __commit_first_stage(self, transaction):
        pathname = os.path.join(self.path_commits(), transaction[0:2], transaction)

        # objects that aren't symlinked in the transaction namespace are orphans
        namespace = [_ for _ in os.scandir(os.path.join(pathname, _PATH_NAMESPACE))]
        targets = set([os.readlink(_.path) for _ in namespace if not _.name.endswith('-') ])

        for entry in os.scandir(os.path.join(pathname, _PATH_OBJECTS)):
            name, path = entry.name, entry.path
            path = entry.path

            if name not in targets:
                os.unlink(path)
                continue

            if name.endswith('-') or os.path.exists(path + '-'):
                continue

            while True:
                try:
                    os.link(src=os.path.join(self.path_objects(), name[0:2], name), dst=path + '-')
                    break
                except FileNotFoundError:
                    try:
                        os.link(src=path, dst=os.path.join(self.path_objects(), name[0:2], name))
                    except FileExistsError:
                        pass
            os.unlink(path=path)

    def __commit_second_stage(self, transaction):
        pathname = os.path.join(self.path_commits(), transaction[0:2], transaction)

        for entry in os.scandir(os.path.join(pathname, _PATH_NAMESPACE)):
            name, path = entry.name, entry.path
            if name.endswith('-') or os.path.exists(path + '-'):
                continue

            target = os.readlink(path)
            os.link(src=os.path.join(self.path_objects(), target[0:2], target), dst=path + '-')
            os.unlink(path)
            try:
                os.unlink(os.path.join(pathname, _PATH_OBJECTS, target + '-'))
            except FileNotFoundError:
                pass


    def __commit_third_stage(self, transaction, namespace):
        pathname = os.path.join(self.path_commits(), transaction[0:2], transaction)
        os.chmod(pathname, 0o000)
        for entry in namespace:
            name, path = entry.name, entry.path
            try:
                os.symlink(src=path,
                           dst=os.path.join(self.path_namespace(), name[0:2], name[:-1] + ':committed'))
            except FileExistsError:
                pass


    def __commit_fourth_stage(self, transaction, namespace):
        pathname = os.path.join(self.path_commits(), transaction[0:2], transaction)
        os.chmod(pathname, 0o700)
        for entry in namespace:
            name, path = entry.name, entry.path
            try:
                os.unlink(os.path.join(self.path_namespace(), name[0:2], name[:-1]))
            except FileNotFoundError:
                pass
            os.rename(src=path, dst=os.path.join(self.path_namespace(), name[0:2], name[:-1]))
            try:
                os.unlink(os.path.join(self.path_namespace(), name[0:2], name[:-1] + ':committed'))
            except FileNotFoundError:
                pass

    def __commit_finalize(self, transaction):
        pathname = os.path.join(self.path_commits(), transaction[0:2], transaction)
        try:
            os.rmdir(os.path.join(pathname, _PATH_OBJECTS))
        except FileNotFoundError:
            pass
        try:
            os.rmdir(os.path.join(pathname, _PATH_NAMESPACE))
        except FileNotFoundError:
            pass
        try:
            os.rmdir(pathname)
        except FileNotFoundError:
            pass


    def commit(self, transaction):
        path = os.path.join(self.path_commits(), transaction[0:2], transaction)

        namespace = [_ for _ in os.scandir(os.path.join(path, _PATH_NAMESPACE)) if not _.name.endswith('-')]
        targets = set([os.readlink(_.path) for _ in namespace ])

        if not targets:
            self.__commit_finalize(transaction)
            return

        # XXX - tricky magic happens here to maintain atomicity

        # 1st stage, synchronize transaction objects with store
        # 2nd stage, synchronize transaction namespace with store
        # 3rd stage, updating namespace
        # 4th stage, enabling new namespace
        # final stage, unlink commit directory

        self.__commit_first_stage(transaction)
        self.__commit_second_stage(transaction)

        namespace = [_ for _ in os.scandir(os.path.join(path, _PATH_NAMESPACE))]

        self.__commit_third_stage(transaction, namespace)
        self.__commit_fourth_stage(transaction, namespace)
        self.__commit_finalize(transaction)


    def purge(self, resource):
        path = os.path.join(self.path_purge(), resource[0:2], resource)
        try:
            os.chmod(path, mode=0o700)
            for subdir in os.scandir(path):             # transaction
                for entry in os.scandir(subdir.path):   # namespace/objects
                    os.unlink(entry.path)
                os.rmdir(subdir.path)
            os.rmdir(path)
        except NotADirectoryError:
            os.unlink(path)


    def get(self, key):
        lkp = self.key_hash(key)
        try:
            with open(os.path.join(self.path_namespace(), lkp[0:2], lkp + ':committed'), "rb") as fp:
                return fp.read()
        except FileNotFoundError:
            pass
        except PermissionError:
            pass

        try:
            with open(os.path.join(self.path_namespace(), lkp[0:2], lkp), "rb") as fp:
                return fp.read()
        except FileNotFoundError:
            return None

class Transaction(object):
    def __init__(self, store):
        self.store = store
        while True:
            self.uuid = uuid.uuid4().hex
            self.directory = os.path.join(self.store.path_transactions(), self.uuid[:2], self.uuid)
            try:
                os.mkdir(self.directory, mode=0o700)
            except FileExistsError:
                continue
            break
        for path in [self.path_namespace(), self.path_objects()]:
            os.makedirs(path, mode=0o700)
        self.done = False

    def path_namespace(self):
        return os.path.join(self.directory, _PATH_NAMESPACE)

    def path_objects(self):
        return os.path.join(self.directory, _PATH_OBJECTS)

    def path_object(self, checksum):
        return os.path.join(self.path_namespace(), checksum)

    def __enter__(self):
        return self

    def __exit__(self, x, y, z):
        if not self.done:
            self.rollback()

    def put(self, key, data):
        lkp = self.store.key_hash(key)

        hasher = hashlib.sha256()
        fdo, filename = tempfile.mkstemp(prefix='.', dir=self.path_objects())
        with os.fdopen(fdo, "wb") as fpo:
            hasher.update(data)
            fpo.write(data)
        checksum = hasher.hexdigest()

        # link new file to transaction object store
        os.rename(src=filename, dst=self.path_object(checksum))

        # symlink transaction object to transaction namespace
        try:
            target = os.readlink(os.path.join(self.path_namespace(), lkp))
            if target != checksum:
                os.symlink(src=checksum, dst=os.path.join(self.path_namespace(), lkp))
        except FileExistsError:
            os.unlink(path=os.path.join(self.path_namespace(), lkp))
            os.symlink(src=checksum, dst=os.path.join(self.path_namespace(), lkp))
        except FileNotFoundError:
            os.symlink(src=checksum, dst=os.path.join(self.path_namespace(), lkp))

    def get(self, key):
        lkp = self.store.key_hash(key)
        try:
            target = os.readlink(os.path.join(self.path_namespace(), lkp))
            with open(self.path_object(target), "rb") as fp:
                return fp.read()
        except FileNotFoundError:
            return self.store.get(key)

    def commit(self):
        if self.done:
            raise Exception
        self.done = True
        destination = os.path.join(self.store.path_commits(), self.uuid[:2], self.uuid)
        os.rename(src=self.directory, dst=destination)
        self.store.commit(self.uuid)

    def rollback(self):
        if self.done:
            raise Exception
        self.done = True
        destination = os.path.join(self.store.path_purge(), self.uuid[:2], self.uuid)
        os.rename(src=self.directory, dst=destination)
        self.store.purge(self.uuid)
