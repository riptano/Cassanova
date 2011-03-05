#!/usr/bin/python
#
# Storage layout:
#
# CassanovaService stores info about keyspaces and column family definitions on
# the keyspaces attribute, separate from data.
#
# Actual data is stored in the data attribute, which is a dictionary mapping
# keyspace names to what I'll call keyspace-dicts.
#
# keyspace-dicts map ColumnFamily names to cf-dicts.
#
# cf-dicts map keys to row-dicts. In addition, the special None key in each
# cf-dict is mapped to its own ColumnFamily name.
#
# row-dicts map column names to Column objects, if in a standard ColumnFamily,
# or supercolumn names to supercolumn-dicts otherwise. In addition, the special
# None key in each row-dict is mapped to its own row key.
#
# supercolumn-dicts map column names to Column objects. In addition, the
# special None key in each supercolumn-dict is mapped to its own supercolumn
# name.

from cassandra_thrift import Cassandra, constants
from cassandra_thrift.ttypes import (KsDef, CfDef, InvalidRequestException, ColumnPath,
                                     NotFoundException, TokenRange, ColumnOrSuperColumn,
                                     SuperColumn, KeySlice, ColumnParent)
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTwisted
from twisted.application import internet, service
from twisted.internet import defer
from twisted.python import log
from zope.interface import implements
from cStringIO import StringIO
from functools import partial
import random
import struct
import hashlib
import os
import re

cluster_name = os.environ.get('CASSANOVA_CLUSTER_NAME', 'Fake Cluster')

valid_ks_name_re = re.compile(r'^[a-z][a-z0-9_]*$', re.I)
valid_cf_name_re = re.compile(r'^[a-z][a-z0-9_]*$', re.I)

identity = lambda n:n

class CassanovaInterface:
    implements(Cassandra.Iface)

    def __init__(self, node):
        self.node = node
        self.service = node.parent
        self.keyspace = None

    def login(self, auth_request):
        pass

    def set_keyspace(self, keyspace):
        k = self.service.get_keyspace(keyspace)
        self.keyspace = k

    def get(self, key, column_path, consistency_level):
        try:
            val = self.service.lookup_column_path(self.keyspace, key, column_path)
        except KeyError, e:
            # should get throw NFE when the cf doesn't even exist? or IRE still?
            log.msg('Could not find %r (keyspace %r)' % (e.args[0], self.keyspace.name))
            raise NotFoundException
        if isinstance(val, dict):
            sc = self.pack_up_supercolumn(self.keyspace, column_path, val)
            val = ColumnOrSuperColumn(super_column=sc)
        else:
            val = ColumnOrSuperColumn(column=val)
        return val

    def get_slice(self, key, column_parent, predicate, consistency_level):
        try:
            cols = self.service.lookup_column_parent(self.keyspace, key, column_parent)
        except KeyError:
            return []
        filteredcols = self.filter_by_predicate(column_parent, cols, predicate)
        if len(filteredcols) == 0:
            return filteredcols
        if isinstance(filteredcols[0], dict):
            pack = partial(self.pack_up_supercolumn, self.keyspace, column_parent)
            typ = 'super_column'
        else:
            pack = identity
            typ = 'column'
        return [ColumnOrSuperColumn(**{typ: pack(col)}) for col in filteredcols]

    def get_count(self, key, column_parent, predicate, consistency_level):
        return len(self.get_slice(key, column_parent, predicate, consistency_level))

    def multiget_slice(self, keys, column_parent, predicate, consistency_level):
        result = {}
        for k in keys:
            cols = self.get_slice(k, column_parent, predicate, consistency_level)
            if cols:
                result[k] = cols
        return result

    def multiget_count(self, keys, column_parent, predicate, consistency_level):
        slices = self.multiget_slice(keys, column_parent, predicate, consistency_level)
        return dict((k, len(cols)) for (k, cols) in slices.iteritems())

    def get_range_slices(self, column_parent, predicate, keyrange, consistency_level):
        cf, dat = self.service.lookup_cf_and_data(self.keyspace,
                                                  column_parent.column_family)
        compar = self.service.key_comparator()
        keypred = self.service.key_predicate(compar, keyrange)
        count = keyrange.count
        keys = sorted((k for k in dat.iterkeys() if k is not None), cmp=compar)
        out = []
        for k in keys:
            if keypred(k):
                cols = self.get_slice(k, column_parent, predicate, consistency_level)
                out.append(KeySlice(key=k, columns=cols))
                if len(out) >= count:
                    break
        return out

    def get_indexed_slices(self, column_parent, index_clause, column_predicate,
                           consistency_level):
        raise InvalidRequestException(why='get_indexed_slices unsupported here')

    def insert(self, key, column_parent, column, consistency_level):
        if not 0 < len(column.name) < (1<<16):
            raise InvalidRequestException('invalid column name')
        try:
            parent = self.service.lookup_column_parent(self.keyspace, key, column_parent,
                                                       make=True)
        except KeyError:
            raise InvalidRequestException(why=e.args[0])
        parent[column.name] = column

    def remove_column(self, key, column_path, tstamp, consistency_level):
        try:
            parent = self.service.lookup_column_parent(self.keyspace, key, column_path)
        except KeyError, e:
            raise InvalidRequestException(why='column parent %s not found' % e.args[0])
        try:
            col = parent[column_path.column]
            if col.timestamp <= tstamp:
                del parent[column_path.column]
        except KeyError:
            pass

    def remove_cols_from(self, row, tstamp):
        newrow = {}
        for colname, col in row.iteritems():
            if colname is not None and col.timestamp <= tstamp:
                continue
            newrow[colname] = col
        return newrow

    def remove_super_column(self, key, column_parent, tstamp, consistency_level):
        try:
            _, row = self.service.lookup_cf_and_row(self.keyspace, key,
                                                    column_parent.column_family)
            oldsc = row[column_parent.super_column]
        except KeyError:
            return
        newsc = self.remove_cols_from(oldsc, tstamp)
        if len(newsc) <= 1:
            del row[column_parent.super_column]
        else:
            row[column_parent.super_column] = newsc

    def remove_key(self, cfname, key, tstamp, consistency_level):
        try:
            cf, data = self.service.lookup_cf_and_data(self.keyspace, cfname)
            row = data[key]
        except KeyError:
            return
        if cf.column_type == 'Super':
            par = ColumnParent(column_family=cfname)
            for scname in list(row.keys()):
                if scname is None:
                    continue
                par.super_column = scname
                self.remove_super_column(key, par, tstamp, consistency_level)
        else:
            row = self.remove_cols_from(row, tstamp)
        data[key] = row

    def remove(self, key, column_path, tstamp, consistency_level):
        if column_path.column is not None:
            return self.remove_column(key, column_path, tstamp, consistency_level)
        else:
            if column_path.super_column is not None:
                return self.remove_super_column(key, column_path, tstamp,
                                                consistency_level)
            else:
                return self.remove_key(column_path.column_family, key, tstamp,
                                       consistency_level)

    def apply_mutate_delete(self, cf, row, deletion, consistency_level):
        cp = ColumnPath(column_family=cf.name, super_column=deletion.super_column)
        if deletion.predicate is None:
            return self.remove(row[None], cp, deletion.timestamp, consistency_level)
        if deletion.predicate.slice_range is not None:
            raise InvalidRequestException('Not supposed to support batch_mutate '
                                          'deletions with a slice_range (although '
                                          'we would if this check was gone)')
        if deletion.super_column is not None:
            try:
                row = row[deletion.super_column]
            except KeyError:
                return
        killme = self.filter_by_predicate(cp, row, deletion.predicate)
        for col in killme:
            if isinstance(col, dict):
                del row[col[None]]
            else:
                del row[col.name]

    def apply_mutate_insert_super(self, row, sc, consistency_level):
        scdat = {None: sc.name}
        for col in sc.columns:
            scdat[col.name] = col
        row[sc.name] = scdat

    def apply_mutate_insert_standard(self, row, col, consistency_level):
        row[col.name] = col

    def apply_mutate_insert(self, cf, row, cosc, consistency_level):
        if cf.column_type == 'Super':
            if cosc.super_column is None:
                raise InvalidRequestException('inserting Column into SuperColumnFamily')
            self.apply_mutate_insert_super(row, cosc.super_column, consistency_level)
        else:
            if cosc.super_column is not None:
                raise InvalidRequestException('inserting SuperColumn into standard '
                                              'ColumnFamily')
            self.apply_mutate_insert_standard(row, cosc.column, consistency_level)

    def batch_mutate(self, mutation_map, consistency_level):
        for key, col_mutate_map in mutation_map.iteritems():
            for cfname, mutatelist in col_mutate_map.iteritems():
                cf, dat = self.service.lookup_cf_and_row(self.keyspace, key, cfname,
                                                         make=True)
                for mutation in mutatelist:
                    if mutation.column_or_supercolumn is not None \
                    and mutation.deletion is not None:
                        raise InvalidRequestException(why='Both deletion and insertion'
                                                          ' in same mutation')
                    if mutation.column_or_supercolumn is not None:
                        self.apply_mutate_insert(cf, dat, mutation.column_or_supercolumn,
                                                 consistency_level)
                    else:
                        self.apply_mutate_delete(cf, dat, mutation.deletion,
                                                 consistency_level)

    def truncate(self, cfname):
        try:
            cf, dat = self.service.lookup_cf_and_data(self.keyspace, cfname)
        except KeyError, e:
            raise InvalidRequestException(why=e.args[0])
        name = dat[None]
        dat.clear()
        dat[None] = name

    def describe_schema_versions(self):
        smap = {}
        for c in self.service.ring.values():
            smap.setdefault(c.schema_code(), []).append(c.addr.host)
        return smap

    def describe_keyspaces(self):
        return self.service.get_schema()

    def describe_cluster_name(self):
        return self.service.clustername

    def describe_version(self):
        return constants.VERSION

    def describe_ring(self, keyspace):
        if keyspace == 'system':
            # we are a bunch of jerks
            raise InvalidRequestException("no replication ring for keyspace 'system'")
        self.service.get_keyspace(keyspace)
        trmap = self.service.get_range_to_endpoint_map()
        return [TokenRange(start_token=lo,
                           end_token=hi,
                           endpoints=[c.addr.host for c in endpoints])
                for ((lo, hi), endpoints) in trmap.items()]

    def describe_partitioner(self):
        return self.service.partitioner.java_name

    def describe_snitch(self):
        return self.service.snitch.java_name

    def describe_keyspace(self, ksname):
        try:
            return self.service.get_keyspace(ksname)
        except InvalidRequestException:
            raise NotFoundException

    def describe_splits(self, cfname, starttoken, endtoken, keys_per_split):
        raise InvalidRequestException(why='describe_splits unsupported here')

    def system_add_column_family(self, cfdef):
        ks = self.keyspace
        for cf in ks.cf_defs:
            if cf.name == cfdef.name:
                raise InvalidRequestException(why='CF %r already exists' % cf.name)
        self.set_defaults_on_cfdef(cfdef)
        cfdef.keyspace = self.keyspace.name
        ks.cf_defs.append(cfdef)
        return self.node.schema_code()

    def set_defaults_on_cfdef(self, cfdef):
        if not valid_cf_name_re.match(cfdef.name):
            raise InvalidRequestException(why='Invalid columnfamily name %r'
                                              % cfdef.name)
        if cfdef.comparator_type is None:
            cfdef.comparator_type = 'BytesType'
        if cfdef.subcomparator_type is None:
            cfdef.subcomparator_type = 'BytesType'
        try:
            vtype = self.service.load_class_by_java_name(cfdef.comparator_type)
            vtype.comparator
            cfdef.comparator_type = vtype.java_name
            if cfdef.column_type == 'Super':
                vstype = self.service.load_class_by_java_name(cfdef.subcomparator_type)
                vstype.comparator
                cfdef.subcomparator_type = vstype.java_name
        except (KeyError, AttributeError), e:
            raise InvalidRequestException(why='Invalid comparator or '
                                              'subcomparator class: %s'
                                              % (e.args[0],))
        if cfdef.gc_grace_seconds is None:
            cfdef.gc_grace_seconds = 864000
        if cfdef.column_metadata is None:
            cfdef.column_metadata = []
        if cfdef.default_validation_class is None:
            cfdef.default_validation_class = cfdef.comparator_type
        cfdef.id = id(cfdef) & 0xffffffff
        if cfdef.min_compaction_threshold is None:
            cfdef.min_compaction_threshold = 4
        if cfdef.max_compaction_threshold is None:
            cfdef.max_compaction_threshold = 32
        if cfdef.row_cache_save_period_in_seconds is None:
            cfdef.row_cache_save_period_in_seconds = 0
        if cfdef.key_cache_save_period_in_seconds is None:
            cfdef.key_cache_save_period_in_seconds = 14400

    def system_drop_column_family(self, cfname):
        ks = self.keyspace
        newlist = [cf for cf in ks.cf_defs if cf.name != cfname]
        if len(newlist) == len(ks.cf_defs):
            raise InvalidRequestException(why='no such CF %r' % cfname)
        ks.cf_defs = newlist
        try:
            del self.service.data[ks.name][cfname]
        except KeyError:
            pass
        return self.node.schema_code()

    def system_add_keyspace(self, ksdef):
        if ksdef.name in self.service.keyspaces:
            raise InvalidRequestException(why='keyspace %r already exists' % ksdef.name)
        if not valid_ks_name_re.match(ksdef.name):
            raise InvalidRequestException(why='invalid keyspace name %r' % ksdef.name)
        try:
            ksdef.strat = self.service.load_class_by_java_name(ksdef.strategy_class)
        except KeyError:
            raise InvalidRequestException(why='Unable to find replication strategy %r'
                                              % ksdef.strategy_class)
        for cf in ksdef.cf_defs:
            cf.keyspace = ksdef.name
            self.set_defaults_on_cfdef(cf)
        self.service.keyspaces[ksdef.name] = ksdef
        return self.node.schema_code()

    def system_drop_keyspace(self, ksname):
        try:
            del self.service.keyspaces[ksname]
        except KeyError:
            raise InvalidRequestException(why='no such keyspace %r' % ksname)
        try:
            del self.service.data[ksname]
        except KeyError:
            pass
        return self.node.schema_code()

    def system_update_keyspace(self, ksdef):
        if not valid_ks_name_re.match(ksdef.name):
            raise InvalidRequestException(why='invalid keyspace name %r' % ksdef.name)
        self.service.get_keyspace(ksdef.name)
        try:
            ksdef.strat = self.service.load_class_by_java_name(ksdef.strategy_class)
        except KeyError:
            raise InvalidRequestException(why='Unable to find replication strategy %r'
                                              % ksdef.strategy_class)
        for cf in ksdef.cf_defs:
            cf.keyspace = ksdef.name
            self.set_defaults_on_cfdef(cf)
        self.service.keyspaces[ksdef.name] = ksdef
        return self.node.schema_code()

    def system_update_column_family(self, cfdef):
        try:
            oldcf = self.service.lookup_cf(self.keyspace, cfdef.name)
        except KeyError:
            raise InvalidRequestException(why='no such columnfamily %s'
                                              % cfdef.name)
        self.set_defaults_on_cfdef(cfdef)
        # can't change some attributes
        for attr in ('keyspace', 'comparator_type', 'subcomparator_type'):
            if getattr(cfdef, attr, None) != getattr(oldcf, attr, None):
                raise InvalidRequestException(why="can't change %s" % attr)
        for attr, val in cfdef.__dict__.iteritems():
            if attr != 'id':
                setattr(oldcf, attr, val)
        return self.node.schema_code()

    def execute_cql_query(query, compression):
        raise InvalidRequestException(why='CQL unsupported here')

    @staticmethod
    def filter_by_col_names(cols, colnames):
        results = []
        for n in colnames:
            try:
                results.append(cols[n])
            except KeyError:
                pass
        return results

    def filter_by_slice_range(self, column_parent, cols, slicerange):
        compar = self.service.comparator_for(self.keyspace, column_parent)
        colpred = self.service.slice_predicate(compar, slicerange)
        count = slicerange.count
        if count == 0:
            return []
        name = cols.pop(None)
        items = sorted(cols.iteritems(), cmp=compar, key=lambda i:i[0],
                       reverse=bool(slicerange.reversed))
        cols[None] = name
        filtered = []
        for cname, col in items:
            if colpred(cname):
                filtered.append(col)
                if len(filtered) >= count:
                    break
        return filtered

    def filter_by_predicate(self, column_parent, cols, predicate):
        if predicate.column_names is not None:
            # yes, ignore slice_range even if set)
            return self.filter_by_col_names(cols, predicate.column_names)
        else:
            return self.filter_by_slice_range(column_parent, cols, predicate.slice_range)

    def pack_up_supercolumn(self, keyspace, column_parent, dat):
        name = dat.pop(None)
        cp = ColumnParent(column_family=column_parent.column_family,
                          super_column=name)
        compar = self.service.comparator_for(self.keyspace, cp)
        cols = dat.values()
        cols.sort(cmp=compar, key=lambda c:c.name)
        dat[None] = name
        return SuperColumn(name=name, columns=cols)

class CassanovaServerProtocol(TTwisted.ThriftServerProtocol):
    def processError(self, error):
        # stoopid thrift doesn't log this stuff
        log.err(error, 'Error causing connection reset')
        TTwisted.ThriftServerProtocol.processError(self, error)

class FactoryProxy:
    def __init__(self, realfactory, processormaker):
        self.realfactory = realfactory
        self.processor = processormaker()

    def __getattr__(self, name):
        return getattr(self.realfactory, name)

class LogCallWrapper:
    implements(Cassandra.Iface)

    def __init__(self, food, wrapnames):
        self.food = food
        self.wrapnames = set(wrapnames)

    def __getattr__(self, name):
        val = getattr(self.food, name)
        if name in self.wrapnames:
            def wrapper(*a, **kw):
                args = map(repr, a)
                args += ['%s=%r' % (k, v) for (k, v) in kw.iteritems()]
                log.msg('%s(%s)' % (name, ', '.join(args)))
                d = defer.maybeDeferred(val, *a, **kw)
                d.addCallback(lambda ans: [log.msg('=> %r' % ans), ans][1])
                d.addErrback(lambda err: [log.err(err, 'Returning error'), err][1])
                return d
            wrapper.func_name = 'wrapper_for_%s' % name
            setattr(self, name, wrapper)
            return wrapper
        return name

class CassanovaProcessor(Cassandra.Processor):
    def __init__(self, handler):
        wraphandler = LogCallWrapper(handler, ())
        Cassandra.Processor.__init__(self, wraphandler)
        wraphandler.wrapnames.update(self._processMap)

class CassanovaFactory(TTwisted.ThriftServerFactory):
    protocol = CassanovaServerProtocol

    def __init__(self, node):
        self.node = node
        protocolfactory = TBinaryProtocol.TBinaryProtocolFactory()
        TTwisted.ThriftServerFactory.__init__(self, None, protocolfactory)

    def buildProtocol(self, addr):
        """
        This lets us get around Thrift's vehement hatred of statefulness
        in the interface handler; we abuse the protocol instance's "factory"
        attribute to point at a per-connection object.
        """

        p = self.protocol()
        p.factory = FactoryProxy(self, self.make_processor)
        return p

    def make_processor(self):
        return CassanovaProcessor(CassanovaInterface(self.node))

class CassanovaNode(internet.TCPServer):
    factory = CassanovaFactory

    def __init__(self, port, interface, token=None):
        internet.TCPServer.__init__(self, port, self.factory(self),
                                    interface=interface)
        if token is None:
            token = random.getrandbits(127)
        self.mytoken = token

    def startService(self):
        internet.TCPServer.startService(self)
        self.addr = self._port.getHost()

    def get_schema(self):
        # in case tests want to override this functionality
        return self.parent.get_schema()

    def schema_code(self):
        return self.make_code(self.serialize_schema_def())

    @staticmethod
    def make_code(info):
        x = hashlib.md5()
        x.update(info)
        digits = x.hexdigest()
        return '-'.join([digits[n:(n+s)]
                         for (n, s) in [(0,8),(8,4),(12,4),(16,4),(20,12)]])

    def serialize_schema_def(self):
        klist = sorted(self.get_schema(), key=lambda k: k.name)
        buf = StringIO()
        for k in klist:
            k.write(TBinaryProtocol.TBinaryProtocol(buf))
        return buf.getvalue()

    def __hash__(self):
        return hash((self.__class__, self.addr, self.mytoken))

    def __str__(self):
        return '<%s at %s:%d>' % (self.__class__.__name__, self.addr.host, self.addr.port)
    __repr__ = __str__

java_class_map = {}
class register_java_class(type):
    def __new__(metacls, name, bases, attrs):
        newcls = super(register_java_class, metacls).__new__(metacls, name, bases, attrs)
        javaname = attrs.get('java_name', '*dummy*.%s' % name)
        java_class_map[javaname] = newcls
        java_class_map[javaname.rsplit('.', 1)[-1]] = newcls
        return newcls

class JavaMimicClass(object):
    __metaclass__ = register_java_class

class ReplicationStrategy(JavaMimicClass):
    pass

class SimpleStrategy(ReplicationStrategy):
    java_name = 'org.apache.cassandra.locator.SimpleStrategy'

class OldNetworkTopologyStrategy(ReplicationStrategy):
    java_name = 'org.apache.cassandra.locator.OldNetworkTopologyStrategy'

class NetworkTopologyStrategy(ReplicationStrategy):
    java_name = 'org.apache.cassandra.locator.NetworkTopologyStrategy'

class LocalStrategy(ReplicationStrategy):
    java_name = 'org.apache.cassandra.locator.LocalStrategy'

class Partitioner(JavaMimicClass):
    min_bound = ''
    key_cmp = staticmethod(cmp)

    @classmethod
    def token_as_bytes(cls, tok):
        return tok

class RandomPartitioner(Partitioner):
    java_name = 'org.apache.cassandra.dht.RandomPartitioner'

    @classmethod
    def token_as_bytes(cls, tok):
        return '%d' % tok

class SimpleSnitch(JavaMimicClass):
    java_name = 'org.apache.cassandra.locator.SimpleSnitch'

class AbstractType(JavaMimicClass):
    @classmethod
    def input(cls, mybytes):
        return mybytes

    @classmethod
    def comparator(cls, a, b):
        return cmp(cls.input(a), cls.input(b))

class BytesType(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.BytesType'

class LongType(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.LongType'

    @classmethod
    def input(cls, mybytes):
        if len(mybytes) < 8:
            mybytes = '\x00' * (8 - len(mybytes)) + mybytes
        elif len(mybytes) > 8:
            raise ValueError('Bad input %r to LongType' % (mybytes,))
        return struct.unpack('!q', mybytes)[0]

class IntegerType(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.IntegerType'

class UTF8Type(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.UTF8Type'

class TimeUUIDType(AbstractType):
    java_name = 'org.apache.cassandra.db.marshal.TimeUUIDType'

class CassanovaService(service.MultiService):
    partitioner = RandomPartitioner()
    snitch = SimpleSnitch()
    port = 9160

    def __init__(self, port=None):
        service.MultiService.__init__(self)
        self.keyspaces = {'system': KsDef(
            'system',
            replication_factor=1,
            strategy_class='org.apache.cassandra.locator.LocalStrategy',
            cf_defs=[]
        )}
        self.data = {}
        if port is not None:
            self.port = port
        self.ring = {}
        self.clustername = cluster_name

    def add_node(self, iface, token=None):
        node = CassanovaNode(self.port, iface, token=token)
        node.setServiceParent(self)
        self.ring[node.mytoken] = node

    def get_keyspace(self, ks):
        try:
            return self.keyspaces[ks]
        except KeyError:
            raise InvalidRequestException(why='No such keyspace %r' % ks)

    def get_range_to_endpoint_map(self):
        ring = sorted(self.ring.items())
        trmap = {}
        for num, r in enumerate(ring):
            endpoints = (ring[num-1][0], ring[num][0])
            trmap[tuple(map(self.partitioner.token_as_bytes, endpoints))] = [r[1]]
        return trmap

    def get_schema(self):
        return self.keyspaces.values()

    def lookup_cf(self, ks, cfname):
        ksdef = self.get_keyspace(ks.name)
        for cfdef in ksdef.cf_defs:
            if cfdef.name == cfname:
                break
        else:
            raise InvalidRequestException(why='No such column family %r' % cfname)
        return cfdef

    def lookup_cf_and_data(self, ks, cfname):
        cfdef = self.lookup_cf(ks, cfname)
        datadict = self.data.setdefault(ks.name, {}).setdefault(cfname, {None: cfname})
        return cfdef, datadict

    def lookup_cf_and_row(self, ks, key, cfname, make=False):
        cf, data = self.lookup_cf_and_data(ks, cfname)
        try:
            row = data[key]
        except KeyError:
            if make:
                row = data[key] = {None: key}
            else:
                raise
        return cf, row

    def lookup_column_parent(self, ks, key, column_parent, make=False):
        cf, row = self.lookup_cf_and_row(ks, key, column_parent.column_family, make=make)
        if cf.column_type == 'Super':
            if column_parent.super_column is None:
                colparent = row
            else:
                try:
                    colparent = row[column_parent.super_column]
                except KeyError:
                    if make:
                        sc = column_parent.super_column
                        colparent = row[sc] = {None: sc}
                    else:
                        raise
        else:
            if column_parent.super_column is not None:
                raise InvalidRequestException(
                    why='Improper ColumnParent to ColumnFamily')
            colparent = row
        return colparent

    def lookup_column_path(self, ks, key, column_path):
        try:
            colparent = self.lookup_column_parent(ks, key, column_path)
        except InvalidRequestException, e:
            newmsg = e.why.replace('ColumnParent', 'ColumnPath')
            raise InvalidRequestException(why=newmsg)
        if column_path.column is None:
            return colparent
        return colparent[column_path.column]

    def comparator_for(self, ks, column_parent):
        cf = self.lookup_cf(ks, column_parent.column_family)
        if column_parent.super_column is not None:
            return self.load_class_by_java_name(cf.subcomparator_type).comparator
        else:
            return self.load_class_by_java_name(cf.comparator_type).comparator

    def key_comparator(self):
        return self.partitioner.key_cmp

    def load_class_by_java_name(self, name):
        return java_class_map[name]

    def slice_predicate(self, comparator, slicerange):
        start = slicerange.start
        end = slicerange.finish
        if slicerange.reversed:
            start, end = end, start
        if start == self.partitioner.min_bound:
            scmp = lambda n: True
        else:
            scmp = lambda n: comparator(start,n) <= 0
        if end == self.partitioner.min_bound:
            ecmp = lambda n: True
        else:
            ecmp = lambda n: comparator(end,n) >= 0
        return lambda n: scmp(n) and ecmp(n)

    def key_predicate_keys(self, compar, start, end):
        if end == self.partitioner.min_bound:
            return lambda k: compar(k, start) >= 0
        else:
            return lambda k: compar(k, start) >= 0 \
                             and compar(k, end) <= 0

    def key_predicate_tokens(self, compar, start, end):
        if compar(start, end) >= 0:
            return lambda k: compar(k, start) > 0 \
                             or compar(k, end) <= 0
        else:
            return lambda k: compar(k, start) > 0 \
                             and compar(k, end) <= 0

    def key_predicate(self, comparator, keyrange):
        isnones = [getattr(keyrange, x) is not None
                   for x in ('start_key', 'end_key', 'start_token', 'end_token')]
        if isnones == [True, True, False, False]:
            return self.key_predicate_keys(comparator,
                                           keyrange.start_key, keyrange.end_key)
        elif isnones == [False, False, True, True]:
            return self.key_predicate_tokens(comparator,
                                             keyrange.start_token, keyrange.end_token)
        raise InvalidRequestException(why="Improper KeyRange")
