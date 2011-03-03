# this 'test suite' just pulls in the cassandra Thrift test suite and
# removes the ones not expected to pass because of unsupported features.

import os
import sys
import __init__

__init__.root = os.path.dirname(__file__)
__init__.pid_fname = os.path.join(__init__.root, 'system_test.pid')

from test_thrift_server import TestMutations, TestTruncate

for attr in list(TestMutations.__dict__):
    if 'batch_mutate' in attr and attr.startswith('test_'):
        delattr(TestMutations, attr)

# this expects to see a TApplicationException from a bad insert call, but the
# thrift definition doesn't say that we can throw one of those, so for now,
# dying and closing the thrift conn is probably good enough
del TestMutations.test_bad_calls

# batch_mutate not supported
del TestMutations.test_batch_insert
del TestMutations.test_batch_insert_blocking
del TestMutations.test_batch_insert_super
del TestMutations.test_batch_insert_super_blocking

# COPP not supported
del TestMutations.test_wrapped_range_slices
del TestMutations.test_describe_partitioner
del TestMutations.test_range_collation
del TestMutations.test_range_partial

# resurrection not supported
del TestMutations.test_cf_remove
del TestMutations.test_cf_remove_column
del TestMutations.test_super_cf_remove_column
del TestMutations.test_super_cf_remove_supercolumn

# indexes not supported
del TestMutations.test_dynamic_indexes_with_system_update_cf
del TestMutations.test_index_scan
del TestMutations.test_index_scan_expiring

# column validators not supported
del TestMutations.test_column_validators

# TTLs not supported
del TestMutations.test_simple_expiration
