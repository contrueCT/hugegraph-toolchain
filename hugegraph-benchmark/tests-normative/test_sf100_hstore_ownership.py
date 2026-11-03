"""Historical executed-v7 ownership regression, portable without private run evidence.

The frozen function fixture preserves the original test target. It does not
qualify a newer supervisor; current execution has its own versioned checks.
HGBENCH_SUPERVISOR_SOURCE can still point at the original executed source.
"""
import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import unittest
from unittest.mock import Mock


FIXTURE = Path(__file__).with_name('fixtures') / 'sf100-hstore-ownership-v7.py'
SOURCE = Path(os.environ.get('HGBENCH_SUPERVISOR_SOURCE', str(FIXTURE)))
FUNCTIONS_SHA256 = '996dc6ffb7bc1f675ec801d7e5324809f2f6942ed432ac883cb9ef2941ac1e5c'
OWNER = 'hgbench.sf100-real-20261025-001'
ROOT = Path('/owned/hstore-v7')
OWN = 'a' * 64
PROTECTED = 'b' * 64
OTHER = 'a' * 12 + 'c' * 52


def container(cid=OWN, role='pd', running=True, root=ROOT):
    return {'Id': cid, 'Config': {'Labels': {'hgbench.owner': OWNER,
            'hgbench.root': str(root), 'hgbench.role': role}},
            'State': {'Running': running, 'Status': 'running' if running else 'exited'}}


class HostPath:
    def __init__(self, name):
        self.name = name

    def read_text(self):
        return 'Filename Type Size Used Priority\n' if self.name == '/proc/swaps' else 'MemAvailable: 8388608 kB\n'


class OwnershipTest(unittest.TestCase):
    def setUp(self):
        # Execute the unchanged historical functions without any service-start entry point.
        source = SOURCE.read_text()
        tree = ast.parse(source)
        functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                     and n.name in {'owned_ids', 'guard', 'stop_owned'}]
        canonical = '\n\n'.join(ast.get_source_segment(source, node) for node in functions)
        self.assertEqual(hashlib.sha256(canonical.encode()).hexdigest(), FUNCTIONS_SHA256)
        self.ns = {'ROOT': ROOT, 'OWNER': OWNER, 'Path': HostPath, 'STOP': False,
                   'DEADLINE': time.monotonic() + 60, 'READY': True,
                   'STARTUP_DEADLINE': None, 'time': time, 'json': json,
                   'P': {'protectedContainerIds': [PROTECTED]}, 'STARTED': [],
                   'budget': lambda: {'ok': True}, 'save': Mock(), 'inspect': Mock(),
                   'subprocess': Mock(spec=subprocess)}
        self.ns['subprocess'].check_output.return_value = ''
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(SOURCE), 'exec'), self.ns)
        self.ns['docker'] = Mock(return_value=OWN)
        self.ns['inspect'].return_value = container()

    def test_query_requests_full_ids(self):
        self.assertEqual(self.ns['owned_ids'](), [OWN])
        self.assertIn('--no-trunc', self.ns['docker'].call_args.args)

    def test_abbreviated_result_is_canonicalized_by_inspect(self):
        self.ns['docker'].return_value = OWN[:12]
        self.assertEqual(self.ns['owned_ids'](), [OWN])
        self.ns['inspect'].assert_called_once_with(OWN[:12])

    def test_stopped_reference_is_retained_but_excluded(self):
        self.ns['inspect'].return_value = container(role='reference-v2', running=False)
        self.assertEqual(self.ns['owned_ids'](), [])

    def test_running_reference_is_still_owned(self):
        self.ns['inspect'].return_value = container(role='reference-v2')
        self.assertEqual(self.ns['owned_ids'](), [OWN])

    def test_wrong_root_fails_closed(self):
        self.ns['inspect'].return_value = container(root='/external')
        with self.assertRaisesRegex(RuntimeError, 'ownership mismatch'):
            self.ns['owned_ids']()

    def test_invalid_canonical_id_fails_closed(self):
        self.ns['inspect'].return_value = container(cid=OWN[:12])
        with self.assertRaisesRegex(RuntimeError, 'Invalid full container identity'):
            self.ns['owned_ids']()

    def configure_guard(self, running):
        self.ns['docker'].side_effect = lambda *args: '\n'.join(running) if '--format' in args else OWN[:12]

    def test_guard_allows_owned_and_protected_full_ids(self):
        self.configure_guard([OWN, PROTECTED])
        self.assertEqual(self.ns['guard'](), {'ok': True})

    def test_guard_rejects_external_even_with_same_short_prefix(self):
        self.configure_guard([OWN, PROTECTED, OTHER])
        with self.assertRaisesRegex(RuntimeError, OTHER):
            self.ns['guard']()
        self.assertFalse(any(c.args[0] in {'stop', 'rm', 'restart'}
                             for c in self.ns['docker'].call_args_list))

    def test_cleanup_only_stops_canonical_owned_identity(self):
        self.ns['stop_owned']()
        stopped = [c.args for c in self.ns['docker'].call_args_list if c.args[0] == 'stop']
        self.assertEqual(stopped, [('stop', '--time', '30', OWN)])
        self.assertFalse(any(PROTECTED in c.args for c in self.ns['docker'].call_args_list))


if __name__ == '__main__':
    unittest.main(verbosity=2)
