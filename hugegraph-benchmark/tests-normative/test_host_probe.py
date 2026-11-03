#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#/

"""Cgroup counter and identity semantics; these fixtures generate no database load."""
import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('host_probe', Path(__file__).resolve().parents[1] / 'tools/sf100-host-probe.py')
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class HostProbeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        values = {'cpu.max': '400000 100000', 'memory.max': '4294967296',
                  'memory.current': '4000000000', 'memory.peak': '4294967296',
                  'memory.swap.max': '0', 'memory.swap.current': '0',
                  'memory.stat': 'anon 100000000\nfile 3900000000\n',
                  'memory.events': 'max 20\noom 0\noom_kill 0\n',
                  'cpu.stat': 'usage_usec 1000000\nthrottled_usec 100\n',
                  'pids.current': '3', 'pids.max': '64', 'pids.events': 'max 0\n'}
        for name, value in values.items():
            (self.root / name).write_text(value)

    def test_cache_is_separate_from_anonymous_memory_and_oom(self):
        state = probe.cgroup_snapshot(self.root)
        self.assertEqual(state['memoryStatBytesAndCounters']['anon'], '100000000')
        self.assertEqual(state['memoryStatBytesAndCounters']['file'], '3900000000')
        self.assertEqual(state['memoryEvents']['oom'], '0')
        self.assertEqual(state['controls']['cpuQuotaMicros'], '400000')
        self.assertEqual(state['unavailable'], [])

    def test_missing_invalid_and_unlimited_are_not_zero(self):
        (self.root / 'memory.peak').unlink()
        (self.root / 'memory.max').write_text('max')
        (self.root / 'cpu.max').write_text('100000 0')
        state = probe.cgroup_snapshot(self.root)
        self.assertIsNone(state['memoryPeakBytes'])
        self.assertIsNone(state['controls']['cpuQuotaMicros'])
        self.assertEqual(state['controls']['memoryMaxBytes'], 'max')
        self.assertIn('cpu.max', state['unavailable'])
        self.assertIn('memory.peak', state['unavailable'])

    def test_counter_delta_and_control_changes(self):
        first = probe.cgroup_snapshot(self.root)
        (self.root / 'cpu.stat').write_text('usage_usec 7000000\nthrottled_usec 200\n')
        (self.root / 'cpu.max').write_text('200000 100000')
        delta = probe.cgroup_delta(first, probe.cgroup_snapshot(self.root), 2)
        self.assertTrue(delta['valid'])
        self.assertTrue(delta['controlsChanged'])
        self.assertEqual(delta['averageCpuCores'], 3)
        self.assertEqual(delta['counterDeltas']['cpuStatMicrosAndCounters']['throttled_usec'], '100')

    def test_missing_replaced_or_reset_group_is_not_combined(self):
        first = probe.cgroup_snapshot(self.root)
        self.assertFalse(probe.cgroup_delta(first, probe.cgroup_snapshot(self.root / 'missing'), 1)['valid'])
        replaced = copy.deepcopy(first)
        replaced['identity']['inode'] = 'another'
        self.assertFalse(probe.cgroup_delta(first, replaced, 1)['valid'])
        reset = copy.deepcopy(first)
        reset['cpuStatMicrosAndCounters']['usage_usec'] = '1'
        self.assertEqual(probe.cgroup_delta(first, reset, 1)['reason'], 'counter-reset')
        self.assertFalse(probe.cgroup_delta(first, first, 0)['valid'])

    def test_exhausted_pid_budget_is_visible_even_without_new_failure(self):
        (self.root / 'pids.current').write_text('64')
        (self.root / 'pids.events').write_text('max 159420\n')
        first = probe.cgroup_snapshot(self.root)
        interval = probe.cgroup_delta(first, first, 3)
        signals = probe.resource_signals(first, first, interval)
        self.assertTrue(interval['valid'])
        self.assertTrue(signals['pidsAtLimitAfter'])
        self.assertEqual(signals['cumulativeFailureEventsAtEnd']['pidAllocationFailures'], '159420')
        self.assertEqual(signals['failureEventsWithinInterval']['pidAllocationFailures'], '0')
        self.assertEqual(signals['failureEventsWithinInterval']['oomEvents'], '0')

    def test_historical_and_new_resource_failures_are_distinct(self):
        (self.root / 'pids.events').write_text('max 159420\n')
        first = probe.cgroup_snapshot(self.root)
        (self.root / 'pids.events').write_text('max 159422\n')
        (self.root / 'memory.events').write_text('max 22\noom 1\noom_kill 1\n')
        after = probe.cgroup_snapshot(self.root)
        signals = probe.resource_signals(first, after, probe.cgroup_delta(first, after, 2))
        self.assertFalse(signals['pidsAtLimitAfter'])
        self.assertEqual(signals['failureEventsWithinInterval']['pidAllocationFailures'], '2')
        self.assertEqual(signals['failureEventsWithinInterval']['oomKills'], '1')
        self.assertIsNone(signals['failureEventsWithinInterval']['oomGroupKills'])

    def test_missing_and_reset_pid_events_never_become_zero(self):
        first = probe.cgroup_snapshot(self.root)
        (self.root / 'pids.events').unlink()
        (self.root / 'pids.current').unlink()
        after = probe.cgroup_snapshot(self.root)
        signals = probe.resource_signals(first, after, probe.cgroup_delta(first, after, 1))
        self.assertIsNone(signals['pidsAtLimitAfter'])
        self.assertIsNone(signals['failureEventsWithinInterval']['pidAllocationFailures'])
        self.assertIn('pids.events', after['unavailable'])
        reset = copy.deepcopy(first)
        first['pidsEvents']['max'] = '2'
        interval = probe.cgroup_delta(first, reset, 1)
        self.assertFalse(interval['valid'])
        self.assertEqual(interval['resetCounters'], ['pidsEvents.max'])
        self.assertTrue(all(value is None for value in
                            probe.resource_signals(first, reset, interval)['failureEventsWithinInterval'].values()))


if __name__ == '__main__':
    unittest.main()
