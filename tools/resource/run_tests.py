#!/usr/bin/env python3
"""Execute already-built test programs inside the constrained Linux cgroup."""
import json
import pathlib
import re
import subprocess
import sys
import time

cgroup = pathlib.Path('/sys/fs/cgroup')
report_path = pathlib.Path('/report/report.json')
manifest = json.loads(pathlib.Path('/report/artifacts.json').read_text())
limit = int(sys.argv[1])
limits = {name: (cgroup / name).read_text().strip() for name in ['cpu.max', 'memory.max', 'memory.swap.max']}
child_identity = {'user': 65534, 'group': 65534, 'extra_groups': []}
report = {'limits': limits, 'expected_memory_bytes': limit, 'test_uid': 65534, 'tests': [], 'passed': False}
def save():
    report['memory_peak_bytes'] = int((cgroup / 'memory.peak').read_text())
    report['memory_events'] = dict(line.split() for line in (cgroup / 'memory.events').read_text().splitlines())
    report['cpu_stat'] = dict(line.split() for line in (cgroup / 'cpu.stat').read_text().splitlines())
    report_path.write_text(json.dumps(report, indent=2) + '\n')
try:
    assert limits == {'cpu.max': '100000 100000', 'memory.max': str(limit), 'memory.swap.max': '0'}, limits
    assert manifest, 'no compiled test programs selected'
    actual_uid = subprocess.check_output(['python3', '-c', 'import os; print(os.getuid())'], text=True, **child_identity).strip()
    assert actual_uid == '65534', 'test identity did not become unprivileged'
    print('Verified cgroup: CPU quota 1, memory ' + str(limit) + ', swap 0', flush=True)
    for artifact in manifest:
        name, executable = artifact['name'], artifact['executable']
        listing = subprocess.run([executable, '--list', '--format', 'terse'], text=True, capture_output=True, timeout=60, check=True, **child_identity)
        count = sum(line.endswith(': test') for line in listing.stdout.splitlines())
        assert count > 0, name + ' selected zero tests'
        started = time.monotonic()
        result = subprocess.run([executable, '--test-threads=1'], text=True, capture_output=True, timeout=300, **child_identity)
        print(name + ': ' + result.stdout.strip(), flush=True)
        if result.stderr:
            print(result.stderr[-16000:], file=sys.stderr, flush=True)
        summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', result.stdout)
        success = result.returncode == 0 and len(summaries) == 1 and tuple(map(int, summaries[0])) == (count, 0, 0)
        report['tests'].append({'target': name, 'selected': count, 'passed': success, 'exit_code': result.returncode, 'seconds': round(time.monotonic()-started, 3)})
        save()
        assert success, name + ' failed, skipped tests, or produced no exact successful summary'
    save()
    assert report['memory_events'].get('oom', '0') == '0', 'cgroup OOM occurred'
    assert report['memory_events'].get('oom_kill', '0') == '0', 'cgroup OOM kill occurred'
    report['passed'] = True
finally:
    save()
