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

"""Frozen functions from the executed v7 supervisor; supplied mocks own all I/O.

Historical ownership regression input only, with no service-start entry point.
Current v24 execution is verified by its separately frozen environment checks.
"""

def owned_ids():
    result=docker('ps','-aq','--no-trunc','--filter','label=hgbench.owner='+OWNER,'--filter','label=hgbench.root='+str(ROOT))
    result_ids=[]
    for candidate in result.splitlines():
        info=inspect(candidate)
        labels=info['Config'].get('Labels') or {}
        if labels.get('hgbench.owner')!=OWNER or labels.get('hgbench.root')!=str(ROOT):
            raise RuntimeError('Container ownership mismatch: '+candidate)
        # Always use the canonical inspect identity, including if a Docker wrapper
        # returns abbreviated IDs despite the requested --no-trunc option.
        cid=info['Id']
        if len(cid)!=64 or any(c not in '0123456789abcdef' for c in cid):
            raise RuntimeError('Invalid full container identity: '+candidate)
        if (labels.get('hgbench.role') in {'reference','reference-v2'}
            and info['State'].get('Running') is False
            and info['State'].get('Status')=='exited'):
            continue
        result_ids.append(cid)
    return result_ids

def stop_owned():
    result=[]
    rank={'import':-2,'graph':-1,'server':0,'init':1,'store-3':2,'store-2':3,'store-1':4,'pd':5,'net':6}
    containers=[inspect(cid) for cid in owned_ids()]
    for container in sorted(containers,key=lambda c:rank.get(c['Config']['Labels'].get('hgbench.role'),0)):
        cid=container['Id']
        try:
            info=inspect(cid)
            if info['Config']['Labels'].get('hgbench.root')!=str(ROOT):raise RuntimeError('Container ownership mismatch')
            if info['State']['Running']:docker('stop','--time','30',cid,timeout=45)
            info=inspect(cid);result.append({'id':cid,'name':info['Name'],'state':info['State']})
        except Exception as e:result.append({'id':cid,'stopError':str(e)})
    save('stopped.json',{'atUnix':time.time(),'containers':result})

def guard():
    if STOP:raise RuntimeError('Environment stop requested')
    if time.monotonic()>DEADLINE:raise TimeoutError('Environment lifetime exhausted')
    if not READY and STARTUP_DEADLINE is not None and time.monotonic()>STARTUP_DEADLINE:raise TimeoutError('Overall service startup deadline exhausted')
    b=budget()
    if len(Path('/proc/swaps').read_text().strip().splitlines())>1:raise RuntimeError('Host swap became enabled')
    memory={k:int(v.split()[0])*1024 for k,v in (line.split(':',1) for line in Path('/proc/meminfo').read_text().splitlines())}
    if memory['MemAvailable']<4*1024**3:raise RuntimeError('Host available memory below 4 GiB reserve')
    running=set(docker('ps','--no-trunc','--format','{{.ID}}').split())
    allowed=set(owned_ids())|set(P['protectedContainerIds'])
    extra=running-allowed
    if extra:raise RuntimeError('Other Docker workload appeared; yield this attempt: '+','.join(sorted(extra)))
    units=subprocess.check_output(['systemctl','--user','list-units','--state=running','--no-legend','--no-pager'],text=True,timeout=10)
    if any('hgbench' in line and 'hgbench-sf100-real-001' not in line for line in units.splitlines()):raise RuntimeError('Another HG-Bench unit appeared; yielding this attempt')
    for name in STARTED:
        info=inspect(IDS[name]);s=info['State']
        if not s['Running'] or s.get('OOMKilled'):raise RuntimeError('Owned container stopped: '+name+' '+json.dumps(s))
        observed=resource_controls(name,info);LAST_CONTROLS[name]=observed
        save('resource-status.json',{'atUnix':time.time(),'roles':LAST_CONTROLS})
        if observed['pidsEvents'].get('max',0):raise RuntimeError('PID allocation failed: '+name)
        if any(observed['memoryEvents'].get(key,0) for key in ['oom','oom_kill','oom_group_kill']):
            raise RuntimeError('Memory OOM event: '+name)
        if observed['pidsCurrent']>observed['pidsMax']-P['readiness']['minimumPidHeadroom']:
            raise RuntimeError('PID headroom exhausted: '+name)
    return b
