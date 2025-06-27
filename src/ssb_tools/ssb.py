# Copyright (c) 2025, André Araújo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import requests
import time
import urllib3

from ssb_tools.utils import print_json

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _update_payload(job, sql=None, use_savepoint=None, per_job=False, session=False, batch=False,
                    streaming=False):
    payload = {
        "sql": sql or job['sql'],
        "mv_endpoints": job['mv_endpoints'],
        "job_config": {
            "job_name": job['name'],
            "autoscaler_config": job['autoscaler_config'],
            "checkpoint_config": job['checkpoint_config'],
            "kubernetes_config": job['kubernetes_config'],
            "mv_config": job['mv_config'],
            "runtime_config": job['runtime_config'],
        },
    }
    del payload['job_config']['mv_config']['not_indexed_columns']  # this is not a valid option in the rest API
    if per_job:
        payload['job_config']["runtime_config"]["execution_mode"] = 'PER_JOB'
    elif session:
        payload['job_config']["runtime_config"]["execution_mode"] = 'SESSION'
    if streaming:
        payload['job_config']['runtime_config']['runtime_mode'] = 'STREAMING'
    elif batch:
        payload['job_config']['runtime_config']['runtime_mode'] = 'BATCH'
    if use_savepoint is not None:
        payload['job_config']['runtime_config']['start_with_savepoint'] = use_savepoint
    return payload


def _stop_payload(job=None, savepoint=False):
    payload = {
        "savepoint": savepoint or job['runtime_config']['start_with_savepoint'],
    }
    return payload


class SsbTools(object):
    def __init__(self, base_url, username, password, debug=False):
        self.base_url = base_url
        self.username = username
        self.password = password

        self._session = None

        if debug:
            LOG.setLevel(logging.DEBUG)

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = (self.username, self.password)
            self._session.headers.update({
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            })
            self._session.verify = False
        return self._session

    def _api_call(self, method, path, expected_status_codes=None, **kwargs):
        expected_status_codes = expected_status_codes or [requests.codes.ok]
        url = self.base_url + path
        LOG.debug(f'{method} {url}')
        resp = self.session.request(method, url, **kwargs)
        if resp.status_code not in expected_status_codes:
            raise RuntimeError(f"Unexpected response for {method} {url}: {resp}")
        return resp

    def _get(self, path, **kwargs):
        return self._api_call('GET', path, **kwargs)

    def _post(self, path, **kwargs):
        return self._api_call('POST', path, **kwargs)

    def _put(self, path, **kwargs):
        return self._api_call('PUT', path, **kwargs)

    def list_projects(self, project_name=None, project_id=None):
        projects = self._get("/api/v2/projects").json()
        return [p for p in projects
                if (project_name is None or p["name"] == project_name)
                and (project_id is None or p["id"] == project_id)]

    def list_jobs(self, project_name=None, project_id=None, job_names=None, job_ids=None):
        if project_name:
            project_id = self.list_projects(project_name=project_name)[0]["id"]
        jobs = self._get(f"/api/v2/projects/{project_id}/jobs").json()['jobs']
        return [j for j in jobs
                if (not job_ids or j['job_id'] in job_ids) and (not job_names or j['name'] in job_names)]

    def list_jobs_state(self, project_name=None, project_id=None, job_names=None, job_ids=None):
        jobs = self.list_jobs(project_name=project_name, project_id=project_id, job_names=job_names, job_ids=job_ids)
        return [{"job_id": j['job_id'], "job_name": j['name'], "state": j['state']} for j in jobs]

    def _update_job(self, job, use_savepoint=False, per_job=False, session=False, batch=False, streaming=False):
        payload = _update_payload(job=job, use_savepoint=use_savepoint, per_job=per_job,
                                  session=session, batch=batch, streaming=streaming)
        self._put(f"/api/v2/projects/{job['project_id']}/jobs/{job['job_id']}", json=payload)

    def update_jobs(self, project_name=None, project_id=None, job_names=None, job_ids=None, all_jobs=False,
                    use_savepoint=False, per_job=False, session=False,
                    batch=False, streaming=False):
        job_names = job_names or []
        job_ids = job_ids or []
        jobs = self.list_jobs(project_name=project_name, project_id=project_id,
                              job_names=None if all_jobs else job_names,
                              job_ids=None if all_jobs else job_ids)

        for job in jobs:
            if job['state'] != "STOPPED":
                print(f"Job {job['name']} (job_id={job['job_id']}) is already in state {job['state']}.")
            else:
                print(f"Updating job {job['name']} (job_id={job['job_id']})")
                self._update_job(job, use_savepoint=use_savepoint, per_job=per_job, session=session, batch=batch,
                                 streaming=streaming)

    def stop_jobs(self, project_name=None, project_id=None, job_names=None, job_ids=None, all_jobs=False,
                  savepoint=False):
        job_names = job_names or []
        job_ids = job_ids or []
        jobs = self.list_jobs(project_name=project_name, project_id=project_id,
                              job_names=None if all_jobs else job_names,
                              job_ids=None if all_jobs else job_ids)
        for job in jobs:
            if job['state'] == "STOPPED":
                print(f"Job {job['name']} (job_id={job['job_id']}) is already in state {job['state']}.")
            else:
                print(f"Stopping job {job['name']} (job_id={job['job_id']})")
                print(self._post(f"/api/v2/projects/{job['project_id']}/jobs/{job['job_id']}/stop",
                                 json=_stop_payload(job=job, savepoint=savepoint)).text)

    def start_jobs(self, project_name=None, project_id=None, job_names=None, job_ids=None, all_jobs=False,
                   use_savepoint=False, per_job=False, session=False, batch=False, streaming=False):
        job_names = job_names or []
        job_ids = job_ids or []
        jobs = self.list_jobs(project_name=project_name, project_id=project_id,
                              job_names=None if all_jobs else job_names,
                              job_ids=None if all_jobs else job_ids)
        # TODO: Make multiple job starts asynchronous
        for job in jobs:
            if job['state'] != "STOPPED":
                print(f"Job {job['name']} (job_id={job['job_id']}) is already in state {job['state']}.")
            else:
                print(f"Starting job {job['name']} (job_id={job['job_id']})")
                self._update_job(job, use_savepoint=use_savepoint, per_job=per_job, session=session, batch=batch,
                                 streaming=streaming)
                resp = self._post(f"/api/v2/projects/{job['project_id']}/jobs/{job['job_id']}/execute",
                                  expected_status_codes=[200, 500])
                if resp.status_code == 200:
                    print(resp.json())
                else:
                    # TODO: Remove this once the API is improved
                    # Currently the execute call can timeout if the jobs takes a bit too long to start, which
                    # is common for PROD jobs. In this case we check for job status to ensure it started ok
                    attempts = 120
                    job_state = 'STOPPED'
                    while attempts > 0 and job_state in ["STOPPED", "INITIALIZING"]:
                        job = self.list_jobs(project_id=job['project_id'], job_ids=[job['job_id']])[0]
                        job_state = job['state']
                        LOG.debug(f"Job {job['name']} (job_id={job['job_id']}) is in state {job_state}.")
                        if job_state == "STOPPED":
                            attempts -= 1
                            time.sleep(1)
                    if job_state == "RUNNING":
                        print_json({
                            "responses": [
                                {
                                    "type": "job",
                                    "ssb_job_id": job['job_id'],
                                    "job_name": job['name'],
                                    "flink_job_id": job['flink_job_id'],
                                    "sample_id": job['sample_id'],
                                }
                            ]
                        })
                    else:
                        raise RuntimeError(f"Job {job['name']} (job_id={job['job_id']}) failed to start.")
