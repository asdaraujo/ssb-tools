import requests
import urllib3

from ssb_tools.utils import print_json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SsbTools(object):
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password

        self._session = None

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = (self.username, self.password)
            self._session.verify = False
        return self._session

    def _api_call(self, method, path, expected_status_codes=None, **kwargs):
        expected_return_codes = expected_status_codes or [requests.codes.ok]
        url = self.base_url + path
        resp = self.session.request(method, url, **kwargs)
        if resp.status_code not in expected_return_codes:
            raise RuntimeError(f"Unexpected response for {method} {url}: {resp}")
        return resp

    def _get(self, path, **kwargs):
        return self._api_call('GET', path, **kwargs)

    def _post(self, path, **kwargs):
        return self._api_call('POST', path, **kwargs)

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
                if (job_ids is None or j['job_id'] in job_ids) or (job_names is None or j['name'] in job_names)]

    def list_jobs_state(self, project_name=None, project_id=None, job_names=None, job_ids=None):
        jobs = self.list_jobs(project_name=project_name, project_id=project_id, job_names=job_names, job_ids=job_ids)
        return [{"job_id": j['job_id'], "job_name": j['name'], "state": j['state']} for j in jobs]

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
                                 json={
                                     'savepoint': savepoint,
                                 }).text)

    def _start_payload(self, job=None, sql=None, execution_mode=None, runtime_mode=None):
        payload = {
            "sql": sql or job['sql'],
            # "selection": False,
            "job_config": {
                "job_name": job['name'],
                "runtime_config": {
                    "execution_mode": execution_mode or job['runtime_config']['execution_mode'],
                }
            }
        }
        if runtime_mode or 'runtime_mode' in job['runtime_config']:
            payload['job_config']['runtime_config']['runtime_mode'] = runtime_mode \
                                                                      or job['runtime_config']['runtime_mode']
        return payload

    def start_jobs(self, project_name=None, project_id=None, job_names=None, job_ids=None, all_jobs=False):
        job_names = job_names or []
        job_ids = job_ids or []

        jobs = self.list_jobs(project_name=project_name, project_id=project_id,
                              job_names=None if all_jobs else job_names,
                              job_ids=None if all_jobs else job_ids)
        for job in jobs:
            if job['state'] != "STOPPED":
                print(f"Job {job['name']} (job_id={job['job_id']}) is already in state {job['state']}.")
            else:
                print(f"Starting job {job['name']} (job_id={job['job_id']})")
                print_json(self._post(f"/api/v2/projects/{job['project_id']}/jobs/{job['job_id']}/execute",
                                      json=self._start_payload(job)).json())

