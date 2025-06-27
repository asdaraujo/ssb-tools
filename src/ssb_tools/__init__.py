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

from argparse import ArgumentParser, BooleanOptionalAction
from getpass import getpass
import yaml

from ssb_tools.ssb import SsbTools
from ssb_tools.utils import print_json


def process_arguments(args):
    if args.config:
        config = yaml.load(open(args.config, 'r'), Loader=yaml.Loader)
    else:
        config = {}

    args.base_url = args.base_url or config.get("base_url")
    args.username = args.username or config.get("username")
    args.password = args.password or config.get("password")
    assert args.base_url, "--base-url must be provided."
    args.base_url = args.base_url.rstrip("/")
    assert args.username, "--username must be provided."
    if not args.password:
        args.password = getpass('Password: ')

    if args.command in ["list-jobs", "list-jobs-state", "stop-jobs", "start-jobs"]:
        assert args.project_name is not None or args.project_id is not None, (
            "Either --project-id or --project-name must be provided.")
        assert args.project_name is None or args.project_id is None, (
            "Only one of --project-id or --project-name must be provided.")

    if args.command in ["stop-jobs", "start-jobs"]:
        assert args.job_name or args.job_id or args.all_jobs, (
            "At least one of --job-name or --job-id or --all must be provided.")

    if args.command in ["start-jobs", "update-jobs"]:
        assert not (args.batch and args.streaming), (
            "Only one of --batch or --streaming must be provided.")
        assert not (args.per_job and args.session), (
            "Only one of --per-job or --session must be provided.")


def _add_project_identifier_args(parser):
    parser.add_argument('-p', '--project-name', action='store', help='Project name.')
    parser.add_argument('-i', '--project-id', action='store', help='Project ID.')


def _add_job_identifier_args(parser):
    parser.add_argument('-j', '--job-name', action='append',
                        help='Name of the job to be stopped. This parameter can be used multiple times.')
    parser.add_argument('-k', '--job-id', action='append',
                        help='ID of the job to be stopped. This parameter can be used multiple times.')


def _add_all_jobs_arg(parser):
    parser.add_argument('-a', '--all-jobs', action='store_true',
                        help='Stop all jobs.')


def _add_job_update_args(parser):
    parser.add_argument('--use-savepoint', action=BooleanOptionalAction, default=False,
                        help='Start the job from the latest savepoint, if specified. Default: Savepoint is not used.')
    parser.add_argument('--per-job', action='store_true', default=False,
                        help='Start the job in Per-Job (PROD) mode. Default: What\'s set in the job.')
    parser.add_argument('--session', action='store_true', default=False,
                        help='Start the job in SESSION mode. Default: What\'s set in the job.')
    parser.add_argument('--batch', action='store_true', default=False,
                        help='Start the job in BATCH mode. Default: Default: What\'s set in the job.')
    parser.add_argument('--streaming', action='store_true', default=False,
                        help='Start the job in STREAMING mode. Default: Default: What\'s set in the job.')


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument('-c', '--config', action='store', help='Configuration file (YAML format).')
    parser.add_argument('-b', '--base-url', action='store', help='SSB API base URL.')
    parser.add_argument('-u', '--username', action='store', help='SSB username.')
    parser.add_argument('-p', '--password', action='store',
                        help='SSB password. If not specified the password will be prompted (recommended).')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debugging output.')

    subparsers = parser.add_subparsers(dest='command')

    subparser = subparsers.add_parser('list-projects', help='List SSB projects.')

    subparser = subparsers.add_parser('list-jobs', help='List SSB jobs')
    _add_project_identifier_args(subparser)
    _add_job_identifier_args(subparser)

    subparser = subparsers.add_parser('list-jobs-state', help='List SSB jobs\' state.')
    _add_project_identifier_args(subparser)
    _add_job_identifier_args(subparser)

    subparser = subparsers.add_parser('update-jobs', help='Update SSB job properties.')
    _add_project_identifier_args(subparser)
    _add_job_identifier_args(subparser)
    _add_all_jobs_arg(subparser)
    _add_job_update_args(subparser)

    subparser = subparsers.add_parser('stop-jobs', help='Stop SSB jobs.')
    _add_project_identifier_args(subparser)
    _add_job_identifier_args(subparser)
    _add_all_jobs_arg(subparser)
    subparser.add_argument('-s', '--savepoint', action='store_true', default=False,
                           help='Create a savepoint when stopping the job. Default: false.')

    subparser = subparsers.add_parser('start-jobs', help='Start SSB jobs.')
    _add_project_identifier_args(subparser)
    _add_job_identifier_args(subparser)
    _add_all_jobs_arg(subparser)
    _add_job_update_args(subparser)

    args = parser.parse_args()
    process_arguments(args)

    ssb = SsbTools(base_url=args.base_url, username=args.username, password=args.password, debug=args.debug)
    if args.command == 'list-projects':
        print_json(ssb.list_projects())
    elif args.command == 'list-jobs':
        print_json(ssb.list_jobs(project_name=args.project_name, project_id=args.project_id,
                                 job_names=args.job_name, job_ids=args.job_id))
    elif args.command == 'list-jobs-state':
        print_json(ssb.list_jobs_state(project_name=args.project_name, project_id=args.project_id,
                                       job_names=args.job_name, job_ids=args.job_id))
    elif args.command == 'update-jobs':
        ssb.update_jobs(project_name=args.project_name, project_id=args.project_id,
                        job_names=args.job_name, job_ids=args.job_id,
                        all_jobs=args.all_jobs, use_savepoint=args.use_savepoint,
                        per_job=args.per_job, session=args.session, batch=args.batch, streaming=args.streaming)
    elif args.command == 'stop-jobs':
        ssb.stop_jobs(project_name=args.project_name, project_id=args.project_id,
                      job_names=args.job_name, job_ids=args.job_id,
                      all_jobs=args.all_jobs, savepoint=args.savepoint)
    elif args.command == 'start-jobs':
        ssb.start_jobs(project_name=args.project_name, project_id=args.project_id,
                       job_names=args.job_name, job_ids=args.job_id,
                       all_jobs=args.all_jobs, use_savepoint=args.use_savepoint,
                       per_job=args.per_job, session=args.session, batch=args.batch, streaming=args.streaming)
    else:
        parser.print_help()
        exit(1)


if __name__ == '__main__':
    main()
