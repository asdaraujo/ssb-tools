from argparse import ArgumentParser
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


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument('-c', '--config', action='store', help='Configuration file (YAML format).')
    parser.add_argument('-b', '--base-url', action='store', help='SSB API base URL.')
    parser.add_argument('-u', '--username', action='store', help='SSB username.')
    parser.add_argument('-p', '--password', action='store',
                        help='SSB password. If not specified the password will be prompted (recommended).')

    subparsers = parser.add_subparsers(dest='command')

    subparser = subparsers.add_parser('list-projects', help='List SSB projects.')

    subparser = subparsers.add_parser('list-jobs', help='List SSB jobs')
    subparser.add_argument('-p', '--project-name', action='store', help='Project name.')
    subparser.add_argument('-i', '--project-id', action='store', help='Project ID.')

    subparser = subparsers.add_parser('list-jobs-state', help='List SSB jobs\' state.')
    subparser.add_argument('-p', '--project-name', action='store', help='Project name.')
    subparser.add_argument('-i', '--project-id', action='store', help='Project ID.')

    subparser = subparsers.add_parser('stop-jobs', help='Stop SSB jobs.')
    subparser.add_argument('-p', '--project-name', action='store', help='Project name.')
    subparser.add_argument('-i', '--project-id', action='store', help='Project ID.')
    subparser.add_argument('-j', '--job-name', action='append',
                           help='Name of the job to be stopped. This parameter can be used multiple times.')
    subparser.add_argument('-k', '--job-id', action='append',
                           help='ID of the job to be stopped. This parameter can be used multiple times.')
    subparser.add_argument('-a', '--all-jobs', action='store_true',
                           help='Stop all jobs.')
    subparser.add_argument('-s', '--savepoint', action='store_true', default=False,
                           help='Create a savepoint when stopping the job. Default: false.')

    subparser = subparsers.add_parser('start-jobs', help='Start SSB jobs.')
    subparser.add_argument('-p', '--project-name', action='store', help='Project name.')
    subparser.add_argument('-i', '--project-id', action='store', help='Project ID.')
    subparser.add_argument('-j', '--job-name', action='append',
                           help='Name of the job to be stopped. This parameter can be used multiple times.')
    subparser.add_argument('-k', '--job-id', action='append',
                           help='ID of the job to be stopped. This parameter can be used multiple times.')
    subparser.add_argument('-a', '--all-jobs', action='store_true',
                           help='Stop all jobs.')

    args = parser.parse_args()
    process_arguments(args)

    ssb = SsbTools(base_url=args.base_url, username=args.username, password=args.password)
    if args.command == 'list-projects':
        print_json(ssb.list_projects())
    elif args.command == 'list-jobs':
        print_json(ssb.list_jobs(project_name=args.project_name, project_id=args.project_id))
    elif args.command == 'list-jobs-state':
        print_json(ssb.list_jobs_state(project_name=args.project_name, project_id=args.project_id))
    elif args.command == 'stop-jobs':
        ssb.stop_jobs(project_name=args.project_name, project_id=args.project_id, job_names=args.job_name,
                      job_ids=args.job_id, all_jobs=args.all_jobs, savepoint=args.savepoint)
    elif args.command == 'start-jobs':
        ssb.start_jobs(project_name=args.project_name, project_id=args.project_id, job_names=args.job_name,
                       job_ids=args.job_id, all_jobs=args.all_jobs)
    else:
        parser.print_help()
        exit(1)


if __name__ == '__main__':
    main()
