import re
import unittest
from pathlib import Path

from tools.maintenance.controller import DEFAULT_CHECKS

WORKFLOWS = Path(__file__).resolve().parents[3] / '.github/workflows'


def pull_request_jobs():
    jobs = {}
    for workflow in sorted(WORKFLOWS.glob('*.yml')):
        text = workflow.read_text()
        triggers = re.search(r'^on:\n((?: .*\n|\n)*)', text, re.M).group(1)
        if not re.search(r'^  pull_request:', triggers, re.M):
            continue
        for job in re.split(r'\n(?=  [\w-]+:\n)', text.split('\njobs:\n', 1)[1]):
            name = re.search(r'^    name: (.+)$', job, re.M)
            condition = re.search(r'^    if: (.+)$', job, re.M)
            if name:
                jobs[name.group(1).strip()] = condition.group(1) if condition else ''
    return jobs


def skips_pull_requests(condition):
    return "!= 'pull_request'" in condition or ('github.event_name' in condition and "== 'pull_request'" not in condition)


class RequiredChecksTests(unittest.TestCase):
    def test_pull_request_workflows_run_every_job_on_pull_requests(self):
        # review_ready needs every check run on the head to succeed, and a skipped job leaves a skipped run.
        self.assertEqual([name for name, condition in pull_request_jobs().items() if skips_pull_requests(condition)], [])

    def test_default_checks_are_pull_request_jobs(self):
        self.assertLessEqual(set(DEFAULT_CHECKS), set(pull_request_jobs()))


if __name__ == '__main__':
    unittest.main()
