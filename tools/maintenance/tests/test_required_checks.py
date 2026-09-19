import re
import unittest
from pathlib import Path

from tools.maintenance.controller import DEFAULT_CHECKS

WORKFLOWS = Path(__file__).resolve().parents[3] / '.github/workflows'


def pull_request_job_names():
    names = set()
    for workflow in WORKFLOWS.glob('*.yml'):
        text = workflow.read_text()
        triggers = re.search(r'^on:\n((?: .*\n|\n)*)', text, re.M).group(1)
        if not re.search(r'^  pull_request:', triggers, re.M):
            continue
        for job in re.split(r'\n(?=  [\w-]+:\n)', text.split('\njobs:\n', 1)[1]):
            name = re.search(r'^    name: (.+)$', job, re.M)
            condition = re.search(r'^    if: (.+)$', job, re.M)
            if name and not (condition and "github.event_name != 'pull_request'" in condition.group(1)):
                names.add(name.group(1).strip())
    return names


class RequiredChecksTests(unittest.TestCase):
    def test_default_checks_are_jobs_that_run_on_pull_requests(self):
        # A required check that never runs on pull requests keeps repair PRs waiting forever.
        self.assertLessEqual(set(DEFAULT_CHECKS), pull_request_job_names())


if __name__ == '__main__':
    unittest.main()
