import importlib.util
import sys
import unittest
from pathlib import Path

path = Path(__file__).resolve().parents[3] / ".github/scripts/dispatch_incident.py"
spec = importlib.util.spec_from_file_location("dispatch_incident", path)
dispatch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dispatch)


class DispatchTests(unittest.TestCase):
    def test_gate_checks_current_author_actor_labels_repository_and_open_state(self):
        owner = {"login": "iamwavecut", "id": 239034}
        event = {"action": "labeled", "sender": owner, "repository": {"full_name": dispatch.REPO}, "issue": {"number": 5}}
        issue = {"number": 5, "user": owner, "state": "open", "labels": [{"name": "agent:created"}, {"name": "agent:queued"}]}
        env = {"GITHUB_REPOSITORY": dispatch.REPO, "GITHUB_EVENT_NAME": "issues",
               "GITHUB_ACTOR": "iamwavecut", "GITHUB_ACTOR_ID": "239034"}
        self.assertTrue(dispatch.validate(event, issue, env))
        self.assertFalse(dispatch.validate(event, {**issue, "labels": [{"name": "agent:created"}]}, env))
        self.assertFalse(dispatch.validate(event, {**issue, "state": "closed"}, env))
        self.assertFalse(dispatch.validate(event, {**issue, "user": {"login": "iamwavecut", "id": 1}}, env))
        self.assertFalse(dispatch.validate(event, issue, {**env, "GITHUB_ACTOR": "attacker"}))
        self.assertFalse(dispatch.validate(event, issue, {**env, "GITHUB_REPOSITORY": "attacker/openplotva"}))


if __name__ == "__main__":
    unittest.main()
