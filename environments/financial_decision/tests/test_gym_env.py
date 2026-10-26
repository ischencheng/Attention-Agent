"""Gymnasium interface and text boundary, using the bundled synthetic episode."""
import importlib.util
import json
from pathlib import Path
import unittest

GYM_AVAILABLE = importlib.util.find_spec("gymnasium") is not None
FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "demo.json"


@unittest.skipUnless(GYM_AVAILABLE, "Install the gym extra for the optional adapter")
class GymAdapterTests(unittest.TestCase):
    def test_official_checker_and_registered_entry_point(self):
        import gymnasium as gym
        from gymnasium.utils.env_checker import check_env
        from environments.financial_decision.gym_env import FinancialDecisionGymEnv

        core = FinancialDecisionGymEnv(FIXTURE)
        check_env(core, skip_render_check=True)
        with gym.make("AttentionFinancialDecision-v0", scenario=FIXTURE) as env:
            text, _ = env.reset(seed=7)
            self.assertTrue(env.observation_space.contains(text))
            self.assertIsInstance(json.loads(text), dict)
            transition = env.step(json.dumps({"type": "wait", "reason": "Await new evidence."}))
            self.assertEqual(len(transition), 5)
            self.assertTrue(env.observation_space.contains(transition[0]))

    def test_invalid_json_consumes_budget_without_research_mutation(self):
        from environments.financial_decision.gym_env import FinancialDecisionGymEnv

        env = FinancialDecisionGymEnv(FIXTURE, max_steps=2)
        before, _ = env.reset(seed=7)
        for action in ("{broken", '{"type":"forecast","probability":NaN}'):
            text, reward, terminated, truncated, info = env.step(action)
            self.assertLess(reward, 0)
            self.assertTrue(info["rejection"])
            for key in ("theses", "forecasts", "current_time"):
                self.assertEqual(json.loads(before)[key], json.loads(text)[key])
        self.assertFalse(terminated)
        self.assertTrue(truncated)

    def test_text_limits_fail_explicitly_and_render_uses_public_observation(self):
        from environments.financial_decision.gym_env import FinancialDecisionGymEnv

        env = FinancialDecisionGymEnv(FIXTURE, render_mode="ansi", max_action_chars=32)
        text, _ = env.reset(seed=7)
        self.assertEqual(json.loads(env.render()), json.loads(text))
        with self.assertRaises(ValueError):
            env.step("x" * 33)
        with self.assertRaisesRegex(ValueError, "Observation exceeds"):
            FinancialDecisionGymEnv(FIXTURE, max_observation_chars=5).reset()


if __name__ == "__main__":
    unittest.main()
