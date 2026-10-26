"""Optional Gymnasium adapter for JSON text policies."""
from __future__ import annotations

import json
import string

import gymnasium as gym
from gymnasium import spaces
from gymnasium.envs.registration import register, registry

from .env import FinancialDecisionEnv


def _reject_constant(value):
    raise ValueError(f"Non-finite JSON constant: {value}")


class FinancialDecisionGymEnv(gym.Env):
    """A text action/observation boundary for language-agent training loops.

    Valid JSON actions follow the core environment's action contract. Text
    spaces do not make this a tensor policy compatible with every RL trainer.
    """

    metadata = {"render_modes": ["ansi"]}

    def __init__(self, scenario, max_steps=128, render_mode=None,
                 max_action_chars=32768, max_observation_chars=1000000):
        if render_mode not in (None, "ansi"):
            raise ValueError("render_mode must be None or ansi")
        self.render_mode = render_mode
        self.core = FinancialDecisionEnv(scenario, max_steps=max_steps)
        self.action_space = spaces.Text(max_length=max_action_chars, min_length=0,
                                        charset=string.printable)
        self.observation_space = spaces.Text(max_length=max_observation_chars,
                                             min_length=0, charset=string.printable)
        self._last_observation = None

    def _encode(self, observation):
        encoded = json.dumps(observation, ensure_ascii=True, sort_keys=True,
                             separators=(",", ":"), allow_nan=False)
        if not self.observation_space.contains(encoded):
            raise ValueError("Observation exceeds configured text space; increase max_observation_chars")
        self._last_observation = encoded
        return encoded

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed)
        if seed is not None:
            self.action_space.seed(seed)
        observation, info = self.core.reset(seed=seed, options=options)
        return self._encode(observation), info

    def step(self, action):
        if not self.action_space.contains(action):
            raise ValueError("Action must belong to the configured JSON text space")
        try:
            decoded = json.loads(action, parse_constant=_reject_constant)
        except (ValueError, TypeError):
            decoded = action
        observation, reward, terminated, truncated, info = self.core.step(decoded)
        return self._encode(observation), reward, terminated, truncated, info

    def render(self):
        if self._last_observation is None:
            return "Call reset() to start a financial research episode."
        return json.dumps(json.loads(self._last_observation), ensure_ascii=False, indent=2)

    def close(self):
        return None


if "AttentionFinancialDecision-v0" not in registry:
    register(id="AttentionFinancialDecision-v0",
             entry_point="environments.financial_decision.gym_env:FinancialDecisionGymEnv")
