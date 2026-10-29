"""Admitted historical episodes executed through the pinned local Codex policy."""
from .archive import load_archive, replay_integrity
from .local_model import SERVED_MODEL, verify_server_identity
from .runner import run_episode


def run_archived(archive, output, *, model_endpoint, codex_bin="codex", max_calls=96,
                 timeout=180, max_steps=4096, actions_per_cutoff=64):
    bundle = load_archive(archive)
    identity = bundle["policy_manifest"]
    # Fail before creating output or making a model request if the service is
    # not serving every artifact admitted by the archive contract.
    verify_server_identity(model_endpoint, identity)
    return run_episode(bundle["manifest"], output, codex=True, model=SERVED_MODEL,
                       model_endpoint=model_endpoint, policy_manifest=identity,
                       archive_contract=replay_integrity(bundle["contract"]),
                       strict_replay=True, scheduled=True, codex_bin=codex_bin,
                       max_calls=max_calls, timeout=timeout, max_steps=max_steps,
                       actions_per_cutoff=actions_per_cutoff)
