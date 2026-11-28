"""Focused regression for the direct-ML batch-prewarm call site.

Run on a Brain node with:
  python3 test_batch_ml_provider_reuse.py [candidate-framework-path]

The completed actual-row/provider parity replay is recorded separately in
batch_ml_provider_reuse_receipt.json; this small test keeps the ready-hit guard
and exact provider date ordering executable without importing the service.
"""
import ast
import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace


def candidate_if(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for fn in (n for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)):
        if fn.name != "_call_model_impl":
            continue
        for node in ast.walk(fn):
            if not isinstance(node, ast.If):
                continue
            test = ast.unparse(node.test)
            if test == "s.batch_model_fn is not None":
                return node
    raise AssertionError("direct ML prewarm guard not found")


def callsite_runner(if_node):
    fn = ast.AsyncFunctionDef(
        name="run", args=ast.arguments(posonlyargs=[], args=[], kwonlyargs=[],
            kw_defaults=[], defaults=[]), body=[if_node, ast.Return(value=ast.Constant(None))],
        decorator_list=[], returns=None, type_comment=None,
    )
    module = ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[]))
    ns = {}
    exec(compile(module, "candidate prewarm branch", "exec"), ns)
    return ns["run"]


def make_env(*, ready, provider_sequence):
    target = datetime(2026, 11, 25)
    seq = [(datetime.fromisoformat(stamp), sign) for stamp, sign in provider_sequence]
    tag = "1_0_0_3_"
    seq_key = tuple((int(dt.timestamp()), sign) for dt, sign in seq)
    calls = []

    class Reverse:
        _seq_codes_cache = {(tag, seq_key): []} if ready else {}

        @staticmethod
        def collect_extremums_forward(*args, **kwargs):
            return list(reversed(seq))

        @staticmethod
        def collect_extremums_back(*args, **kwargs):
            return list(seq)

    async def prewarm(*args, **kwargs):
        calls.append((args, kwargs))

    state = SimpleNamespace(
        batch_model_fn=object(), np_simple_rates={}, ML_EXTREMUM_LIMIT=50,
        NODE_NAME="fixture",
    )
    namespace = {
        "s": state, "np_r": {}, "rl": Reverse, "calc_type": 0,
        "target_date": target, "calc_var": 3, "pair": 1, "day": 0,
        "param": "", "_prewarm_ml_active_cache": prewarm,
        "log": lambda *args, **kwargs: None,
    }
    return namespace, calls, seq, tag, seq_key


def run_one(if_node, ready, provider_sequence):
    run = callsite_runner(if_node)
    env, calls, seq, tag, seq_key = make_env(ready=ready, provider_sequence=provider_sequence)
    run.__globals__.update(env)
    asyncio.run(run())
    if ready:
        assert calls == [], "completed sequence cache hit must not prewarm"
    else:
        assert len(calls) == 1, "cold missing sequence must prewarm once"
        kwargs = calls[0][1]
        actual_dates = kwargs["exact_ext_dates"]
        assert actual_dates == [dt for dt, _ in seq], "provider timestamps/order changed"
        assert kwargs["request_context"] is True
    return len(seq)


def main():
    script_path = Path(__file__).resolve()
    repo_root = next((parent for parent in script_path.parents if (parent / "shared/brain_framework.py").is_file()), None)
    if repo_root is None:
        raise AssertionError("could not locate repository root containing shared/brain_framework.py")
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else repo_root / "shared/brain_framework.py"
    if_node = candidate_if(path)
    # Keep a concrete chronological sequence in the call-site test. The separate
    # actual-data replay verifies this exact handoff against ReverseStore's list.
    control = datetime(2026, 11, 25)
    provider_sequence = [
        [(control - timedelta(hours=12 * i)).isoformat(sep=" "), (-1 if i % 2 else 1)]
        for i in range(1, 51)
    ]
    ready = run_one(if_node, True, provider_sequence)
    cold = run_one(if_node, False, provider_sequence)
    assert ready == cold == 50
    print(json.dumps({
        "candidate_sha256": __import__("hashlib").sha256(path.read_bytes()).hexdigest(),
        "ready_completed_sequence_prewarms": 0,
        "cold_missing_sequence_prewarms": 1,
        "passed_exact_reverse_order_dates": 50,
        "sequence_cache_key": "(model_tag, tuple((int(timestamp), sign), ...))",
        "passed": True,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
