#!/usr/bin/env python3
"""One bounded daily research cycle; settlement still runs after model failure."""
from __future__ import annotations
import argparse
from contextlib import redirect_stdout
from datetime import datetime, timezone
import fcntl
import io
import json
from pathlib import Path
from types import SimpleNamespace

from ledger import Ledger, utcnow
import run


def render(ledger, state):
    rows = ["# Attention Proxy 持续判断记录", "", f"更新：{utcnow()}", "",
            "按原项目主题发现问题，不设固定组合，不执行订单。价格表现与长期论点验证分开记录。", "",
            f"原始观察 {state['observations']} 条；前瞻判断 {state['forward_judgments']} 次；已结算价格观察 {state['resolved_forward_price_observations']} 个；已结算预注册概率预测 {state['resolved_predeclared_forward_forecasts']} 个。", "",
            "## 当前判断", ""]
    for key, value in state["latest_theses"].items():
        d = value["discovery"]
        rows.extend([f"### {d['title']}（v{value['version']}，{d['action']}）", "", d["statement"], "",
                     f"验证期限：{d.get('thesis_horizon', '见原始判断')}。不确定性：{d.get('uncertainty', '见原始判断')}", "",
                     f"失效条件：{d['invalidation']}", "", f"后续研究：{'；'.join(d.get('next_questions', []))}", "",
                     f"价格观察日期：日 {value['dates']['day']} / 周 {value['dates']['week']} / 月 {value['dates']['month']}；下一次研究检查 {d['next_review_at']}。", "",
                     f"判断摘要校验值：`{value['judgment_hash']}`", ""])
    rows.extend(["## 已结算的前瞻价格观察", "", "| 对象 | 期限 | 对象收益 | SPY 收益 | 相对差 | 收盘路径最大回撤 | 预测 Brier |", "|---|---|---:|---:|---:|---:|---:|"])
    forward = {e["hash"] for e in ledger.entries("judgment") if e["payload"]["mode"] == "forward"}
    for entry in ledger.entries("settlement"):
        p = entry["payload"]
        if p["judgment_hash"] not in forward:
            continue
        o, f = p["outcome"], p.get("forecast_score")
        rows.append(f"| {p['symbol']} | {p['horizon']} | {o['symbol_return']:.2%} | {o['benchmark_return']:.2%} | {o['excess_return']:.2%} | {o.get('max_close_drawdown', 0):.2%} | {f['brier'] if f else '未预注册预测'} |")
    rows.extend(["", "未到期不计成绩；缺数据或公司行动需要调整时保持 unresolved。没有数值预测的价格观察不计命中。长期经营判断仍需按冻结条件独立核验，以上不是组合收益、成交记录或总回报。", "",
                 "RSS 试跑的来源覆盖有限；Docker 全量消息流没有在本机启动。原文、模型轨迹和不可改写账本保存在此目录。", ""])
    return "\n".join(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=run.HERE / "runs/paper")
    parser.add_argument("--codex-bin", default="codex")
    parser.add_argument("--model", default=None)
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--settle-only", action="store_true")
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    with (args.root / "daily.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"status": "already_running"}))
            return
        ledger = Ledger(args.root / "ledger.sqlite3")
        try:
            before = run.report(ledger)
            commands = ["settle"] if args.settle_only else ["cycle", "settle"]
            results = {}
            call_args = SimpleNamespace(input=None, fetch=True, as_of=None, run_root=args.root / "attempts", timeout=args.timeout, model=args.model, codex_bin=args.codex_bin)
            for command in commands:
                output = io.StringIO()
                try:
                    with redirect_stdout(output):
                        getattr(run, command)(call_args, ledger)
                    results[command] = {"status": "ok", "result": json.loads(output.getvalue())}
                except Exception as exc:
                    results[command] = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
            state = run.report(ledger)
            (args.root / "report.json").write_text(json.dumps(state, ensure_ascii=False, indent=2))
            (args.root / "REPORT.zh-CN.md").write_text(render(ledger, state))
            summary = {"at": utcnow(), "results": results,
                       "new_judgments": state["judgments"] - before["judgments"],
                       "new_settlements": state["resolved_price_observations"] - before["resolved_price_observations"],
                       "due_reviews": state["due_reviews"], "report": str(args.root / "REPORT.zh-CN.md")}
            name = datetime.now(timezone.utc).strftime("daily-%Y%m%dT%H%M%S.json")
            (args.root / name).write_text(json.dumps(summary, ensure_ascii=False, indent=2))
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            if any(r["status"] == "error" for r in results.values()):
                raise SystemExit(1)
        finally:
            ledger.close()


if __name__ == "__main__":
    main()
