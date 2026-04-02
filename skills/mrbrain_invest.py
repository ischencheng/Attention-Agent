#!/usr/bin/env python3
"""Evaluate investment opportunities using MrBrain's investment framework.

Applies the 10-layer investment analysis system distilled from 脑总MrBrain's
43 YouTube videos covering value investing, risk management, macro cycles,
competition theory, and investor psychology.

Usage:
    python skills/mrbrain_invest.py '{"asset": "NVDA", "context": "AI chip leader, PE 60, considering buying"}'
    python skills/mrbrain_invest.py '{"asset": "BTC", "context": "considering 20% portfolio allocation"}'
    python skills/mrbrain_invest.py '{"action": "checklist", "asset": "TSLA", "context": "hold position, up 300%"}'
    python skills/mrbrain_invest.py '{"action": "framework", "topic": "cycle"}'
"""
from __future__ import annotations

import io
import json
import os
import sys

# Ensure UTF-8 output on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

FRAMEWORK_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "MrBrain-invest-principle",
    "investment_framework.md",
)

FRAMEWORK = {
    "层次结构": {
        "第一层_哲学基础": {
            "核心": "认识论与风险观",
            "原则": [
                "休谟问题：过去的规律逻辑上无法推导出未来仍然成立",
                "遍历性陷阱：有破产状态的游戏中期望值毫无意义，只看赔率",
                "幸存者偏差：3亿人掷硬币必然产生300个'大师'",
                "叙事谬误：人脑天生为随机事件编造因果故事",
            ],
        },
        "第二层_风险管理": {
            "核心": "反脆弱体系",
            "原则": [
                "极端斯坦：7500个交易日中只有20天真正重要",
                "杠铃策略：稳定端（安全现金流）+ 投机端（控制规模），避免中间端",
                "切肤之痛：只有承担后果的人才有权做决策",
                "负向黑天鹅防御：永保充足流动性，现金是用来不死的",
                "正向黑天鹅暴露：低代价最大化暴露在极端上行机会",
            ],
        },
        "第三层_复利公式": {
            "核心": "财富 = 本金 × (1 + 收益率)^时间",
            "原则": [
                "本金：场外现金流是一切投资的物质前提",
                "收益率：选择有护城河的商业模式",
                "时间：不追涨杀跌，让时间成为朋友",
                "平常心不是修炼出来的，是用钱堆出来的",
            ],
        },
        "第四层_价值投资": {
            "核心": "段永平+巴菲特体系",
            "原则": [
                "买股票就是买公司——前提是法律有效保护股东权益",
                "五种护城河：无形资产、转换成本、网络效应、成本优势、高效规模",
                "卖出三问：逻辑变了吗→估值贵了吗→有更好标的吗",
                "超越性认知：识别企业超越利润的使命",
                "利润 = 无法感知的超越性价值 - 市场已感知到的价值",
            ],
        },
        "第五层_宏观周期": {
            "核心": "四大嵌套周期 + 三层结构",
            "原则": [
                "基钦3-4年（库存）、朱格拉7-11年（设备）、库兹涅茨15-25年（房地产）、康波50-60年（技术）",
                "三层嵌套：人性恐贪 + 信贷约束 + 技术范式",
                "三层共振 = 几十年一遇的大机会",
                "逆周期行事毫无意义，真正的大钱都是周期的钱",
            ],
        },
        "第六层_货币资产观": {
            "核心": "法币本质与资产配置",
            "原则": [
                "贷款创造存款，借贷就是印钞",
                "所有法币长期贬值，区别只是速度",
                "货币崩盘铁律：印钞权不受有效约束",
                "黄金/BTC = 不受权力控制的自由",
                "债务即权力：内债不是债的前提是债权人与国家同构",
            ],
        },
        "第七层_竞争理论": {
            "核心": "反竞争框架",
            "原则": [
                "竞争一文不值：完全竞争长期利润趋近于零",
                "伟大公司诞生第一天就有反竞争基因",
                "正确路径：先垄断小市场→再扩张",
                "模仿性欲望：垄断欲望传播中介通道 = 结构性垄断",
                "信息不对称溢价：消灭信息甄别成本 = 创造溢价",
            ],
        },
        "第八层_投资心理": {
            "核心": "心智模式",
            "原则": [
                "聪明的弱者：智力被挪用来为失败发明借口",
                "低信任陷阱：避开100个小坑但错过3个改变命运的大机会",
                "普通人四王牌：行业经验、生活观察力、情绪独立性、资金灵活性",
                "第一步不是学知识，而是维护硬件（身体）升级操作系统（心力）",
            ],
        },
        "第九层_技术分析": {
            "核心": "辅助工具，不是主角",
            "原则": [
                "两条公设：价格围绕价值波动 + 钟摆式过度波动",
                "唯一有效工具：趋势+均线（MACD/KDJ已被量化套利至失效）",
                "心法：顺大势逆小势",
                "基本面告诉你买什么，技术分析告诉你何时买",
            ],
        },
        "第十层_投机流量模型": {
            "核心": "接盘流动性 = 流量 × 转化率 × 客单价",
            "原则": [
                "存量流量型（自带顶流）：来得快去得快",
                "增量流量型（病毒传播）：K值>1.5 = 指数爆发",
                "A股/币圈流量为王，美股基本面为王",
            ],
        },
    },
    "买入检查清单": [
        "有充足场外现金流，亏完不影响生活",
        "理解所在市场的制度风险（法律保护、VIE结构等）",
        "商业模式有护城河（五种之一以上）",
        "企业有超越利润的使命（超越性认知）",
        "当前处于周期什么位置（三层嵌套+四大周期）",
        "杠铃策略：这笔投资属于哪一端",
        "遍历性检查：最坏情况下是否会破产",
    ],
    "持有检查清单": [
        "买入核心逻辑是否发生根本性改变",
        "估值是否进入严重高估区间（PEG>2）",
        "是否出现明显更好的标的",
    ],
    "绝对禁止": [
        "不用杠杆（不做归零风险）",
        "不做自己不懂的生意",
        "不在有破产状态的游戏中忽视破产概率",
        "不追涨杀跌（保护时间变量）",
        "不迷信'永不卖出'",
    ],
}


def get_framework(topic: str | None = None) -> dict:
    """Return the full framework or a specific layer/topic."""
    if not topic:
        return FRAMEWORK

    topic_lower = topic.lower()
    # Search in layer names
    for key, val in FRAMEWORK["层次结构"].items():
        if topic_lower in key.lower() or topic_lower in val["核心"].lower():
            return {key: val}

    # Search in principles
    results = {}
    for key, val in FRAMEWORK["层次结构"].items():
        for p in val["原则"]:
            if topic_lower in p.lower():
                results[key] = val
                break
    return results if results else {"message": f"未找到与'{topic}'相关的框架内容"}


def evaluate_asset(asset: str, context: str, action: str = "evaluate") -> dict:
    """Evaluate an asset/investment using MrBrain's framework."""
    result = {
        "asset": asset,
        "framework": "脑总MrBrain投资原则体系",
        "source": "43 videos from youtube.com/@脑总MrBrain",
    }

    if action == "checklist":
        result["type"] = "持有期检查" if "hold" in context.lower() or "持有" in context else "买入前检查"
        if result["type"] == "买入前检查":
            result["checklist"] = FRAMEWORK["买入检查清单"]
        else:
            result["checklist"] = FRAMEWORK["持有检查清单"]
        result["绝对禁止"] = FRAMEWORK["绝对禁止"]
        result["context"] = context
        result["instruction"] = (
            "请逐项对照检查清单评估该资产。"
            "每一项给出 PASS/FAIL/UNKNOWN 及简要理由。"
            "最后给出综合建议。"
        )
    elif action == "framework":
        topic = context if context else None
        result["framework_detail"] = get_framework(topic)
    else:
        # Full evaluation
        result["evaluation_dimensions"] = {
            "1_遍历性检查": {
                "question": f"如果{asset}归零，你还能继续生活和投资吗？",
                "principle": "有破产状态的游戏中期望值无意义",
            },
            "2_杠铃定位": {
                "question": f"{asset}在你的杠铃策略中属于哪一端？",
                "options": ["稳定端（安全现金流）", "投机端（控制规模）", "危险的中间端"],
            },
            "3_护城河分析": {
                "question": f"{asset}拥有哪些护城河？",
                "五种护城河": ["无形资产", "转换成本", "网络效应", "成本优势", "高效规模"],
            },
            "4_超越性认知": {
                "question": f"{asset}是否有超越利润的使命？能凝聚顶级人才和忠诚用户吗？",
                "principle": "利润 = 无法感知的超越性价值 - 市场已感知到的价值",
            },
            "5_周期定位": {
                "question": "当前处于哪些周期的什么位置？",
                "四大周期": ["基钦3-4年", "朱格拉7-11年", "库兹涅茨15-25年", "康波50-60年"],
                "三层结构": ["人性恐贪", "信贷约束", "技术范式"],
            },
            "6_制度风险": {
                "question": f"{asset}所在市场的法律保护程度如何？",
                "考量": ["股东权益保护", "VIE架构风险", "政策风险", "做空机制是否健全"],
            },
            "7_卖出三问": {
                "questions": [
                    "买入核心逻辑是否发生根本性改变？",
                    "估值是否严重高估（PEG>2）？",
                    "是否有明显更好的标的？",
                ],
            },
            "8_流量模型（投机标的适用）": {
                "question": f"{asset}的接盘流动性来自哪里？",
                "formula": "流动性 = 流量 × 转化率 × 客单价",
                "type_check": "存量流量型 or 增量流量型？K值是否>1.5？",
            },
        }
        result["context"] = context
        result["instruction"] = (
            "请根据以上8个维度，结合提供的context信息，"
            "对该资产进行系统性评估。每个维度给出判断和理由，"
            "最后给出综合投资建议（买入/持有/卖出/观望）及仓位建议。"
        )

    return result


def main():
    if len(sys.argv) < 2:
        print(json.dumps({
            "error": "Usage: python mrbrain_invest.py '<json_params>'",
            "examples": [
                '{"asset": "NVDA", "context": "AI chip leader, PE 60"}',
                '{"action": "checklist", "asset": "BTC", "context": "hold 20% position"}',
                '{"action": "framework", "topic": "cycle"}',
                '{"action": "framework"}',
            ],
        }, ensure_ascii=False))
        sys.exit(1)

    try:
        params = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}))
        sys.exit(1)

    action = params.get("action", "evaluate")

    if action == "framework":
        topic = params.get("topic", params.get("context"))
        result = {
            "framework": "脑总MrBrain投资原则体系",
            "source": "43 videos from youtube.com/@脑总MrBrain",
            "detail": get_framework(topic),
        }
    else:
        asset = params.get("asset", "")
        context = params.get("context", "")
        if not asset:
            print(json.dumps({"error": "Missing required field: asset"}))
            sys.exit(1)
        result = evaluate_asset(asset, context, action)

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
