import argparse
import os
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import requests


BASE_URL = "https://gamma-api.polymarket.com/markets"


def to_iso_z(dt: datetime) -> str:
    """将 datetime 转为带 Z 的 ISO8601 字符串。"""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_markets(hours: int = 24, min_liquidity: float = 1000.0, limit: int = 500) -> List[Dict[str, Any]]:
    """
    获取将在未来指定小时内结束、未关闭、且流动性大于指定 USDC 的市场。

    过滤条件：
    - closed=false
    - liquidity_num_min >= min_liquidity
    - end_date_min = 当前时间（UTC）
    - end_date_max = 当前时间 + hours（UTC）

    Args:
        hours: 结束时间窗口（小时），默认 24 小时内结束。
        min_liquidity: 最小流动性（USDC）。
        limit: API 返回上限，默认 500。

    Returns:
        List[Dict]: 满足条件的市场列表。
    """
    now = datetime.now(timezone.utc)
    end_min = to_iso_z(now - timedelta(hours=hours))
    end_max = to_iso_z(now + timedelta(hours=hours))

    params = {
        "closed": "false",
        "liquidity_num_min": min_liquidity,
        "end_date_min": end_min,
        "end_date_max": end_max,
        "limit": limit,
    }

    # 让 requests 自动使用环境中的代理（HTTPS_PROXY/HTTP_PROXY）
    print(f"请求 URL: {BASE_URL}")
    print(f"查询参数: {params}")
    if os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY"):
        print("检测到代理环境变量，HTTP 请求将通过代理发送。")

    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()

    # 响应可能直接是列表，也可能包装在字典中
    if isinstance(data, dict):
        results = data.get("markets") or data.get("results") or []
    else:
        results = data

    if not isinstance(results, list):
        print("返回数据格式异常：期望列表")
        return []

    # 双重过滤与排序（以防 API 端存在边界包含差异）
    def _parse_end_date(m: Dict[str, Any]) -> datetime:
        val = m.get("endDateIso") or m.get("endDate")
        if not val:
            return datetime.max.replace(tzinfo=timezone.utc)
        try:
            s = str(val).strip()
            # 将尾部 Z 标记替换为 +00:00，便于 fromisoformat 解析毫秒等格式
            if s.endswith("Z") or s.endswith("z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
        except Exception:
            # 回退到无毫秒的格式
            try:
                dt = datetime.strptime(str(val), "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                return datetime.max.replace(tzinfo=timezone.utc)

        # 统一为带时区（UTC）
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt

    soon = []
    for m in results:
        # 过滤 closed=false
        if m.get("closed") is True:
            continue

        # 过滤流动性
        liq = m.get("liquidityNum") or m.get("liquidity_num") or 0
        try:
            liq = float(liq)
        except Exception:
            liq = 0.0
        if liq < float(min_liquidity):
            continue

        # 过滤结束时间窗口（冗余校验）
        end_dt = _parse_end_date(m)
        if not (now <= end_dt <= now + timedelta(hours=hours)):
            continue

        soon.append(m)

    # 按结束时间升序排列
    soon.sort(key=_parse_end_date)
    return soon


def main():
    parser = argparse.ArgumentParser(description="获取 24 小时内结束、未关闭、且流动性充足的 Polymarket 市场")
    parser.add_argument("--hours", type=int, default=24, help="结束时间窗口（小时），默认 24")
    parser.add_argument("--liquidity", type=float, default=1000.0, help="最小流动性（USDC），默认 1000")
    parser.add_argument("--limit", type=int, default=500, help="API 返回上限，默认 500")
    parser.add_argument("--out", type=str, default="data/ending_markets.json", help="输出 JSON 文件路径，默认 data/ending_markets.json")
    args = parser.parse_args()

    try:
        markets = fetch_markets(hours=args.hours, min_liquidity=args.liquidity, limit=args.limit)
    except requests.HTTPError as e:
        print(f"HTTP 错误：{e}")
        return
    except requests.RequestException as e:
        print(f"网络错误：{e}")
        return

    print(f"匹配到 {len(markets)} 个市场。\n")
    for i, m in enumerate(markets, start=1):
        end = m.get("endDateIso") or m.get("endDate") or ""
        liq = m.get("liquidityNum") or m.get("liquidity_num")
        outcomes = m.get("outcomes")
        outcome_prices = m.get("outcomePrices") or m.get("outcome_prices")
        print(
            f"[{i}] id={m.get('id')} slug={m.get('slug')} end={end} liquidityNum={liq} question={m.get('question')}"
        )
        print(f"    outcomes={outcomes} outcomePrices={outcome_prices}")

    # 保存为 JSON 文件
    out_path = Path(args.out)
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(markets, f, ensure_ascii=False, indent=2)
        print(f"\n已保存到: {out_path.resolve()}")
    except Exception as e:
        print(f"保存 JSON 文件失败: {e}")


if __name__ == "__main__":
    main()