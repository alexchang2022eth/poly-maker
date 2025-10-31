import argparse
import json
import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta

import requests

# 兼容以文件路径直接运行：尝试从项目根导入 data_updater 包
try:
    from data_updater.trading_utils import get_clob_client
except ModuleNotFoundError:
    import sys
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if ROOT not in sys.path:
        sys.path.append(ROOT)
    from data_updater.trading_utils import get_clob_client

# 批量获取多个 order books 的 CLOB REST 端点（参考官方文档：POST /<clob-endpoint>/books）
CLOB_BOOKS_URL = "https://clob.polymarket.com/books"


def ensure_data_dir(path: str = "data") -> None:
    if not os.path.exists(path):
        os.makedirs(path)


def pick(value: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in value and value[k] is not None:
            return value[k]
    return default


def extract_reward_fields(m: Dict[str, Any]) -> Dict[str, Any]:
    rewards = m.get("rewards") or {}
    rates = rewards.get("rates") or []
    # 优先选 USDC 的 daily rate（Polygon USDC 地址）
    usdc_addr = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    rewards_daily_rate = None
    for r in rates:
        if r.get("asset_address") == usdc_addr and r.get("rewards_daily_rate") is not None:
            rewards_daily_rate = r.get("rewards_daily_rate")
            break
    return {
        "rewards_daily_rate": rewards_daily_rate,
        "min_size": rewards.get("min_size"),
        "max_spread": rewards.get("max_spread"),
    }


def build_market_record(m: Dict[str, Any]) -> Dict[str, Any]:
    reward_fields = extract_reward_fields(m)
    record: Dict[str, Any] = {
        "question": m.get("question"),
        "market_slug": pick(m, ["market_slug", "slug"], None),
        "condition_id": m.get("condition_id"),
        "question_id": m.get("question_id"),
        "end_date_iso": pick(m, ["end_date_iso", "endDateIso", "endDate"], None),
        "neg_risk": m.get("neg_risk"),
        "minimum_order_size": m.get("minimum_order_size"),
        "minimum_tick_size": m.get("minimum_tick_size"),
        "accepting_orders": m.get("accepting_orders"),
        "rewards_daily_rate": reward_fields.get("rewards_daily_rate"),
        "min_size": reward_fields.get("min_size"),
        "max_spread": reward_fields.get("max_spread"),
        "tokens": [],
    }
    for t in m.get("tokens") or []:
        record["tokens"].append({
            "token_id": t.get("token_id") or t.get("tokenId"),
            "outcome": t.get("outcome"),
            "price": t.get("price"),
            # bids/asks 将通过批量接口填充
            "bids": [],
            "asks": [],
        })
    return record


def get_all_markets_sampling(client) -> List[Dict[str, Any]]:
    cursor = ""
    results: List[Dict[str, Any]] = []
    while True:
        try:
            resp = client.get_sampling_markets(next_cursor=cursor)
            data = resp.get("data", [])
            cursor = resp.get("next_cursor")
            if isinstance(data, list):
                results.extend(data)
            if cursor is None:
                break
        except Exception as ex:
            print(f"分页获取失败：{ex}")
            break
    return results


def parse_iso_to_utc(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str:
        return None
    s = str(iso_str)
    # 支持末尾 'Z' 表示 UTC
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def fetch_order_books_bulk(token_ids: List[str], batch_size: int = 10, timeout: int = 20, debug: bool = False, client=None) -> Dict[str, Dict[str, Any]]:
    """
    通过 CLOB 批量接口获取多个 token 的 orderbook summaries。
    返回映射：token_id -> { bids: [...], asks: [...] }
    文档：POST /<clob-endpoint>/books
    """
    result: Dict[str, Dict[str, Any]] = {}
    # 过滤掉 None
    token_ids = [str(tid) for tid in token_ids if tid]
    def coerce_levels(levels: Any) -> List[Dict[str, Any]]:
        """将 bids/asks 的层级统一转换为可 JSON 序列化的字典数组 {price, size}。"""
        out: List[Dict[str, Any]] = []
        if not levels:
            return out
        for lv in levels:
            if isinstance(lv, dict):
                price = lv.get("price")
                size = lv.get("size")
            else:
                price = getattr(lv, "price", None)
                size = getattr(lv, "size", None)
            if price is None or size is None:
                # 跳过非预期结构
                continue
            out.append({"price": str(price), "size": str(size)})
        return out

    for i in range(0, len(token_ids), batch_size):
        batch = token_ids[i : i + batch_size]
        if not batch:
            continue
        try:
            # 优先使用官方客户端以确保请求负载结构正确
            if client is not None:
                try:
                    from py_clob_client.clob_types import BookParams
                    params = [BookParams(token_id=tid) for tid in batch]
                    books_arr = client.get_order_books(params=params)
                    if debug:
                        print(f"客户端 get_order_books 返回 {len(books_arr)} 条")
                    # books_arr 是列表，元素具有属性访问或字典访问
                    for ob in books_arr:
                        # 兼容对象或字典
                        tid = getattr(ob, "asset_id", None) or getattr(ob, "token_id", None) or (
                            ob.get("asset_id") if isinstance(ob, dict) else None
                        ) or (ob.get("token_id") if isinstance(ob, dict) else None)
                        if not tid:
                            continue
                        bids = getattr(ob, "bids", None) if not isinstance(ob, dict) else ob.get("bids")
                        asks = getattr(ob, "asks", None) if not isinstance(ob, dict) else ob.get("asks")
                        result[str(tid)] = {
                            "bids": coerce_levels(bids),
                            "asks": coerce_levels(asks),
                        }
                    # 成功使用客户端则跳过 HTTP 直接进入下一批
                    continue
                except Exception as ex_client:
                    if debug:
                        print(f"客户端 get_order_books 失败，回退 HTTP：{ex_client}")

            # HTTP 回退遵循批量接口示例：请求体为 JSON 数组，而不是包裹在对象中的 params 字段
            payload = [{"token_id": tid} for tid in batch]
            if debug:
                print(f"请求批次（{len(batch)}）: 首个 token_id={batch[0]}")
                print(f"POST {CLOB_BOOKS_URL} payload 列表长度: {len(payload)} 示例项: {payload[0] if payload else None}")
            resp = requests.post(CLOB_BOOKS_URL, json=payload, timeout=timeout)
            resp.raise_for_status()
            resp_json = resp.json()
            if debug:
                print(f"响应类型: {type(resp_json)}")
                if isinstance(resp_json, dict):
                    print(f"响应字典键: {list(resp_json.keys())}")
                else:
                    print(f"响应长度: {len(resp_json) if hasattr(resp_json, '__len__') else 'N/A'}")

            # 兼容多种响应结构：数组或字典中的某个键
            if isinstance(resp_json, list):
                books_arr = resp_json
            elif isinstance(resp_json, dict):
                books_arr = resp_json.get("books") or resp_json.get("data") or resp_json.get("orderbooks") or []
            else:
                books_arr = []

            if debug:
                print(f"解析后的 books 条目数: {len(books_arr)}")
                if books_arr:
                    sample = books_arr[0]
                    print(f"示例条目键: {list(sample.keys())}")
                    print(f"示例 asset_id: {sample.get('asset_id')} bids_len={len(sample.get('bids') or [])} asks_len={len(sample.get('asks') or [])}")

            for ob in books_arr:
                tid = ob.get("asset_id") or ob.get("token_id")
                if not tid:
                    continue
                result[str(tid)] = {
                    "bids": coerce_levels(ob.get("bids") or []),
                    "asks": coerce_levels(ob.get("asks") or []),
                }
        except Exception as ex:
            print(f"批量获取 orderbooks 失败（batch 首个 token: {batch[0]}）：{ex}")
            # 遇到错误时继续处理后续批次
            continue
    return result


def print_record(record: Dict[str, Any]) -> None:
    slug = record.get("market_slug")
    end_date = record.get("end_date_iso")
    print(f"slug={slug}")
    print(f"  end_date={end_date}")
    print(f"  accepting_orders={record.get('accepting_orders')}")
    print(f"  min_order_size={record.get('minimum_order_size')}  min_tick={record.get('minimum_tick_size')}")
    print(f"  rewards_daily_rate={record.get('rewards_daily_rate')}  min_size={record.get('min_size')}  max_spread={record.get('max_spread')}")
    for t in record.get("tokens") or []:
        print(f"  token_id={t.get('token_id')} outcome={t.get('outcome')} price={t.get('price')} bids_levels={len(t.get('bids') or [])} asks_levels={len(t.get('asks') or [])}")


def main():
    parser = argparse.ArgumentParser(description="使用 Clob 客户端 cursor 分页获取 markets，批量拉取 orderbooks 并保存指定字段 JSON")
    parser.add_argument("--out", type=str, default="data/markets_sampling_books.json", help="输出 JSON 文件路径，默认 data/markets_sampling_books.json")
    parser.add_argument("--batch_size", type=int, default=10, help="批量请求 orderbooks 的 token 数，默认 10")
    parser.add_argument("--debug_books", action="store_true", help="调试模式：打印批量 books 的响应结构")
    parser.add_argument("--hours", type=float, default=24.0, help="筛选结束时间窗口（小时），默认 24")
    parser.add_argument("--print_questions", action="store_true", help="打印全部获取到的 markets 的 question 字段")
    args = parser.parse_args()

    # 在开头打印当前时间（UTC，ISO8601 带 Z）
    now_utc = datetime.now(timezone.utc)
    now_iso_z = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"当前 UTC 时间: {now_iso_z}")

    client = get_clob_client()
    if client is None:
        print("无法创建 Clob 客户端，请检查环境变量 PK")
        return

    if os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY"):
        print("检测到代理环境变量，HTTP 请求将通过代理发送。")

    ensure_data_dir(os.path.dirname(args.out) or "data")

    markets = get_all_markets_sampling(client)
    print(f"共获取 {len(markets)} 个市场")

    # 可选：打印全部市场的 question 字段（在筛选前）
    if args.print_questions:
        print("所有市场的 question：")
        for idx, m in enumerate(markets, start=1):
            q = m.get("question")
            print(f"{idx}. {q}")

    # 按要求筛选：accepting_orders=True 且 end_date_iso ∈ [now-24h, now+args.hours]
    now_utc = datetime.now(timezone.utc)
    forward_hours = max(0.0, args.hours)
    start_cutoff = now_utc - timedelta(hours=24)
    end_cutoff = now_utc + timedelta(hours=forward_hours)
    filtered_markets: List[Dict[str, Any]] = []
    for m in markets:
        accepting = bool(m.get("accepting_orders"))
        end_iso = pick(m, ["end_date_iso", "endDateIso", "endDate"], None)
        end_dt = parse_iso_to_utc(end_iso)
        if accepting and end_dt is not None and start_cutoff <= end_dt <= end_cutoff:
            filtered_markets.append(m)

    print(f"筛选后 {len(filtered_markets)} 个市场（accepting_orders=True 且 end_date ∈ [now-24h, now+{args.hours}h]）")

    # 构造所需字段的记录（仅对筛选结果）
    records: List[Dict[str, Any]] = [build_market_record(m) for m in filtered_markets]
    # 收集所有 token_id 以批量获取 orderbooks
    all_token_ids: List[str] = []
    for r in records:
        for t in r.get("tokens") or []:
            tid = t.get("token_id")
            if tid:
                all_token_ids.append(str(tid))

    books_map = fetch_order_books_bulk(all_token_ids, batch_size=args.batch_size, debug=args.debug_books, client=client)
    if args.debug_books:
        print(f"books_map 收到 {len(books_map)} 个 token 的订单簿摘要")
        # 打印前 3 个样本
        c = 0
        for k, v in books_map.items():
            print(f"样本 token_id={k} bids_levels={len(v.get('bids') or [])} asks_levels={len(v.get('asks') or [])}")
            c += 1
            if c >= 3:
                break

    # 将 bids/asks 合并进 tokens
    for r in records:
        for t in r.get("tokens") or []:
            tid = str(t.get("token_id"))
            ob = books_map.get(tid)
            if ob:
                t["bids"] = ob.get("bids") or []
                t["asks"] = ob.get("asks") or []

    # 打印每个市场的简要信息
    for r in records:
        print_record(r)

    print(f"保存到 {args.out}")
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()