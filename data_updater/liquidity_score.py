import argparse
import os
import math
from typing import Dict, Any, List, Tuple, Optional

import requests


ORDERBOOK_SUMMARY_URL = "https://clob.polymarket.com/orderbook-summary"
MIDPOINT_URL = "https://clob.polymarket.com/midpoint"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"


def to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def fetch_orderbook_summary(token_id: str) -> Dict[str, Any]:
    resp = requests.get(ORDERBOOK_SUMMARY_URL, params={"token_id": token_id}, timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_midpoint(token_id: str) -> float:
    resp = requests.get(MIDPOINT_URL, params={"token_id": token_id}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    # API 返回 {"mid": "0.43"}
    return to_float(data.get("mid"))


def utility_score(v_cents: float, s_cents: float, b_multiplier: float = 1.0) -> float:
    """S(v, s) = ((v - s)/v)^2 * b，当 s > v 时得分为 0。"""
    if s_cents < 0:
        s_cents = 0
    if s_cents > v_cents:
        return 0.0
    ratio = (v_cents - s_cents) / v_cents
    return (ratio * ratio) * b_multiplier


def score_side(orders: List[Tuple[float, float]], mid: float, v_cents: float, b: float) -> float:
    """
    针对某一侧（bids 或 asks）的订单计算得分之和。
    orders: 列表 [(price, size_in_shares)]
    mid: 中位价
    v_cents: 最大合格价差（单位：美分）
    b: 乘数（默认 1.0）
    """
    total = 0.0
    for price, size in orders:
        s_cents = abs(price - mid) * 100.0
        S = utility_score(v_cents, s_cents, b)
        total += S * size
    return total


def compute_qmin(q_bid: float, q_ask: float, mid: float, c_scale: float = 3.0) -> float:
    """
    根据文档的 Qmin 规则：
    - 若 midpoint ∈ [0.10, 0.90]，允许单边得分：Qmin = max(min(Qone, Qtwo), max(Qone/c, Qtwo/c))
    - 若 midpoint 在此区间外，需双边：Qmin = min(Qone, Qtwo)
    在此近似中，将 Qone 视为 bids 侧总分，Qtwo 视为 asks 侧总分。
    """
    if 0.10 <= mid <= 0.90:
        return max(min(q_bid, q_ask), max(q_bid / c_scale, q_ask / c_scale))
    else:
        return min(q_bid, q_ask)


def propose_orders(mid: float, tick_size: float, v_cents: float, min_size: float, usdc: float, side: str) -> Dict[str, List[Tuple[float, float]]]:
    """
    将给定 USDC 均分到 tick 间隔的价格层，范围在 midpoint±v 之内（以 tick 为步长）。
    - side: "both" | "bid" | "ask"
    返回: {"bids": [(price, size)], "asks": [(price, size)]}
    注意：size 是 shares 数量（USDC = price * shares）。
    """
    v_dollars = v_cents / 100.0
    # 生成价格层（不含 midpoint 本身），确保在 [0,1] 边界内
    n_steps = max(1, int(math.floor(v_dollars / tick_size)))

    bid_prices = []
    ask_prices = []
    for i in range(1, n_steps + 1):
        bp = mid - i * tick_size
        ap = mid + i * tick_size
        if 0.0 < bp < 1.0:
            bid_prices.append(bp)
        if 0.0 < ap <= 1.0:
            ask_prices.append(ap)

    orders = {"bids": [], "asks": []}

    # 按侧分配预算
    if side == "both":
        budget_bid = usdc / 2.0
        budget_ask = usdc / 2.0
    elif side == "bid":
        budget_bid = usdc
        budget_ask = 0.0
    else:
        budget_bid = 0.0
        budget_ask = usdc

    # 按层均分 USDC，然后转换为 shares；不满足最小 size 的层剔除
    def allocate(prices: List[float], budget: float) -> List[Tuple[float, float]]:
        if budget <= 0 or not prices:
            return []
        notional_each = budget / len(prices)
        usable: List[Tuple[float, float]] = []
        for p in prices:
            size = notional_each / p
            if size >= min_size:
                usable.append((p, size))
        # 若无任何层满足最小 size，则尝试用整笔预算在最近一层下单
        if not usable:
            # 选择最近层（距离 mid 最近）
            closest = min(prices, key=lambda x: abs(x - mid)) if prices else None
            if closest is not None:
                size = budget / closest
                if size >= min_size:
                    usable.append((closest, size))
        return usable

    orders["bids"] = allocate(bid_prices, budget_bid)
    orders["asks"] = allocate(ask_prices, budget_ask)
    return orders


def summarize_current_book(ob: Dict[str, Any], mid: float, v_cents: float, b: float, min_size: float) -> Tuple[float, float, float]:
    """
    计算当前 orderbook 的近似得分（仅该 token 的 bids/asks），过滤掉 size < min_size 或价差超过 v 的条目。
    返回 (q_bids, q_asks, qmin)
    """
    bids = [(to_float(e.get("price")), to_float(e.get("size"))) for e in ob.get("bids", [])]
    asks = [(to_float(e.get("price")), to_float(e.get("size"))) for e in ob.get("asks", [])]

    bids = [(p, s) for (p, s) in bids if s >= min_size and abs(p - mid) * 100.0 <= v_cents]
    asks = [(p, s) for (p, s) in asks if s >= min_size and abs(p - mid) * 100.0 <= v_cents]

    q_bids = score_side(bids, mid, v_cents, b)
    q_asks = score_side(asks, mid, v_cents, b)
    qmin = compute_qmin(q_bids, q_asks, mid)
    return q_bids, q_asks, qmin


def fetch_markets_page(limit: int = 500, offset: int = 0, extra_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    params = {"limit": limit, "offset": offset}
    if extra_params:
        params.update(extra_params)
    resp = requests.get(GAMMA_MARKETS_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "markets" in data:
        return data["markets"]
    if isinstance(data, list):
        return data
    return []


def find_market_by_token_id(token_id: str, max_pages: int = 20, page_size: int = 500) -> Optional[Dict[str, Any]]:
    offset = 0
    for _ in range(max_pages):
        markets = fetch_markets_page(limit=page_size, offset=offset)
        if not markets:
            break
        for m in markets:
            cti = m.get("clobTokenIds") or m.get("clob_token_ids")
            if cti and token_id in cti:
                return m
        offset += page_size
    return None


def find_market_by_id_or_slug(market_id: Optional[str] = None, slug: Optional[str] = None, max_pages: int = 20, page_size: int = 500) -> Optional[Dict[str, Any]]:
    offset = 0
    for _ in range(max_pages):
        markets = fetch_markets_page(limit=page_size, offset=offset)
        if not markets:
            break
        for m in markets:
            if market_id and m.get("id") == market_id:
                return m
            if slug and m.get("slug") == slug:
                return m
        offset += page_size
    return None


def get_market_and_tokens(token_id: Optional[str] = None, market_id: Optional[str] = None, slug: Optional[str] = None) -> Tuple[Dict[str, Any], List[str]]:
    market = None
    if market_id or slug:
        market = find_market_by_id_or_slug(market_id=market_id, slug=slug)
    if not market and token_id:
        market = find_market_by_token_id(token_id)
    if not market:
        raise ValueError("未能在 Gamma markets 找到对应市场")
    token_ids = market.get("clobTokenIds") or market.get("clob_token_ids") or []
    if not token_ids or len(token_ids) < 2:
        raise ValueError("市场未返回两个 clobTokenIds")
    return market, token_ids[:2]


def token_contribution(ob: Dict[str, Any], mid: float, v_cents: float, b: float, min_size: float) -> Tuple[float, float, float]:
    bids = [(to_float(e.get("price")), to_float(e.get("size"))) for e in ob.get("bids", [])]
    asks = [(to_float(e.get("price")), to_float(e.get("size"))) for e in ob.get("asks", [])]

    bids = [(p, s) for (p, s) in bids if s >= min_size and abs(p - mid) * 100.0 <= v_cents]
    asks = [(p, s) for (p, s) in asks if s >= min_size and abs(p - mid) * 100.0 <= v_cents]

    q_bids = score_side(bids, mid, v_cents, b)
    q_asks = score_side(asks, mid, v_cents, b)
    q_token = q_bids + q_asks
    return q_bids, q_asks, q_token


def compute_qmin_tokens(q_one: float, q_two: float, mid: float, c_scale: float = 3.0) -> float:
    if 0.10 <= mid <= 0.90:
        return max(min(q_one, q_two), max(q_one / c_scale, q_two / c_scale))
    else:
        return min(q_one, q_two)


def main():
    parser = argparse.ArgumentParser(description="基于 Polymarket 奖励方法的订单得分近似计算（双 token 版本，自动解析 clobTokenIds）")
    parser.add_argument("token_id", type=str, help="token id（若提供 --market_id 或 --slug，将自动忽略此值用于发现市场）")
    parser.add_argument("usdc", type=float, help="计划提供的 USDC 数量（默认在两个 token 间均分）")
    parser.add_argument("--market_id", type=str, default=None, help="Gamma markets 的市场 id")
    parser.add_argument("--slug", type=str, default=None, help="Gamma markets 的市场 slug")
    parser.add_argument("--side", type=str, default="both", choices=["both", "bid", "ask"], help="资金下单侧，默认 both")
    parser.add_argument("--v_cents", type=float, default=3.0, help="最大合格价差（单位：美分），默认 3.0")
    parser.add_argument("--b_mult", type=float, default=1.0, help="乘数 b，默认 1.0")
    parser.add_argument("--c_scale", type=float, default=3.0, help="单边评分缩放因子 c，默认 3.0")
    args = parser.parse_args()

    token_id_input = args.token_id
    usdc = args.usdc
    market_id = args.market_id
    slug = args.slug
    side = args.side
    v_cents = args.v_cents
    b_mult = args.b_mult
    c_scale = args.c_scale

    if os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY"):
        print("检测到代理环境变量，HTTP 请求将通过代理发送。")

    # 找到市场与两个 clobTokenIds
    try:
        market, token_ids = get_market_and_tokens(
            token_id=None if (market_id or slug) else token_id_input,
            market_id=market_id,
            slug=slug,
        )
    except Exception as e:
        print(f"解析市场失败：{e}")
        return

    market_id_val = market.get("id")
    market_slug = market.get("slug")
    print(f"市场识别：id={market_id_val} slug={market_slug} clobTokenIds={token_ids}")

    token_a, token_b = token_ids[0], token_ids[1]

    # 拉取两个 token 的 orderbook 与 midpoint
    ob_a = fetch_orderbook_summary(token_a)
    ob_b = fetch_orderbook_summary(token_b)
    mid_a = fetch_midpoint(token_a)
    mid_b = fetch_midpoint(token_b)

    tick_a = to_float(ob_a.get("tick_size"), 0.01)
    tick_b = to_float(ob_b.get("tick_size"), 0.01)
    min_a = to_float(ob_a.get("min_order_size"), 0.001)
    min_b = to_float(ob_b.get("min_order_size"), 0.001)
    neg_a = bool(ob_a.get("neg_risk", False))
    neg_b = bool(ob_b.get("neg_risk", False))

    print(f"TokenA={token_a} mid={mid_a:.4f} tick_size={tick_a} min_order_size={min_a} neg_risk={neg_a} v_cents={v_cents}")
    print(f"TokenB={token_b} mid={mid_b:.4f} tick_size={tick_b} min_order_size={min_b} neg_risk={neg_b} v_cents={v_cents}")

    # 当前 orderbook 两个 token 的贡献与组合 Qmin
    qb_cur_a, qa_cur_a, qsum_cur_a = token_contribution(ob_a, mid_a, v_cents, b_mult, min_a)
    qb_cur_b, qa_cur_b, qsum_cur_b = token_contribution(ob_b, mid_b, v_cents, b_mult, min_b)
    qmin_cur_tokens = compute_qmin_tokens(qsum_cur_a, qsum_cur_b, mid_a, c_scale)

    print(f"当前订单得分（每 token）：")
    print(f"  TokenA: Q_bids={qb_cur_a:.4f} Q_asks={qa_cur_a:.4f} Q_token={qsum_cur_a:.4f}")
    print(f"  TokenB: Q_bids={qb_cur_b:.4f} Q_asks={qa_cur_b:.4f} Q_token={qsum_cur_b:.4f}")
    print(f"组合 Qmin_current={qmin_cur_tokens:.4f}")

    # 拟下单：将 USDC 在两个 token 间均分
    usdc_each = usdc / 2.0
    prop_a = propose_orders(mid_a, tick_a, v_cents, min_a, usdc_each, side)
    prop_b = propose_orders(mid_b, tick_b, v_cents, min_b, usdc_each, side)

    q_user_a = score_side(prop_a["bids"], mid_a, v_cents, b_mult) + score_side(prop_a["asks"], mid_a, v_cents, b_mult)
    q_user_b = score_side(prop_b["bids"], mid_b, v_cents, b_mult) + score_side(prop_b["asks"], mid_b, v_cents, b_mult)
    qmin_user_tokens = compute_qmin_tokens(q_user_a, q_user_b, mid_a, c_scale)

    print(f"拟下单层数（均分 USDC）：")
    print(f"  TokenA bids={len(prop_a['bids'])} asks={len(prop_a['asks'])}")
    for (p, s) in prop_a["bids"]:
        print(f"    BID  price={p:.4f} size={s:.4f} spread_cents={(abs(p-mid_a)*100.0):.2f}")
    for (p, s) in prop_a["asks"]:
        print(f"    ASK  price={p:.4f} size={s:.4f} spread_cents={(abs(p-mid_a)*100.0):.2f}")
    print(f"  TokenB bids={len(prop_b['bids'])} asks={len(prop_b['asks'])}")
    for (p, s) in prop_b["bids"]:
        print(f"    BID  price={p:.4f} size={s:.4f} spread_cents={(abs(p-mid_b)*100.0):.2f}")
    for (p, s) in prop_b["asks"]:
        print(f"    ASK  price={p:.4f} size={s:.4f} spread_cents={(abs(p-mid_b)*100.0):.2f}")

    print(f"我们的订单得分（每 token）：")
    print(f"  TokenA: Q_user={q_user_a:.4f}")
    print(f"  TokenB: Q_user={q_user_b:.4f}")
    print(f"组合 Qmin_user={qmin_user_tokens:.4f}")

    # 合计 Qmin（当前+我们），以及近似占比
    qsum_total_a = qsum_cur_a + q_user_a
    qsum_total_b = qsum_cur_b + q_user_b
    qmin_total = compute_qmin_tokens(qsum_total_a, qsum_total_b, mid_a, c_scale)

    denom = qmin_user_tokens + qmin_cur_tokens
    share = (qmin_user_tokens / denom) if denom > 0 else 0.0
    print(f"合计 Qmin_total={qmin_total:.4f}")
    print(f"近似样本占比（Qnormal 近似）：{share:.4%}")

    print("说明：该计算是双 token 的近似模型，未进行 size-cutoff 调整 midpoint 的细节处理。实际奖励以官方方法为准。")


if __name__ == "__main__":
    main()