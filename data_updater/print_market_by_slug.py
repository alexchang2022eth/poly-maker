import argparse
import json
import os
import re
from typing import Optional

import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"
MARKET_BY_SLUG_URL_TMPL = GAMMA_BASE + "/markets/slug/{slug}"


def extract_slug(raw: str) -> str:
    """从可能的完整 URL 或路径中提取 slug；否则返回原字符串。"""
    # 例：https://polymarket.com/market/will-bitcoin-reach-100k-in-2025?tid=12345
    m = re.search(r"/(event|market)/([^/?#]+)", raw)
    if m:
        return m.group(2)
    return raw


def main():
    parser = argparse.ArgumentParser(description="通过 market slug（直接接口）获取并打印市场 JSON")
    parser.add_argument("slug", type=str, help="市场 slug 或包含该 slug 的完整 URL")
    parser.add_argument("--out", type=str, default=None, help="可选：将结果保存到指定文件路径")
    args = parser.parse_args()

    slug_input: str = args.slug.strip()
    slug: str = extract_slug(slug_input)

    if os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY"):
        print("检测到代理环境变量，HTTP 请求将通过代理发送。")

    url = MARKET_BY_SLUG_URL_TMPL.format(slug=slug)
    try:
        resp = requests.get(url, timeout=30)
    except requests.RequestException as e:
        print(f"网络错误：{e}")
        return

    if resp.status_code != 200:
        print(f"HTTP {resp.status_code} 错误：{resp.text}")
        return

    data = resp.json()
    text = json.dumps(data, ensure_ascii=False, indent=2)
    print(text)

    if args.out:
        try:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"已保存到 {args.out}")
        except Exception as e:
            print(f"保存到 {args.out} 失败：{e}")


if __name__ == "__main__":
    main()