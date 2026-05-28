#!/usr/bin/env python3
"""TrendForce Press Center ingester (T3 行业研究 free).

PRD: docs/thesis-engine-public-sources-prd.md §3.4 / §5.

TrendForce 无公开 RSS, 走 HTML 抓:
  - listing: https://www.trendforce.com/presscenter (含文章链接)
  - article URL pattern: /presscenter/news/YYYYMMDD-NNNNN.html
  - article 结构: <article class="presscenter"> 内 <h1> 标题 + .tag-row
    (日期 "27 May 2026" + category tag + author) + <p> 正文段落
  - <meta name="description"> 有现成摘要

segment 路由: hard match thesis knowledge.json 的 segment label / 关键词
(PRD §8 开放问题 5 — 不让 LLM 自由发挥, miss 的写 unmatched_segments).
key_numbers: v1 不在 ingest 阶段抽 (留 thesis agent 自己 web_search 补), v2 加
LLM 抽取.

用法:
  python scripts/pull_trendforce.py            # 增量拉
  python scripts/pull_trendforce.py --since 2026-05-01 --limit 20 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup

# 让脚本能 import utils/ (sh_quant editable package)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from utils.source_provider import (  # noqa: E402
    ExtractedItem,
    RawItem,
    SourceProvider,
)

logger = logging.getLogger(__name__)

_LISTING_URL = "https://www.trendforce.com/presscenter"
_BASE = "https://www.trendforce.com"
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
# /presscenter/news/YYYYMMDD-NNNNN.html
_ARTICLE_RE = re.compile(r"/presscenter/news/(\d{8})-(\d+)\.html")


class TrendForceProvider(SourceProvider):
    source_id = "trendforce_free"
    source_tier = "T3"
    source_name = "TrendForce Press Center"
    language = "en"
    storage_kind = "industry_news"
    default_secondhand = False

    def __init__(self, *, config=None, segment_keywords: dict[str, list[str]] | None = None):
        super().__init__(config=config)
        # segment_id -> [关键词], 用于 hard match. None 时用内置最小集.
        self.segment_keywords = segment_keywords or _DEFAULT_SEGMENT_KEYWORDS

    # ── fetch ────────────────────────────────────────────────────────

    def fetch_new(self, since: datetime) -> Iterable[RawItem]:
        """抓 listing 找文章 URL, 过滤 published >= since, 逐篇拉 HTML."""
        resp = requests.get(_LISTING_URL, headers={"User-Agent": _UA}, timeout=20)
        resp.raise_for_status()
        # 去重收集 (YYYYMMDD, id, url)
        seen: set[str] = set()
        urls: list[tuple[str, str]] = []  # (yyyymmdd, full_url)
        for m in _ARTICLE_RE.finditer(resp.text):
            yyyymmdd, nid = m.group(1), m.group(2)
            key = f"{yyyymmdd}-{nid}"
            if key in seen:
                continue
            seen.add(key)
            urls.append((yyyymmdd, f"{_BASE}/presscenter/news/{key}.html"))

        since_date = since.date()
        for yyyymmdd, url in urls:
            try:
                art_date = datetime.strptime(yyyymmdd, "%Y%m%d").date()
            except ValueError:
                continue
            if art_date < since_date:
                continue
            try:
                art_resp = requests.get(url, headers={"User-Agent": _UA}, timeout=20)
                art_resp.raise_for_status()
            except Exception as e:
                logger.warning("fetch article failed %s: %s", url, e)
                continue
            yield RawItem(
                source_id=self.source_id,
                fetched_at=datetime.now(timezone.utc),
                raw_content=art_resp.text,
                metadata={"url": url, "yyyymmdd": yyyymmdd},
            )

    # ── extract ──────────────────────────────────────────────────────

    def extract(self, raw: RawItem) -> ExtractedItem | None:
        url = raw.metadata.get("url", "")
        soup = BeautifulSoup(raw.raw_content, "lxml")
        article = soup.select_one("article.presscenter")
        if article is None:
            logger.warning("no <article.presscenter> in %s (404 or layout change?)", url)
            return None

        h1 = article.find("h1")
        title = h1.get_text(strip=True) if h1 else ""
        if not title:
            return None

        # 日期: .tag-row 里 fa-calendar 后面那段 "27 May 2026"
        published_at = self._parse_date(article, raw.metadata.get("yyyymmdd", ""))

        # 摘要: meta description 优先, 否则第一段
        summary = ""
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            summary = meta["content"].strip()
        if not summary:
            first_p = article.find("p")
            summary = first_p.get_text(strip=True)[:500] if first_p else ""

        # 正文 (用于 segment hard match)
        body = " ".join(p.get_text(" ", strip=True) for p in article.find_all("p"))
        haystack = f"{title} {body}".lower()
        segments = [
            sid
            for sid, kws in self.segment_keywords.items()
            if any(_kw_match(kw, haystack) for kw in kws)
        ]

        return ExtractedItem(
            source_id=self.source_id,
            source_tier=self.source_tier,
            source_name=self.source_name,
            title=title,
            published_at=published_at,
            ingested_at=datetime.now(timezone.utc),
            url=url,
            language="en",
            summary=summary,
            key_numbers=[],  # v1 不在 ingest 抽; thesis agent web_search 补
            tickers=[],
            segments=segments,
            secondhand=False,
            cited_source=None,
            raw_path=None,
            content_hash=self.hash_content(self.source_id, url),
        )

    @staticmethod
    def _parse_date(article, yyyymmdd_fallback: str) -> datetime:
        cal = article.select_one(".tag-row .fa-calendar")
        if cal and cal.parent:
            txt = cal.parent.get_text(strip=True)  # "27 May 2026"
            for fmt in ("%d %b %Y", "%d %B %Y"):
                try:
                    return datetime.strptime(txt, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        # fallback: URL 里的 yyyymmdd
        try:
            return datetime.strptime(yyyymmdd_fallback, "%Y%m%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return datetime.now(timezone.utc)


def _kw_match(kw: str, haystack: str) -> bool:
    """英文短词用 word-boundary 避免 substring 误命中 (例 'rack' 命中 'tracking').
    含中文 / 多词短语用 substring (中文无词边界, 多词短语本身够特异)."""
    kw = kw.lower()
    is_ascii_word = kw.isascii() and " " not in kw
    if is_ascii_word:
        return re.search(rf"\b{re.escape(kw)}\b", haystack) is not None
    return kw in haystack


# segment_id -> 关键词. 跟 WealthPilot thesisKnowledge.json 的 segment.id 对齐.
# v1 最小集, 后续可从 knowledge.json 自动生成. 只放 TrendForce 英文文章里会出现的词.
_DEFAULT_SEGMENT_KEYWORDS: dict[str, list[str]] = {
    "gpu": ["gpu", "nvidia", "blackwell", "rubin", "ai accelerator"],
    "hbm-memory": ["hbm", "hbm3e", "hbm4", "high bandwidth memory"],
    "advanced-foundry": ["foundry", "2nm", "3nm", "n2", "tsmc"],
    "cowos-packaging": ["cowos", "advanced packaging", "chip-on-wafer"],
    "abf-substrate": ["abf", "substrate", "ibiden"],
    "ai-pcb": ["pcb", "printed circuit board"],
    "ccl": ["ccl", "copper clad laminate"],
    "mlcc": ["mlcc", "multilayer ceramic"],
    "hvdc-power": ["hvdc", "power supply", "800v"],
    "liquid-cooling": ["liquid cooling", "immersion cooling"],
    "odm-rack": ["odm", "server assembly", "rack"],
    "euv-lithography": ["euv", "lithography", "asml"],
    "domestic-foundry": ["smic", "mature node", "china foundry"],
    "domestic-ai-chip": ["cambricon", "domestic ai chip", "ascend"],
    "domestic-semi-equipment": ["semiconductor equipment", "etch", "deposition"],
}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--since", default=None, help="只拉这天起的文章 (YYYY-MM-DD); 默认增量")
    p.add_argument("--limit", type=int, default=None, help="最多处理几篇 (调试)")
    p.add_argument("--dry-run", action="store_true", help="不落盘, 只打印解析结果")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    provider = TrendForceProvider()

    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        since = provider.last_ingested_at()

    raws = list(provider.fetch_new(since))
    if args.limit:
        raws = raws[: args.limit]
    logger.info("fetched %d raw articles since %s", len(raws), since.date())

    items: list[ExtractedItem] = []
    for raw in raws:
        ext = provider.extract(raw)
        if ext:
            items.append(ext)

    logger.info("extracted %d items", len(items))
    for it in items:
        logger.info(
            "  [%s] %s | segments=%s | %s",
            it.published_at.date(), it.title[:60], it.segments, it.url,
        )

    if args.dry_run:
        logger.info("dry-run: not writing parquet")
        return 0

    written = provider.write(items)
    logger.info("wrote %d new items to ~/.market_data/industry_news/", written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
