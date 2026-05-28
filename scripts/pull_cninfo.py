#!/usr/bin/env python3
"""巨潮资讯网公司公告 ingester (T1 一手公开).

PRD: docs/thesis-engine-public-sources-prd.md §3.4 (评分 24, 最高).

跟 TrendForce (keyword 路由) 不同, A 股公告按 **ticker 路由**: 只拉 thesis
knowledge.json 里 A 股 Player 的公告, 每条公告归到该公司所属的 segment(s).
例: 胜宏科技 (cn:300476) 公告 → 它在 ai-pcb 当 Player → 自动归 ai-pcb segment.

cninfo API (公开, 无需 key):
  - orgId 解析: POST /new/information/topSearch/query  keyWord=<code>
  - 公告查询:   POST /new/hisAnnouncement/query  stock=<code>,<orgId>&column=<szse|sse>
  - PDF: http://static.cninfo.com.cn/<adjunctUrl>

v1: summary = 公告标题 (A 股标题信息量够: "2026年一季度报告" / "关于...项目进展公告").
    PDF 正文抽取留 v2 (PRD §3.3 投资者关系活动记录表 PDF 抽取).

用法:
  python scripts/pull_cninfo.py                    # 增量拉所有 A 股 player
  python scripts/pull_cninfo.py --since 2026-04-01 --limit-per-ticker 10 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from utils.source_provider import (  # noqa: E402
    ExtractedItem,
    RawItem,
    SourceProvider,
)

logger = logging.getLogger(__name__)


def _post_retry(url: str, data: dict, *, tries: int = 3, timeout: int = 15):
    """cninfo 偶发 502 / timeout (限频), 退避重试. PRD §8 风险表缓解项."""
    last_err = None
    for i in range(tries):
        try:
            resp = requests.post(
                url, data=data, headers={"User-Agent": _UA}, timeout=timeout
            )
            resp.raise_for_status()
            return resp
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(1.5 * (i + 1))  # 1.5s / 3s / 4.5s 退避
    raise last_err  # type: ignore[misc]

_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
_SEARCH_URL = "http://www.cninfo.com.cn/new/information/topSearch/query"
_QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
_PDF_BASE = "http://static.cninfo.com.cn/"
# orgId 缓存 (一次解析多次复用), 落共享数据根
_ORGID_CACHE = "thesis/.cninfo_orgid.json"


def _knowledge_path() -> Path:
    import os

    root = (
        Path(os.environ["SH_QUANT_DATA_DIR"]).expanduser().resolve()
        if os.environ.get("SH_QUANT_DATA_DIR")
        else Path.home() / ".market_data"
    )
    return root / "thesis" / "knowledge.json"


def build_ticker_segment_map() -> dict[str, list[str]]:
    """从 knowledge.json 提取 A 股 Player → 所属 segment id 列表.

    返回 {code: [segment_id, ...]}, code 是 6 位数字 (例 "300476").
    skip referenceOnly + 非 cn: player.
    """
    kpath = _knowledge_path()
    if not kpath.exists():
        raise FileNotFoundError(
            f"knowledge.json 不存在: {kpath}\n  先启动 WealthPilot dev server sync, "
            f"或 cp thesisKnowledge.json 过去"
        )
    data = json.loads(kpath.read_text(encoding="utf-8"))
    out: dict[str, list[str]] = {}
    for seg in data.get("segments", []):
        sid = seg["id"]
        for player in seg.get("players", []):
            if player.get("referenceOnly"):
                continue
            cid = player.get("companyId", "")
            if not cid.startswith("cn:"):
                continue
            code = cid.split(":", 1)[1]
            out.setdefault(code, []).append(sid)
    return out


# 行政噪声标题关键词 — 命中即丢. 确定性 keyword 过滤 (非 LLM 质量评分,
# 符合 PRD §9 ADR). 只滤**纯行政**类 (近零 thesis 信号), 保守为主.
# 保留: 季报/年报/项目投资/产能/订单/合同/中标/经营/重大/业绩/收购.
_NOISE_TITLE_KEYWORDS = (
    "权益分派",
    "独立董事",
    "证券变动月报",
    "翌日披露",
    "股票期权注销",
    "限制性股票",
    "回购注销",
    "监事会",
    "会议决议",  # 董事会第N次会议决议 / 股东会决议 — 治理流程
    "股东会",
    "股东大会",
    "法律意见书",
    "会议资料",
    "会计政策变更",
    "为全资子公司提供担保",
    "为子公司提供担保",
    "持股比例被动稀释",
    "超额配售",
    "稳定价格",
    "高级管理人员",
    "持股5%以上",
    "诉讼",
    "关联交易",
    "股东大会",
    "ESG",
    "環境、社會及管治",
    "环境、社会",  # 简体 ESG 报告
    "社会责任",
)


def _is_noise(title: str) -> bool:
    return any(kw in title for kw in _NOISE_TITLE_KEYWORDS)


def _column_for(code: str) -> str:
    """szse: 0/3 开头 (深市 + 创业板); sse: 6 开头 (沪市 + 科创板 688)."""
    return "sse" if code.startswith("6") else "szse"


def _ts_code(code: str) -> str:
    """6 位 code → ts_code (300476 → 300476.SZ / 600519 → 600519.SH)."""
    suffix = "SH" if code.startswith("6") else "SZ"
    return f"{code}.{suffix}"


class CninfoProvider(SourceProvider):
    source_id = "cninfo"
    source_tier = "T1"
    source_name = "巨潮资讯网公司公告"
    language = "zh"
    storage_kind = "industry_news"
    default_secondhand = False

    def __init__(self, *, config=None, limit_per_ticker: int = 30):
        super().__init__(config=config)
        self.limit_per_ticker = limit_per_ticker
        self.ticker_segments = build_ticker_segment_map()
        self._orgid_cache = self._load_orgid_cache()

    # ── orgId 缓存 ───────────────────────────────────────────────────

    def _orgid_cache_path(self) -> Path:
        return self.data_root() / _ORGID_CACHE

    def _load_orgid_cache(self) -> dict[str, str]:
        p = self._orgid_cache_path()
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_orgid_cache(self) -> None:
        p = self._orgid_cache_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self._orgid_cache, ensure_ascii=False), encoding="utf-8")

    def _resolve_orgid(self, code: str) -> str | None:
        if code in self._orgid_cache:
            return self._orgid_cache[code]
        try:
            resp = _post_retry(_SEARCH_URL, {"keyWord": code, "maxNum": 10})
            for row in resp.json():
                if row.get("code") == code:
                    org = row.get("orgId")
                    if org:
                        self._orgid_cache[code] = org
                        return org
        except Exception as e:
            logger.warning("resolve orgId failed for %s: %s", code, e)
        return None

    # ── fetch ────────────────────────────────────────────────────────

    def fetch_new(self, since: datetime) -> Iterable[RawItem]:
        since_date = since.date()
        for code, segments in self.ticker_segments.items():
            org = self._resolve_orgid(code)
            if not org:
                logger.warning("skip %s (no orgId)", code)
                continue
            try:
                resp = _post_retry(
                    _QUERY_URL,
                    {
                        "stock": f"{code},{org}",
                        "tabName": "fulltext",
                        "pageSize": self.limit_per_ticker,
                        "pageNum": 1,
                        "column": _column_for(code),
                    },
                )
                anns = resp.json().get("announcements") or []
            except Exception as e:
                logger.warning("query announcements failed for %s: %s", code, e)
                continue

            for a in anns:
                ts = a.get("announcementTime", 0) / 1000
                if ts:
                    art_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
                    if art_date < since_date:
                        continue
                # 行政噪声过滤
                if _is_noise(a.get("announcementTitle", "")):
                    continue
                yield RawItem(
                    source_id=self.source_id,
                    fetched_at=datetime.now(timezone.utc),
                    raw_content="",  # 公告无需正文 (v1 用标题), PDF url 在 metadata
                    metadata={
                        "code": code,
                        "ts_code": _ts_code(code),
                        "segments": segments,
                        "title": a.get("announcementTitle", ""),
                        "announcementTime": ts,
                        "adjunctUrl": a.get("adjunctUrl", ""),
                        "secName": a.get("secName", ""),
                    },
                )
            time.sleep(0.3)  # 礼貌错峰
        self._save_orgid_cache()

    # ── extract ──────────────────────────────────────────────────────

    def extract(self, raw: RawItem) -> ExtractedItem | None:
        m = raw.metadata
        title = (m.get("title") or "").strip()
        if not title:
            return None
        ts = m.get("announcementTime", 0)
        published_at = (
            datetime.fromtimestamp(ts, tz=timezone.utc)
            if ts
            else datetime.now(timezone.utc)
        )
        pdf_url = _PDF_BASE + m["adjunctUrl"] if m.get("adjunctUrl") else None
        ts_code = m.get("ts_code", "")
        return ExtractedItem(
            source_id=self.source_id,
            source_tier=self.source_tier,
            source_name=self.source_name,
            title=f"{m.get('secName', '')} {title}".strip(),
            published_at=published_at,
            ingested_at=datetime.now(timezone.utc),
            url=pdf_url,
            language="zh",
            summary=title,  # v1: 标题即摘要; PDF 正文抽取留 v2
            key_numbers=[],
            tickers=[ts_code] if ts_code else [],
            segments=list(m.get("segments", [])),
            secondhand=False,
            cited_source=None,
            raw_path=None,
            content_hash=self.hash_content(self.source_id, ts_code, pdf_url or title),
        )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--since", default=None, help="只拉这天起的公告 (YYYY-MM-DD); 默认增量")
    p.add_argument("--limit-per-ticker", type=int, default=30, help="每只票最多拉几条")
    p.add_argument("--dry-run", action="store_true", help="不落盘, 只打印")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    provider = CninfoProvider(limit_per_ticker=args.limit_per_ticker)
    logger.info("A 股 player 映射: %d 只票", len(provider.ticker_segments))

    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        since = provider.last_ingested_at()

    raws = list(provider.fetch_new(since))
    logger.info("fetched %d raw announcements since %s", len(raws), since.date())

    items: list[ExtractedItem] = []
    for raw in raws:
        ext = provider.extract(raw)
        if ext:
            items.append(ext)
    logger.info("extracted %d items", len(items))
    for it in items[:20]:
        logger.info(
            "  [%s] %s | segs=%s", it.published_at.date(), it.title[:50], it.segments
        )

    if args.dry_run:
        logger.info("dry-run: not writing parquet")
        return 0

    written = provider.write(items)
    logger.info("wrote %d new items to ~/.market_data/industry_news/", written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
