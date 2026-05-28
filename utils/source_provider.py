"""SourceProvider — 公开源 ingestion 抽象基类.

PRD: docs/thesis-engine-public-sources-prd.md §6.1

加新源 = 一个子类 + config/source-tiers.yaml 一行. agent prompt **不需要改**,
因为它消费的是 tier 标签 + 统一 SignalItem schema, 不感知具体 source_id.

存储: ~/.market_data/industry_news/<yyyymm>.parquet (跨 ticker 跨 segment, 按月
聚合). 一手公告类放 ~/.market_data/announcements/<ts_code>.parquet (按 ticker,
跟 stocks/ 风格对齐). 子类指定 storage_kind.

约定:
  - fetch_new(since) → Iterable[RawItem]: 拉新增, 子类实现
  - extract(raw) → ExtractedItem: 解析 raw, 子类实现 (HTML / RSS / PDF 各异)
  - write(items): 基类提供, 通用 parquet append + dedup
  - run(since): 基类提供, fetch → extract → write 的 orchestration

ingest LLM 调用: 子类如需要 LLM 抽取 (例 secondhand cited_source / segment 路由 /
key_numbers), 走 utils.llm.* (sh_quant 自己的 LLM client, 不依赖 TradingAgents).
"""

from __future__ import annotations

import hashlib
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


# ── Tier 类型 ────────────────────────────────────────────────────────

SourceTier = Literal["T1", "T2", "T3", "T4", "T5", "T6", "T7"]
StorageKind = Literal["announcements", "interact", "industry_news"]


# ── Data classes ─────────────────────────────────────────────────────


@dataclass
class RawItem:
    """ingest 阶段拿到的原始 item — 还没解析."""

    source_id: str
    fetched_at: datetime
    raw_content: bytes | str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class KeyNumber:
    """从 evidence 抽出来的关键数字 (例: HBM3e 合约价 = $24/Gb 2026Q1)."""

    metric: str
    value: float
    unit: str
    period: str | None = None  # 例 "2026Q1" / "2026-05"
    original_source: str | None = None  # 二手转述时, 数字真正的源头


@dataclass
class ExtractedItem:
    """解析后的 SignalItem, 落盘 schema 的内存形态.

    跟 WealthPilot src/features/thesis/types.ts SignalItem **保持 schema 一致**.
    """

    source_id: str
    source_tier: SourceTier
    source_name: str
    title: str
    published_at: datetime
    ingested_at: datetime
    url: str | None
    language: Literal["zh", "en"]
    summary: str
    key_numbers: list[KeyNumber] = field(default_factory=list)
    tickers: list[str] = field(default_factory=list)
    segments: list[str] = field(default_factory=list)
    secondhand: bool = False
    cited_source: str | None = None
    raw_path: str | None = None
    # 去重用. SHA-1(source_id + url) 或 SHA-1(source_id + title + published_at[:10]).
    content_hash: str = ""


# ── SourceProvider base ──────────────────────────────────────────────


class SourceProvider(ABC):
    """子类必须实现 source_id / source_tier / fetch_new / extract.

    其余 (write / run / 去重 / 路径解析) 基类提供.
    """

    # 子类覆盖
    source_id: str = ""
    source_tier: SourceTier = "T2"
    source_name: str = ""
    language: Literal["zh", "en"] = "zh"
    storage_kind: StorageKind = "industry_news"
    default_secondhand: bool = False  # T2 子类默认 True

    def __init__(self, *, config: dict[str, Any] | None = None):
        """config = yaml 里这条 source 的全部字段, 子类可读 provider_args."""
        self.config = config or {}

    @abstractmethod
    def fetch_new(self, since: datetime) -> Iterable[RawItem]:
        """拉新增. since 是上次成功 ingest 的时间戳, 子类增量."""

    @abstractmethod
    def extract(self, raw: RawItem) -> ExtractedItem | None:
        """解析单 raw → ExtractedItem. 解析失败返回 None (跳过该条).

        子类负责:
          - 抽 title / published_at / summary
          - 抽 tickers (regex 或 LLM)
          - 抽 segments (跟 thesis knowledge.json segment id 字典 hard match)
          - 抽 key_numbers (LLM 抽数字)
          - 抽 cited_source (T2 secondhand 时)
          - 算 content_hash
        """

    # ── 基类提供的通用方法 ──────────────────────────────────────

    @staticmethod
    def data_root() -> Path:
        """跟 AGENTS.md 同契约: 默认 ~/.market_data, 可被 SH_QUANT_DATA_DIR 覆盖."""
        override = os.environ.get("SH_QUANT_DATA_DIR")
        if override:
            return Path(override).expanduser().resolve()
        return Path.home() / ".market_data"

    def storage_dir(self) -> Path:
        """子类写盘目录. 默认按 storage_kind 走 industry_news/ etc."""
        return self.data_root() / self.storage_kind

    def parquet_path(self, item: ExtractedItem) -> Path:
        """单 item 落到哪个 parquet 文件.

        - industry_news: <yyyymm>.parquet (跨 ticker / 跨 segment 聚合按月)
        - announcements: <ts_code>.parquet (按 ticker, 跟 stocks/ 风格对齐;
          多 ticker 的写多份)
        - interact: <ts_code>.parquet (同上)
        """
        if self.storage_kind == "industry_news":
            yyyymm = item.published_at.strftime("%Y%m")
            return self.storage_dir() / f"{yyyymm}.parquet"
        # ticker-keyed storage — 调用方 (write) 负责按 ticker 拆分写多份
        raise NotImplementedError(
            "ticker-keyed storage (announcements/interact) 需 write() 自行处理"
        )

    @staticmethod
    def hash_content(*parts: str) -> str:
        """SHA-1 去重 key. 子类填 source_id + url (或 title + date) 调."""
        h = hashlib.sha1()
        for p in parts:
            h.update(p.encode("utf-8"))
            h.update(b"|")
        return h.hexdigest()

    def write(self, items: list[ExtractedItem]) -> int:
        """通用 parquet append + dedup.

        - industry_news: 按 published_at 月份分文件, append + dedup by content_hash
        - announcements/interact: 按 ticker 拆分, ts_code 一文件 (子类 override
          或基类后续支持)

        返回新写入的行数 (去重后).
        """
        if not items:
            return 0
        if self.storage_kind != "industry_news":
            raise NotImplementedError(
                f"storage_kind={self.storage_kind!r} write() 需子类 override"
            )

        # 按月份分组
        by_month: dict[str, list[ExtractedItem]] = {}
        for it in items:
            yyyymm = it.published_at.strftime("%Y%m")
            by_month.setdefault(yyyymm, []).append(it)

        total_written = 0
        for yyyymm, month_items in by_month.items():
            path = self.storage_dir() / f"{yyyymm}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)

            new_df = pd.DataFrame([self._item_to_row(it) for it in month_items])

            if path.exists():
                old_df = pd.read_parquet(path)
                combined = pd.concat([old_df, new_df], ignore_index=True)
                # dedup: 同一个 content_hash 只保留第一条 (老的)
                combined = combined.drop_duplicates(
                    subset=["content_hash"], keep="first"
                )
                added = len(combined) - len(old_df)
            else:
                combined = new_df
                added = len(new_df)

            # 按 published_at desc 排序, 方便消费
            combined = combined.sort_values("published_at", ascending=False)

            tmp = path.with_suffix(".parquet.tmp")
            combined.to_parquet(tmp, index=False)
            tmp.replace(path)
            total_written += added
            logger.info(
                "%s: wrote %d new items to %s (total %d in file)",
                self.source_id, added, path.name, len(combined),
            )
        return total_written

    @staticmethod
    def _item_to_row(it: ExtractedItem) -> dict[str, Any]:
        """ExtractedItem → parquet 行. 跟 WealthPilot SignalItem schema 对齐."""
        return {
            "id": it.content_hash,
            "source_id": it.source_id,
            "source_tier": it.source_tier,
            "source_name": it.source_name,
            "title": it.title,
            "published_at": it.published_at,
            "ingested_at": it.ingested_at,
            "url": it.url,
            "language": it.language,
            "summary": it.summary,
            # key_numbers / tickers / segments 作为 JSON 字符串落盘 (parquet 支持 list,
            # 但跨语言读取一致性 JSON 字符串更稳)
            "key_numbers_json": _json_dumps([kn.__dict__ for kn in it.key_numbers]),
            "tickers": it.tickers,
            "segments": it.segments,
            "secondhand": it.secondhand,
            "cited_source": it.cited_source,
            "raw_path": it.raw_path,
            "content_hash": it.content_hash,
        }

    def last_ingested_at(self) -> datetime:
        """上次成功 ingest 的时间戳, 用作 fetch_new 增量起点.

        v1 简单粗暴: 扫所有月份 parquet 取 ingested_at max. 后续可加 _meta.parquet
        单独存这个状态.
        """
        d = self.storage_dir()
        if not d.exists():
            return datetime(2026, 1, 1, tzinfo=timezone.utc)
        max_ts = None
        for p in d.glob("*.parquet"):
            try:
                df = pd.read_parquet(p, columns=["ingested_at", "source_id"])
                df = df[df["source_id"] == self.source_id]
                if len(df) > 0:
                    cur = df["ingested_at"].max()
                    if max_ts is None or cur > max_ts:
                        max_ts = cur
            except Exception as e:
                logger.warning("read %s failed: %s", p, e)
        if max_ts is None:
            return datetime(2026, 1, 1, tzinfo=timezone.utc)
        if pd.isna(max_ts):
            return datetime(2026, 1, 1, tzinfo=timezone.utc)
        return pd.Timestamp(max_ts).to_pydatetime()

    def run(self) -> int:
        """fetch → extract → write 主流程. 返回新增行数."""
        since = self.last_ingested_at()
        logger.info("%s: ingesting since %s", self.source_id, since.isoformat())
        raws = list(self.fetch_new(since))
        logger.info("%s: fetched %d raw items", self.source_id, len(raws))
        items: list[ExtractedItem] = []
        for raw in raws:
            try:
                ext = self.extract(raw)
                if ext is not None:
                    items.append(ext)
            except Exception as e:
                logger.warning("%s: extract failed: %s", self.source_id, e)
        logger.info("%s: extracted %d items", self.source_id, len(items))
        return self.write(items)


# ── yaml 加载 ────────────────────────────────────────────────────────


def _json_dumps(obj: Any) -> str:
    """容错 JSON dump. 给 parquet 落盘用."""
    import json
    return json.dumps(obj, ensure_ascii=False, default=str)


def load_source_config() -> dict[str, dict[str, Any]]:
    """加载 config/source-tiers.yaml, 返回 source_id → config dict 字典.

    只返回 enabled=True 的源. 调试时可设 env SOURCE_TIERS_INCLUDE_DISABLED=1.
    """
    project_root = Path(__file__).resolve().parent.parent
    yaml_path = project_root / "config" / "source-tiers.yaml"
    if not yaml_path.exists():
        raise FileNotFoundError(f"source-tiers.yaml 不存在: {yaml_path}")
    with yaml_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    include_disabled = os.environ.get("SOURCE_TIERS_INCLUDE_DISABLED") == "1"
    out: dict[str, dict[str, Any]] = {}
    for s in data.get("sources", []):
        if not include_disabled and not s.get("enabled"):
            continue
        sid = s["id"]
        out[sid] = s
    return out


def load_tier_descriptions() -> dict[str, str]:
    """给 thesis agent prompt 用 — tier 字段的可读描述."""
    project_root = Path(__file__).resolve().parent.parent
    yaml_path = project_root / "config" / "source-tiers.yaml"
    with yaml_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("tier_descriptions", {})
