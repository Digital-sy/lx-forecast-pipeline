#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""在 ``fabric_color_stocking`` 上叠加 SPU 级人工颜色映射。

业务口径：

1. SPU 级人工映射键严格为 ``面料名 + 原始颜色编码 + SPU``；
2. 命中后只决定最终飞书颜色，不修改、补全或反推 A2023/B2024 颜色体系；
3. SPU 级人工映射优先于现有确定性规则和历史四字段人工映射；
4. 若 SPU 人工映射存在冲突，或目标颜色已不在当前飞书清单中，则硬失败并进入
   待确认，不允许静默回退到其他自动规则；
5. 未命中 SPU 级人工映射时，完全复用原 ``fabric_color_stocking`` 的既有逻辑。

本模块仍然只提供 read-only dry-run，不写 MySQL / 飞书。
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, MutableMapping, Sequence

from jobs.feishu import fabric_color_stocking as base

logger = base.logger

MATCH_SPU_MANUAL = "SPU人工确认"
REASON_SPU_MANUAL_AMBIGUOUS = "SPU人工确认映射冲突，禁止自动选择"
REASON_SPU_MANUAL_TARGET_MISSING = "SPU人工确认目标颜色不在当前飞书清单"

DEFAULT_SPU_MANUAL_MAPPING_PATH = Path(
    os.getenv(
        "FABRIC_COLOR_SPU_MANUAL_MAPPING_PATH",
        str(base.PROJECT_ROOT / "shared_config" / "fabric_color_manual_mapping_spu.csv"),
    )
)


@dataclass(frozen=True)
class SpuManualMapping:
    fabric_name: str
    color_code: str
    spu: str
    catalog_color_name: str
    sku_name_color: str = ""
    source: str = ""
    group_key: str = ""

    @property
    def key(self) -> tuple[str, str, str]:
        return self.fabric_name, self.color_code, self.spu


class SpuManualMappingCatalog:
    """按 ``面料 + 原色码 + SPU`` 读取人工确认，不参与颜色体系判断。"""

    def __init__(self, entries: Iterable[SpuManualMapping] = ()):
        self.entries = tuple(entries)
        self.by_key: MutableMapping[
            tuple[str, str, str], list[SpuManualMapping]
        ] = defaultdict(list)
        for entry in self.entries:
            self.by_key[entry.key].append(entry)

    @staticmethod
    def forecast_key(
        forecast: base.ForecastSku,
        fabric_name: str,
    ) -> tuple[str, str, str]:
        return (
            fabric_name.strip(),
            str(forecast.color_code or "").strip(),
            str(forecast.spu or "").strip(),
        )

    def decision(
        self,
        forecast: base.ForecastSku,
        fabric_name: str,
        index: base.CatalogIndex,
    ) -> base.MatchDecision | None:
        """返回 SPU 人工决策；无规则时返回 ``None`` 交给旧流程。"""
        entries = self.by_key.get(self.forecast_key(forecast, fabric_name), ())
        if not entries:
            return None

        targets = {
            entry.catalog_color_name
            for entry in entries
            if entry.catalog_color_name
        }
        if len(targets) != 1:
            return base.MatchDecision(
                None,
                reason_code=REASON_SPU_MANUAL_AMBIGUOUS,
                reason=REASON_SPU_MANUAL_AMBIGUOUS,
            )

        target = next(iter(targets))
        candidates = base._unique(
            index.by_name.get((fabric_name, target), ())
        )
        if len(candidates) == 1:
            return base.MatchDecision(candidates[0], method=MATCH_SPU_MANUAL)
        if len(candidates) > 1:
            return base.MatchDecision(
                None,
                reason_code=REASON_SPU_MANUAL_AMBIGUOUS,
                reason=REASON_SPU_MANUAL_AMBIGUOUS,
                candidates=tuple(candidates),
            )
        return base.MatchDecision(
            None,
            reason_code=REASON_SPU_MANUAL_TARGET_MISSING,
            reason=f"{REASON_SPU_MANUAL_TARGET_MISSING}：{target}",
        )

    def audit(self) -> dict[str, Any]:
        target_sets = {
            key: {
                entry.catalog_color_name
                for entry in entries
                if entry.catalog_color_name
            }
            for key, entries in self.by_key.items()
        }
        conflicts = {
            key: targets
            for key, targets in target_sets.items()
            if len(targets) > 1
        }
        return {
            "read_mapping_count": len(self.entries),
            "mapping_key_count": len(self.by_key),
            "conflicting_mapping_key_count": len(conflicts),
            "conflicting_mapping_keys": [
                {
                    "面料名": key[0],
                    "原始颜色编码": key[1],
                    "SPU": key[2],
                    "最终飞书颜色": sorted(targets),
                }
                for key, targets in sorted(conflicts.items())
            ],
        }


def load_spu_manual_mapping_catalog(
    path: Path | None = None,
) -> SpuManualMappingCatalog:
    """加载 SPU 级人工映射；不存在时等价于零条。"""
    path = path or DEFAULT_SPU_MANUAL_MAPPING_PATH
    if not path.exists():
        logger.info(f"SPU级人工映射不存在，按 0 条处理: {path}")
        return SpuManualMappingCatalog()

    required = {
        "面料名",
        "原始颜色编码",
        "SPU",
        "最终飞书颜色",
    }
    entries: list[SpuManualMapping] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(required - set(reader.fieldnames or ()))
        if missing:
            raise RuntimeError(
                f"SPU级人工映射缺少字段: {', '.join(missing)}"
            )
        for row in reader:
            entry = SpuManualMapping(
                fabric_name=str(row.get("面料名") or "").strip(),
                color_code=str(row.get("原始颜色编码") or "").strip(),
                spu=str(row.get("SPU") or "").strip(),
                catalog_color_name=str(row.get("最终飞书颜色") or "").strip(),
                sku_name_color=str(row.get("SKU品名颜色识别") or "").strip(),
                source=str(row.get("确认来源") or "").strip(),
                group_key=str(row.get("分组主键") or "").strip(),
            )
            if all(
                (
                    entry.fabric_name,
                    entry.color_code,
                    entry.spu,
                    entry.catalog_color_name,
                )
            ):
                entries.append(entry)
    return SpuManualMappingCatalog(entries)


@contextmanager
def _spu_matching_context(spu_catalog: SpuManualMappingCatalog):
    """在一次聚合调用内把 SPU 人工确认放到匹配优先级 0。"""
    original_match = base.match_catalog_row
    original_order = base.MATCH_METHOD_ORDER

    def match_with_spu(
        forecast: base.ForecastSku,
        fabric_name: str,
        index: base.CatalogIndex,
        governance_catalog: base.ColorMappingCatalog,
        manual_catalog: base.ManualMappingCatalog | None = None,
    ) -> base.MatchDecision:
        spu_decision = spu_catalog.decision(forecast, fabric_name, index)
        if spu_decision is not None:
            return spu_decision
        return original_match(
            forecast,
            fabric_name,
            index,
            governance_catalog,
            manual_catalog=manual_catalog,
        )

    base.match_catalog_row = match_with_spu
    base.MATCH_METHOD_ORDER = (MATCH_SPU_MANUAL, *original_order)
    try:
        yield
    finally:
        base.match_catalog_row = original_match
        base.MATCH_METHOD_ORDER = original_order


def aggregate_stocking_with_spu(
    forecasts: Sequence[base.ForecastSku],
    fabric_usage_by_spu: Any,
    catalog_rows: Sequence[base.CatalogRow],
    governance_catalog: base.ColorMappingCatalog,
    source_record_count: int | None = None,
    manual_catalog: base.ManualMappingCatalog | None = None,
    spu_manual_catalog: SpuManualMappingCatalog | None = None,
    fuzzy_config: base.FuzzyMatchConfig | None = None,
    target_fabrics: Sequence[str] | None = None,
) -> base.StockingResult:
    """执行原聚合，但在最前面叠加 SPU 级人工确认。"""
    spu_manual_catalog = spu_manual_catalog or SpuManualMappingCatalog()
    with _spu_matching_context(spu_manual_catalog):
        result = base.aggregate_stocking(
            forecasts=forecasts,
            fabric_usage_by_spu=fabric_usage_by_spu,
            catalog_rows=catalog_rows,
            governance_catalog=governance_catalog,
            source_record_count=source_record_count,
            manual_catalog=manual_catalog,
            fuzzy_config=fuzzy_config,
            target_fabrics=target_fabrics,
        )

    for row in result.auto_merge_rows:
        if row.get("匹配方式") == MATCH_SPU_MANUAL:
            row["匹配依据"] = "SPU级人工审核精确键（面料+原始颜色编码+SPU）"

    result.metrics["spu_manual_mapping_audit"] = spu_manual_catalog.audit()
    result.metrics["spu_manual_match_assignment_count"] = (
        result.metrics.get("match_method_counts", {}).get(MATCH_SPU_MANUAL, 0)
    )
    result.metrics["spu_manual_match_meters"] = (
        result.metrics.get("match_method_meters", {}).get(MATCH_SPU_MANUAL, 0)
    )
    return result


async def run_read_only(
    output_dir: Path,
    as_of: date | None = None,
    base_token: str | None = None,
    table_id: str | None = None,
    view_id: str | None = None,
    manual_mapping_path: Path | None = None,
    spu_manual_mapping_path: Path | None = None,
    fuzzy_config: base.FuzzyMatchConfig | None = None,
) -> tuple[base.StockingResult, Path, Path]:
    """执行带 SPU 人工映射的只读 dry-run。"""
    logger.info("开始面料-颜色备货 SPU人工映射 dry-run（不写 MySQL/飞书）")
    governance_catalog = base.ColorMappingCatalog.from_runtime(strict=True)
    catalog_rows, catalog_source_audit = await base.load_catalog_from_feishu(
        base_token=base_token
        or os.getenv("FABRIC_COLOR_CATALOG_BASE_TOKEN", base.DEFAULT_BASE_TOKEN),
        table_id=table_id
        or os.getenv("FABRIC_COLOR_CATALOG_TABLE_ID", base.DEFAULT_CATALOG_TABLE_ID),
        view_id=(
            view_id
            if view_id is not None
            else os.getenv("FABRIC_COLOR_CATALOG_VIEW_ID", base.DEFAULT_CATALOG_VIEW_ID)
        ),
    )
    forecasts, forecast_audit = base.load_forecast_skus(
        governance_catalog,
        as_of=as_of,
    )
    fabric_usage_by_spu = base.load_fabric_usage_by_spu()
    manual_catalog = base.load_manual_mapping_catalog(manual_mapping_path)
    spu_manual_catalog = load_spu_manual_mapping_catalog(spu_manual_mapping_path)

    result = aggregate_stocking_with_spu(
        forecasts=forecasts,
        fabric_usage_by_spu=fabric_usage_by_spu,
        catalog_rows=catalog_rows,
        governance_catalog=governance_catalog,
        source_record_count=catalog_source_audit["source_record_count"],
        manual_catalog=manual_catalog,
        spu_manual_catalog=spu_manual_catalog,
        fuzzy_config=fuzzy_config,
        target_fabrics=base.TARGET_FABRICS,
    )
    result.metrics["forecast_audit"] = forecast_audit
    result.metrics["catalog_source_audit"] = catalog_source_audit
    result.metrics["fabric_mapping_spu_count"] = len(fabric_usage_by_spu)
    result.metrics["manual_mapping_path"] = str(
        manual_mapping_path or base.DEFAULT_MANUAL_MAPPING_PATH
    )
    result.metrics["spu_manual_mapping_path"] = str(
        spu_manual_mapping_path or DEFAULT_SPU_MANUAL_MAPPING_PATH
    )
    result.metrics["source_adapter_disclosures"] = [
        "SPU级人工映射仅使用面料名+原始颜色编码+SPU；不读取颜色体系作为键，也不修改/反推A2023/B2024",
        "SPU级人工映射命中优先于全部自动规则和历史四字段人工映射；目标颜色必须精确存在于当前飞书同面料清单",
        "SPU级人工映射冲突或目标颜色失效时硬失败进入待确认，不静默回退",
        *result.metrics.get("source_adapter_disclosures", []),
    ]
    result.metrics["generated_at"] = datetime.now().isoformat(timespec="seconds")

    workbook_path, metrics_path = base.export_workbook(result, output_dir)
    logger.info(
        "SPU dry-run 完成：可确定 %.2f 米，已确认 %.2f 米（%.2f%%），"
        "待人工确认 %.2f 米，SPU人工命中 %.2f 米",
        result.metrics["calculable_total_meters"],
        result.metrics["confirmed_color_meters"],
        result.metrics["confirmed_color_coverage_pct"],
        result.metrics["manual_review_color_meters"],
        result.metrics["spu_manual_match_meters"],
    )
    logger.info(f"Excel: {workbook_path}")
    logger.info(f"核对指标: {metrics_path}")
    return result, workbook_path, metrics_path


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="只读生成带SPU人工映射的SKU未来4个月→17面料具体飞书颜色用量"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(
            os.getenv("PROCUREMENT_EXPORT_DIR", str(base.PROJECT_ROOT / "exports"))
        ),
    )
    parser.add_argument("--as-of", type=_parse_date, help="预测窗口基准日 YYYY-MM-DD；默认今天")
    parser.add_argument(
        "--manual-mapping",
        type=Path,
        default=base.DEFAULT_MANUAL_MAPPING_PATH,
        help="历史四字段人工映射 CSV",
    )
    parser.add_argument(
        "--spu-manual-mapping",
        type=Path,
        default=DEFAULT_SPU_MANUAL_MAPPING_PATH,
        help="SPU级人工映射 CSV；键=面料名+原始颜色编码+SPU",
    )
    parser.add_argument("--fuzzy-high-score", type=float, default=90)
    parser.add_argument("--fuzzy-medium-score", type=float, default=80)
    parser.add_argument("--fuzzy-low-score", type=float, default=70)
    parser.add_argument("--fuzzy-lead-margin", type=float, default=10)
    args = parser.parse_args()

    fuzzy_config = base.FuzzyMatchConfig(
        high_min_score=args.fuzzy_high_score,
        medium_min_score=args.fuzzy_medium_score,
        low_min_score=args.fuzzy_low_score,
        high_lead_margin=args.fuzzy_lead_margin,
    )
    fuzzy_config.validate()
    asyncio.run(
        run_read_only(
            output_dir=args.output_dir,
            as_of=args.as_of,
            manual_mapping_path=args.manual_mapping,
            spu_manual_mapping_path=args.spu_manual_mapping,
            fuzzy_config=fuzzy_config,
        )
    )


if __name__ == "__main__":
    main()
