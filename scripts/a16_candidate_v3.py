"""Frozen research-only A16 shop/SPU adapter. No database or production writes.

Inputs are already-governed monthly observations; absent records remain absent.
Legacy formula internals retain their zero-default history semantics explicitly.
"""
import ast
import datetime as dt
import hashlib
import logging
from pathlib import Path
import types

VERSION = 'a16_shop_cutoff_v3_20261016'
SHOPS = ('JQ-US', 'MT-US', 'RKZ-US', 'SY-US')
SOURCE_FILES = (
    'scripts/audit_sales_forecast_horizons.py',
    'scripts/audit_sales_forecast_v5_experiments.py',
    'scripts/audit_sales_forecast_v5_round3.py',
    'scripts/audit_sales_forecast_v5_round5_asymmetric.py',
    'scripts/audit_sales_forecast_v5_round6_segments.py',
    'scripts/audit_sales_forecast_v5_round7_router.py',
    'scripts/audit_sales_forecast_v5_round11_concentration_anomaly.py',
    'scripts/audit_sales_forecast_v5_round12_robust_season_trust.py',
    'jobs/feishu/forecast_sales_improved.py',
)


def shift(month, n):
    if isinstance(month, str):
        month = dt.date.fromisoformat(month[:7] + '-01')
    x = month.year * 12 + month.month - 1 + n
    return dt.date(x // 12, x % 12 + 1, 1)


def source_hashes(root):
    return {p: {'file_sha256': hashlib.sha256((root/p).read_bytes()).hexdigest(),
                'normalized_sha256': hashlib.sha256((root/p).read_text(encoding='utf-8-sig').encode()).hexdigest()}
            for p in SOURCE_FILES}


def load_pure(root, path, deps=None):
    """Do not import production modules or execute their module-level entrypoints."""
    source = (root/path).read_text(encoding='utf-8-sig')
    body = []
    allowed = {'__future__', 're', 'datetime', 'typing', 'math', 'statistics', 'collections', 'pathlib'}
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import) and all(x.name in allowed for x in node.names):
            body.append(node)
        elif isinstance(node, ast.ImportFrom) and node.module in allowed:
            body.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name not in ('main', 'load_spu_season_map', 'load_spu_category_map'):
            body.append(node)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)) and not any(isinstance(x, ast.Call) for x in ast.walk(node)):
            body.append(node)
    module = types.ModuleType(path)
    module.__dict__.update(deps or {})
    module.logger = logging.getLogger('a16_offline')
    module.__file__ = str(root/path)
    exec(compile(ast.Module(body=body, type_ignores=[]), path, 'exec'), module.__dict__)
    return module


class CutoffHistory(dict):
    def __init__(self, observations, cutoff):
        super().__init__((k, v) for k, v in observations.items() if k[1] < cutoff)
        self.cutoff, self.reads, self.missing = cutoff, 0, 0

    def get(self, key, default=None):
        if key[1] >= self.cutoff:
            raise ValueError('future history read rejected')
        self.reads += 1
        self.missing += key not in self
        return super().get(key, default)


class Candidate:
    def __init__(self, observations, categories, root=None):
        self.root = Path(root or Path(__file__).resolve().parents[1])
        self.observations = dict(observations)
        self.categories = dict(categories)
        self.first = {}
        for (item, month), qty in observations.items():
            if qty is None or qty < 0:
                raise ValueError('history must contain only validated nonnegative observations')
            if qty > 0:
                self.first[item] = min(month, self.first.get(item, month))
        self.ids = sorted(self.first)
        self.cache = {}
        self.calibration_cache = {}
        base = load_pure(self.root, SOURCE_FILES[0])
        self.exp = load_pure(self.root, SOURCE_FILES[1], {'base': base})
        r3 = load_pure(self.root, SOURCE_FILES[2], {'base': base, 'exp': self.exp})
        self.r5 = load_pure(self.root, SOURCE_FILES[3], {'base': base, 'exp': self.exp, 'r3': r3})
        self.r6 = load_pure(self.root, SOURCE_FILES[4], {'base': base, 'exp': self.exp})
        self.r7 = load_pure(self.root, SOURCE_FILES[5])
        self.r11 = load_pure(self.root, SOURCE_FILES[6], {'base': base})
        self.r12 = load_pure(self.root, SOURCE_FILES[7], {'base': base, 'r11': self.r11})

    def features(self, target, horizon):
        target = shift(target, 0)
        key = (target, horizon)
        if key in self.cache:
            return self.cache[key]
        cutoff = shift(target, -horizon)
        history = CutoffHistory(self.observations, cutoff)
        rows = []
        for item in self.ids:
            if self.first[item] >= cutoff:
                continue
            shop, spu = item.split('|', 1)
            if shop not in SHOPS:
                continue
            a3, life = self.exp.a3_lifecycle_shrink(history, item, cutoff, target)
            profile = self.r6.demand_profile(self.r6.trailing_values(history, item, cutoff, 12))[0]
            row = {'目标月': str(target)[:7], '快照月': str(cutoff)[:7], 'Horizon': f'H{horizon}',
                   'SPU': item, 'shop': shop, '生命周期': life, '需求形态': profile,
                   '实际销量': self.observations.get((item, target)),
                   'A3_SPU生命周期收缩': a3, '品类': self.categories.get(spu, '未映射')}
            row['可预测性'] = self.r7.classify_forecastability(row)
            rows.append(row)
        rows = self.r5.enrich(history, rows)
        self.cache[key] = (rows, {'reads': history.reads, 'missing_reads': history.missing})
        return self.cache[key]

    def predict(self, target, horizon):
        if horizon not in (1, 2, 3):
            raise ValueError('candidate supports only H1/H2/H3')
        target = shift(target, 0)
        cutoff = shift(target, -horizon)
        training, month = [], shift('2025-01', 0)
        while month < cutoff:
            rows, _ = self.features(month, horizon)
            training.extend(r for r in rows if r['实际销量'] is not None)
            month = shift(month, 1)
        # Frozen adapter training rule: observed labels only, strictly before cutoff.
        if not training:
            raise ValueError('no historical calibration labels')
        assert all(r['目标月'] < str(cutoff)[:7] for r in training)
        gd, gu, _ = self.r5.choose_params(training)
        features, audit = self.features(target, horizon)
        candidates = self.r5.add_a7(features, gd, gu)
        history = CutoffHistory(self.observations, cutoff)
        first_safe = self.r11.first_sale_map(history)
        result = []
        for shop in SHOPS:
            eligible = [r for r in candidates if r['shop'] == shop and r['可预测性'] == 'ESTABLISHED']
            support = self.r12.category_support(history, first_safe, eligible)
            result.extend(self.r12.apply_trust_gates(history, first_safe, eligible, support))
        meta = {'version': VERSION, 'target': str(target)[:7], 'horizon': horizon,
                'cutoff': str(cutoff)[:7], 'training_rows': len(training),
                'training_months': sorted({r['目标月'] for r in training}),
                'gamma_down': gd, 'gamma_up': gu, 'feature_audit': audit,
                'trust_reads': history.reads, 'trust_missing_reads': history.missing,
                'metadata_basis': 'CURRENT_NOT_POINT_IN_TIME', 'output_rows': len(result)}
        return {r['SPU']: r for r in result}, {r['SPU']: r for r in features}, meta


def shadow_envelope(v4_rows, candidate_call, enabled=False):
    """In-memory design probe only. Never mutates V4 or persists either result."""
    import copy
    production = copy.deepcopy(v4_rows)
    if not enabled:
        return {'production': production, 'shadow': None, 'status': 'DISABLED'}
    try:
        shadow = candidate_call(copy.deepcopy(v4_rows))
        return {'production': production, 'shadow': shadow, 'status': 'SHADOW_ONLY'}
    except Exception as exc:
        return {'production': production, 'shadow': None, 'status': 'FAILED_ISOLATED',
                'error_class': type(exc).__name__}
