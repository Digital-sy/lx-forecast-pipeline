"""Synthetic-only tests for exact-name Lingxing reference fallback."""
import ast
import copy
import importlib.util
from pathlib import Path
import unittest

MODULE_PATH = Path(__file__).resolve().parents[1] / 'jobs/feishu/fabric_lingxing_name_reference.py'
_spec = importlib.util.spec_from_file_location('name_reference_under_test', MODULE_PATH)
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


def record(**extra):
    row = {'统计类型': '带颜色', '面料编号': 'F1', '面料': '普通面料',
           '颜色体系': 'A2023', '颜色缩写': 'RD', '中文颜色名称': '红色',
           'A2023中文候选': '', 'B2024中文候选': ''}
    row.update(extra)
    return row


def source(sku='F1-R', product_name='普通面料-12#红色'):
    return {'sku': sku, 'product_name': product_name}


def index(rows=None, pairs=None, values=None):
    return mod.build_name_index(rows if rows is not None else [source()],
                                pairs if pairs is not None else [('F1', '普通面料')],
                                values if values is not None else {'F1-R': '12#红色'})


class ExactNameReferenceTests(unittest.TestCase):
    def test_exact_primary_name(self):
        result = mod.resolve_name_reference(record(), index())
        self.assertEqual(result['status'], 'NAME_MATCHED')
        self.assertEqual(result['value'], '12#红色')
        self.assertEqual(result['source_sku'], 'F1-R')
        self.assertEqual(result['matched_name'], '红色')
        self.assertEqual(result['source_color_name'], '红色')
        self.assertEqual(result['match_method'], 'NAME_EXACT')
        self.assertEqual(result['original_system'], 'A2023')

    def test_em_only_b_candidate_with_unknown_system(self):
        rows = [source('F1-EM', '普通面料-35#翡翠绿')]
        ix = index(rows, values={'F1-EM': '35#翡翠绿'})
        row = record(颜色体系='待定', 颜色缩写='EM', 中文颜色名称='', B2024中文候选='翡翠绿色')
        result = mod.resolve_name_reference(row, ix)
        self.assertEqual((result['status'], result['value'], result['original_system']),
                         ('NAME_MATCHED', '35#翡翠绿', '待定'))
        self.assertEqual(result['match_method'], 'NAME_TRAILING_SE')

    def test_single_trailing_se_optional_both_directions(self):
        for src, target in [('红', '红色'), ('红色', '红')]:
            with self.subTest(src=src, target=target):
                ix = index([source(product_name='普通面料-12#' + src)], values={'F1-R': '12#' + src})
                got = mod.resolve_name_reference(record(中文颜色名称=target), ix)
                self.assertEqual(got['status'], 'NAME_MATCHED')
                self.assertEqual(got['match_method'], 'NAME_TRAILING_SE')

    def test_only_one_trailing_se_is_removed(self):
        self.assertEqual(mod.normalize_color_name('红色色'), '红色')
        self.assertEqual(mod.resolve_name_reference(record(中文颜色名称='红色色'), index())['status'], 'NAME_NOT_FOUND')

    def test_internal_se_not_removed(self):
        ix = index([source(product_name='普通面料-12#色织蓝')], values={'F1-R': '12#色织蓝'})
        self.assertEqual(mod.resolve_name_reference(record(中文颜色名称='织蓝'), ix)['status'], 'NAME_NOT_FOUND')
        self.assertEqual(mod.resolve_name_reference(record(中文颜色名称='色织蓝色'), ix)['status'], 'NAME_MATCHED')

    def test_internal_spaces_not_removed_but_existing_output_preserved(self):
        ix = index([source(product_name='普通面料-12#深 蓝')], values={'F1-R': '12#深蓝'})
        self.assertEqual(mod.resolve_name_reference(record(中文颜色名称='深蓝'), ix)['status'], 'NAME_NOT_FOUND')
        result = mod.resolve_name_reference(record(中文颜色名称=' 深 蓝色 '), ix)
        self.assertEqual((result['status'], result['value']), ('NAME_MATCHED', '12#深蓝'))
        self.assertEqual(result['source_color_name'], '深 蓝')

    def test_main_name_not_replaced_by_available_other_candidate(self):
        row = record(中文颜色名称='蓝色', A2023中文候选='红色', B2024中文候选='红色')
        self.assertEqual(mod.resolve_name_reference(row, index())['status'], 'NAME_NOT_FOUND')

    def test_main_name_not_blocked_by_unrelated_ambiguous_candidates(self):
        row = record(A2023中文候选='蓝色', B2024中文候选='绿色')
        self.assertEqual(mod.resolve_name_reference(row, index())['status'], 'NAME_MATCHED')

    def test_ambiguous_candidates_not_selected_for_availability(self):
        row = record(中文颜色名称='', A2023中文候选='红色', B2024中文候选='蓝色')
        self.assertEqual(mod.resolve_name_reference(row, index())['status'], 'AMBIGUOUS_COLOR_CANDIDATES')

    def test_two_candidates_same_after_allowed_normalization(self):
        row = record(中文颜色名称='', A2023中文候选=' 红 ', B2024中文候选='红色')
        self.assertEqual(mod.resolve_name_reference(row, index())['status'], 'NAME_MATCHED')

    def test_all_names_missing(self):
        self.assertEqual(mod.resolve_name_reference(record(中文颜色名称=''), index())['status'], 'MISSING_COLOR_NAME')

    def test_primary_number_prefix_not_removed(self):
        self.assertEqual(mod.resolve_name_reference(record(中文颜色名称='12#红色'), index())['status'], 'NAME_NOT_FOUND')

    def test_multiple_skus_same_name_conflict(self):
        ix = index([source(), source('F1-R2', '普通面料-14#红')], values={'F1-R': '12#红色', 'F1-R2': '14#红'})
        self.assertEqual(mod.resolve_name_reference(record(), ix)['status'], 'NAME_CONFLICT')
        self.assertEqual(len(ix.blocked_keys), 1)

    def test_missing_second_sku_output_does_not_hide_conflict(self):
        ix = index([source(), source('F1-R2', '普通面料-14#红')])
        got = mod.resolve_name_reference(record(), ix)
        self.assertEqual((got['status'], got['value']), ('NAME_CONFLICT', ''))

    def test_same_sku_inconsistent_names_block_all_corresponding_names(self):
        ix = index([source(), source(product_name='普通面料-12#蓝色')])
        for name in ['红色', '蓝色']:
            self.assertEqual(mod.resolve_name_reference(record(中文颜色名称=name), ix)['status'], 'NAME_CONFLICT')
        self.assertTrue(all(reason == 'INCONSISTENT_SOURCE_SKU' for reason in ix.blocked_keys.values()))

    def test_same_sku_conflicting_unusable_row_still_blocks(self):
        for bad in [None, '', '完全不同的产品', 123]:
            with self.subTest(bad=bad):
                ix = index([source(), source(product_name=bad)])
                self.assertEqual(mod.resolve_name_reference(record(), ix)['status'], 'NAME_CONFLICT')

    def test_identical_duplicate_sku_does_not_create_conflict(self):
        ix = index([source(), source()])
        self.assertEqual(mod.resolve_name_reference(record(), ix)['status'], 'NAME_MATCHED')

    def test_missing_parsed_output_keeps_source_evidence_but_empty_value(self):
        for value in [None, '', '   ', 12, []]:
            with self.subTest(value=value):
                ix = index(values={'F1-R': value})
                got = mod.resolve_name_reference(record(), ix)
                self.assertEqual((got['status'], got['value'], got['source_sku']),
                                 ('SOURCE_COLOR_UNAVAILABLE', '', 'F1-R'))

    def test_cross_fabric_id_and_name_both_required(self):
        for row in [source('F2-R'), source('F10-R'), source(product_name='其他面料-12#红色')]:
            with self.subTest(row=row):
                self.assertEqual(mod.resolve_name_reference(record(), index([row]))['status'], 'NAME_NOT_FOUND')

    def test_optimized_variant_cannot_mix_with_original(self):
        rows = [source(), source('F2-R', '普通面料优化版-15#红色')]
        ix = index(rows, [('F1', '普通面料'), ('F2', '普通面料优化版')], {'F1-R': '12#红色', 'F2-R': '15#红色'})
        self.assertEqual(mod.resolve_name_reference(record(), ix)['value'], '12#红色')
        optimized = record(面料编号='F2', 面料='普通面料优化版')
        self.assertEqual(mod.resolve_name_reference(optimized, ix)['value'], '15#红色')
        self.assertEqual(mod.resolve_name_reference(record(面料='普通面料优化版'), ix)['status'], 'UNKNOWN_FABRIC')

    def test_same_code_original_and_optimized_still_names_isolated(self):
        ix = index([source('F1-O', '普通面料优化版-15#红色')],
                   [('F1', '普通面料'), ('F1', '普通面料优化版')], {'F1-O': '15#红色'})
        self.assertEqual(mod.resolve_name_reference(record(), ix)['status'], 'NAME_NOT_FOUND')
        self.assertEqual(mod.resolve_name_reference(record(面料='普通面料优化版'), ix)['value'], '15#红色')

    def test_complete_fabric_prefix_with_internal_hyphen(self):
        ix = index([source('F-10-R', '特殊-面料-15#红色')], [('F-10', '特殊-面料')], {'F-10-R': '15#红色'})
        got = mod.resolve_name_reference(record(面料编号='F-10', 面料='特殊-面料'), ix)
        self.assertEqual(got['status'], 'NAME_MATCHED')

    def test_reject_non_single_numbered_source_color(self):
        bad = ['红色', '12#红色/13#蓝色', '12#红色，蓝色', '12#红色、蓝色',
               '12#红色;蓝色', '12#红色|蓝色', '12#', '12#色', 'x12#红色', '12 #红色']
        for tail in bad:
            with self.subTest(tail=tail):
                self.assertEqual(mod.resolve_name_reference(record(), index([source(product_name='普通面料-' + tail)]))['status'], 'NAME_NOT_FOUND')

    def test_totals_and_unknown_stat_remain_blank(self):
        for stat, expected in [('总量', 'TOTAL_ROW'), ('', 'UNKNOWN_STAT_TYPE'), ('其他', 'UNKNOWN_STAT_TYPE')]:
            got = mod.resolve_name_reference(record(统计类型=stat), index())
            self.assertEqual((got['status'], got['value']), (expected, ''))

    def test_unknown_or_missing_fabric_never_matches(self):
        self.assertEqual(mod.resolve_name_reference(record(面料编号='F9'), index())['status'], 'UNKNOWN_FABRIC')
        self.assertEqual(mod.resolve_name_reference(record(面料=''), index())['status'], 'MISSING_OR_INVALID_FABRIC')

    def test_invalid_primary_does_not_fall_back(self):
        for primary in [123, ['红色'], '色']:
            got = mod.resolve_name_reference(record(中文颜色名称=primary, B2024中文候选='红色'), index())
            self.assertEqual(got['status'], 'INVALID_COLOR_NAME')

    def test_invalid_secondary_candidate_does_not_pick_good_other(self):
        got = mod.resolve_name_reference(record(中文颜色名称='', A2023中文候选=['红色'], B2024中文候选='红色'), index())
        self.assertEqual(got['status'], 'INVALID_COLOR_CANDIDATE')

    def test_richtext_record_supported_without_link_coercion(self):
        row = record(中文颜色名称=[{'text': '红色', 'type': 'text'}])
        self.assertEqual(mod.resolve_name_reference({'record_id': 'rec_synthetic', 'fields': row}, index())['status'], 'NAME_MATCHED')
        row['中文颜色名称'] = [{'text': '红色', 'type': 'url', 'link': 'not_used'}]
        self.assertEqual(mod.resolve_name_reference(row, index())['status'], 'INVALID_COLOR_NAME')

    def test_no_mutation_inventory_system_or_inputs(self):
        rows = [source()]
        values = {'F1-R': '12#红色'}
        pairs = [('F1', '普通面料')]
        row = record(库存=0, 待到货=22, 颜色体系='待定')
        before = copy.deepcopy((rows, values, pairs, row))
        ix = mod.build_name_index(rows, pairs, values)
        mod.resolve_name_reference(row, ix)
        self.assertEqual((rows, values, pairs, row), before)

    def test_whitespace_outer_only_and_no_fuzzy_or_synonym(self):
        self.assertEqual(mod.resolve_name_reference(record(中文颜色名称='  红色  '), index())['status'], 'NAME_MATCHED')
        for name in ['红', '红色']:
            self.assertEqual(mod.resolve_name_reference(record(中文颜色名称=name), index())['status'], 'NAME_MATCHED')
        for name in ['酒红色', '大红色', '红 色', 'red']:
            self.assertEqual(mod.resolve_name_reference(record(中文颜色名称=name), index())['status'], 'NAME_NOT_FOUND')

    def test_pure_module_has_only_allowlisted_imports(self):
        tree = ast.parse(MODULE_PATH.read_text(encoding='utf-8-sig'))
        imports = [n.module if isinstance(n, ast.ImportFrom) else a.name
                   for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))
                   for a in (n.names if isinstance(n, ast.Import) else [None])]
        self.assertEqual(set(imports), {'__future__', 're', 'collections', 'typing'})
        self.assertFalse(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and
                             n.func.id in {'open', 'exec', 'eval', '__import__'} for n in ast.walk(tree)))


if __name__ == '__main__':
    unittest.main()
