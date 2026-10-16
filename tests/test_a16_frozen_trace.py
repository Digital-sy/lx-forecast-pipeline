"""Synthetic-only checks. No customer observations or credentials."""
import ast
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from scripts.diagnose_a16_frozen_trace import Candidate,CutoffHistory,fixed_stages,independent_a7,state,shift,diagnose,A3,A7,A16


class FrozenTraceTests(unittest.TestCase):
    def test_missing_is_not_zero(self):
        m=shift('2026-01',0)
        self.assertEqual(state({},'X',m)['state'],'MISSING_UNKNOWN')
        self.assertEqual(state({('X',m):0},'X',m)['state'],'OBSERVED_ZERO')

    def test_cross_year_cutoff(self):
        self.assertEqual(str(shift('2027-01',-3)),'2026-10-01')

    def test_future_rejected(self):
        cutoff=shift('2026-10',0)
        h=CutoffHistory({('X',cutoff):1},cutoff)
        with self.assertRaises(ValueError):h.get(('X',cutoff))

    def test_branches(self):
        cases=[(0,50,'RISING',0,'A3_ONLY'),(100,None,'UNKNOWN',100,'A3_ONLY'),
               (100,50,'RISING',50,'DOWN'),(100,95,'FALLING',100,'NEUTRAL'),
               (100,200,'RISING',100,'UP_RISING'),(100,200,'FALLING',100,'UP_BLOCKED')]
        for a,c,p,want,rule in cases:
            self.assertEqual(independent_a7(a,c,p,1,0),(want,rule))

    def test_fixed_full_group_without_retuning(self):
        obs={(f'JQ-US|S{i}',shift('2024-01',j)):100+j for i in range(6) for j in range(33)}
        c=Candidate(obs,{f'S{i}':'synthetic' for i in range(6)})
        with patch.object(c.r5,'choose_params',side_effect=AssertionError('retuning')),patch.object(c,'predict',side_effect=AssertionError('full shadow')):
            rows,features,_,_,_=fixed_stages(c,shift('2026-12',0),2,1,0)
        self.assertEqual(len(features),6)
        self.assertEqual(len(rows),6)
        self.assertTrue(all(r['类目_有效SPU数']==6 for r in rows))
        self.assertTrue(all((r[A7]-r[A3])+(r[A16]-r[A7])==r[A16]-r[A3] for r in rows))

    def test_down_not_vetoed_by_category_or_anomaly(self):
        obs={(f'JQ-US|S{i}',shift('2024-01',j)):100 for i in range(6) for j in range(33)}
        c=Candidate(obs,{f'S{i}':'synthetic' for i in range(6)})
        row={'SPU':'JQ-US|S0','目标月':'2026-12','Horizon':'H2','品类':'synthetic',A3:100,A7:20,'A7规则':'DOWN'}
        x=c.r12.apply_trust_gates(obs,c.r11.first_sale_map(obs),[row],{})[0]
        self.assertEqual(x[A16],20)
        self.assertEqual(x['A16规则'],'KEEP_A7')

    def test_existing_directory_refused_before_inputs_read(self):
        with tempfile.TemporaryDirectory() as p:
            with self.assertRaises(ValueError):diagnose('absent',p,['S'],p)

    def test_no_database_dependency_or_shadow_entry_call(self):
        p=Path(__file__).parents[1]/'scripts/diagnose_a16_frozen_trace.py'
        tree=ast.parse(p.read_text(encoding='utf-8'))
        calls=[n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)]
        self.assertNotIn('predict',calls)
        self.assertNotIn('choose_params',calls)
        self.assertNotIn('execute',calls)


if __name__=='__main__':unittest.main()
