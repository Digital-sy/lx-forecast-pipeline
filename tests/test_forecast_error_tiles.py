"""Synthetic tests: no business fixture, model execution, or database access."""
import math
import unittest
from scripts.build_forecast_error_tiles import visible, pack, make_layout, W, H, stats


class TileTests(unittest.TestCase):
    def test_thresholds(self):
        for w in ['d01_30', 'd31_60']:
            self.assertFalse(visible(9, w)); self.assertTrue(visible(10, w))
        self.assertFalse(visible(19, 'd01_60')); self.assertTrue(visible(20, 'd01_60'))
        self.assertFalse(visible(None, 'd01_30')); self.assertFalse(visible(math.nan, 'd01_30'))

    def test_local_month_filter(self):
        rows=[dict(actual=9, window='d01_30'), dict(actual=10, window='d01_30')]
        self.assertEqual([visible(**r) for r in rows], [False, True])

    def test_geometry(self):
        rows=[dict(id=i, actual=a, window='d01_30', truth='OBSERVED', shop='S',spu=str(i),issue='2025-01-02',month='2025-01') for i,a in enumerate([10,100,1000,10000])]
        layout=make_layout(rows)
        for r in rows:
            x,y,s=layout['coordinates'][r['id']]
            self.assertAlmostEqual(s*s/layout['k'],r['actual'])
            self.assertLessEqual(x+s,W);self.assertLessEqual(y+s,H)
        self.assertEqual(pack(rows,layout['k']),pack(list(reversed(rows)),layout['k']))

    def test_missing_not_zero(self):
        r=dict(actual=100,truth='OBSERVED',shop='S',spu='A',values={'m':dict(p=None,state='NO_PREDICTION')})
        a=stats([r],'m');self.assertEqual(a['n'],1);self.assertEqual(a['missing'],1);self.assertEqual(a['hit'],0);self.assertIsNone(a['wape'])
        r['values']['m']=dict(p=0,state='OK');a=stats([r],'m');self.assertEqual(a['wape'],1);self.assertEqual(a['black'],0)

    def test_unrounded_hit(self):
        r=dict(actual=100,truth='OBSERVED',shop='S',spu='A',values={'m':dict(p=120.,state='OK')})
        self.assertEqual(stats([r],'m')['hit'],1)
        r['values']['m']['p']=math.nextafter(120.,math.inf)
        self.assertEqual(stats([r],'m')['hit'],0)


if __name__=='__main__':unittest.main()
