"""Synthetic safety checks for the offline reference audit; no business files."""
import importlib.util, pathlib, unittest
MODULE=pathlib.Path(__file__).with_name("reference_followup.py")
spec=importlib.util.spec_from_file_location("reference_followup",MODULE)
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)

class ReferenceSafety(unittest.TestCase):
    def setUp(self):
        self.docs={"TOKEN_A":{"document_id":"DOC_A","spu":"SAMPLE_STYLE"}}
        self.versions={("DOC_A","HASH_A"):[{"version_id":"VERSION_A"}]}
    def test_exact_token_and_hash_bind(self):
        r,status=audit.version_binding({"token":"TOKEN_A","source_sha256":"HASH_A"},self.docs,self.versions)
        self.assertEqual(r,{"document_id":"DOC_A","version_id":"VERSION_A"})
        self.assertEqual(status,"EXACT_DOCUMENT_AND_CONTENT_SHA256")
    def test_same_spu_never_binds_other_document(self):
        r,status=audit.version_binding({"token":"TOKEN_B","source_sha256":"HASH_A","spu":"SAMPLE_STYLE"},self.docs,self.versions)
        self.assertIsNone(r);self.assertEqual(status,"SOURCE_DOCUMENT_NOT_FOUND")
    def test_unknown_hash_cannot_bind(self):
        r,_=audit.version_binding({"token":"TOKEN_A"},self.docs,self.versions);self.assertIsNone(r)
    def test_changed_hash_cannot_bind(self):
        r,_=audit.version_binding({"token":"TOKEN_A","source_sha256":"HASH_B"},self.docs,self.versions);self.assertIsNone(r)
    def test_duplicate_version_never_first_wins(self):
        self.versions[("DOC_A","HASH_A")].append({"version_id":"VERSION_B"})
        r,_=audit.version_binding({"token":"TOKEN_A","source_sha256":"HASH_A"},self.docs,self.versions)
        self.assertIsNone(r)
    def test_binding_cannot_fabricate_order_evidence(self):
        r,_=audit.version_binding({"token":"TOKEN_A","source_sha256":"HASH_A"},self.docs,self.versions)
        self.assertEqual(set(r),{"document_id","version_id"})
        self.assertNotIn("order_evidence_id",r)
    def test_empty_reference_stays_unknown(self):
        self.assertEqual(audit.classify_case([]),"NO_ARCHIVED_COLOR_LOCATOR")
    def test_mixed_reference_stays_partial(self):
        refs=[{"lineage_status":"EXACT_REFERENCE_PAGE_LINEAGE_VERIFIED"},{"lineage_status":"REFERENCE_PAGE_LINEAGE_NOT_COMPLETE"}]
        self.assertEqual(audit.classify_case(refs),"REFERENCE_PAGE_LINEAGE_PARTIAL")
    def test_complete_reference_is_only_recoverable_candidate(self):
        self.assertEqual(audit.classify_case([{"lineage_status":"EXACT_REFERENCE_PAGE_LINEAGE_VERIFIED"}]),"REFERENCE_PAGE_LINEAGE_RECOVERABLE")
    def test_same_source_different_cell_changes_candidate_identity(self):
        self.assertNotEqual(audit.stable_id("CONF","TOKEN","SHA","Sheet",3,[{"cell":"B3","value":"8#"}]),
                            audit.stable_id("CONF","TOKEN","SHA","Sheet",4,[{"cell":"B4","value":"8#"}]))
    def test_unknown_number_is_not_inferred_from_color(self):
        ref={"sheet":"Sheet","row":3,"identity_cells":["A3"],"cells":[{"cell":"A3","value":"SAMPLE-BLUE"},{"cell":"B3","value":"蓝色"}]}
        identity,number=audit.locator_quality(ref);self.assertTrue(identity);self.assertFalse(number)
    def test_number_must_equal_archived_cell(self):
        ref={"sheet":"Sheet","row":3,"identity_cells":["A3"],"cells":[{"cell":"A3","value":"SAMPLE-BLUE"},{"cell":"B3","value":"8#"}],
             "explicit_number_evidence":[{"cell":"B3","raw":"9#","number":"9"}]}
        identity,number=audit.locator_quality(ref);self.assertTrue(identity);self.assertFalse(number)
if __name__=="__main__": unittest.main()
