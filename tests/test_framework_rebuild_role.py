import ast,types,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
tree=ast.parse((ROOT/'shared/brain_framework.py').read_text())
fn=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='_rebuild_supported')
class RebuildRoleTest(unittest.TestCase):
 def supported(self,role,**kwargs):
  s=types.SimpleNamespace(compute_worker=role,index_builder_fn=None,weight_builder_fn=None,enrich_fn=None,ENRICHED_TABLE='',**{})
  for k,v in kwargs.items():setattr(s,k,v)
  ns={'s':s};exec(compile(ast.Module(body=[fn],type_ignores=[]),'<actual-rebuild-contract>','exec'),ns)
  return ns['_rebuild_supported']()
 def test_proxy_has_no_local_rebuild_even_when_model_exposes_builders(self):
  for attr in ['index_builder_fn','weight_builder_fn','enrich_fn','ENRICHED_TABLE']:
   with self.subTest(attr=attr):self.assertFalse(self.supported(False,**{attr:object()}))
 def test_local_worker_retains_existing_capabilities(self):
  for attr in ['index_builder_fn','weight_builder_fn','enrich_fn','ENRICHED_TABLE']:
   with self.subTest(attr=attr):self.assertTrue(self.supported(True,**{attr:object()}))
 def test_empty_worker_still_unsupported(self):self.assertFalse(self.supported(True))
 def test_legacy_uninitialized_role_is_compatible(self):self.assertTrue(self.supported(None,enrich_fn=object()))
if __name__=='__main__':unittest.main()
