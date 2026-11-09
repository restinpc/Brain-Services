import copy
import json
import unittest
from shared.model100_h1_features import FeatureBuilder,Config
from shared.model100_feature_store import encode_row,decode_row,closed_hours,insert_rows,source_changes,feature_vector,pending_market_hours


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.builder=FeatureBuilder({'prices':[]},Config(groups=('price',)))
        self.row=self.builder.build_at('2026-11-09T10:00:00Z')

    def test_roundtrip_preserves_nulls_hash_order_and_mode(self):
        packed=encode_row(self.row,self.builder.schema,'v1')
        self.assertIn(None,json.loads(packed['feature_values']))
        self.assertEqual(decode_row(packed,self.builder.schema),self.row)

    def test_reader_vector_uses_existing_builder_contract(self):
        self.assertEqual(feature_vector(self.row,self.builder.schema),self.builder.vectorize(self.row))

    def test_child_node_cannot_start_writer(self):
        from unittest.mock import patch
        from types import SimpleNamespace
        from parser.model100_feature_store_parser import main
        with patch('parser.model100_feature_store_parser.os.uname',return_value=SimpleNamespace(nodename='brain2')):
            with self.assertRaisesRegex(RuntimeError,'B1_RUNTIME_ONLY'):main([])

    def test_gap_recovery_sorted_and_open_bar_excluded(self):
        class Cursor:
            def execute(self,sql,args):
                self.sql=sql
                if 'NOT EXISTS' not in sql:raise AssertionError('must check actual missing keys')
            def __iter__(self):return iter([('2026-11-09T10:00:00Z',),('2026-11-09T08:00:00Z',),('2026-11-09T11:00:00Z',)])
            def close(self):pass
        class DB:
            def cursor(self):return Cursor()
        result=pending_market_hours(DB(),'v1','STRICT_ASOF','2026-11-09T10:00:00Z','2026-11-09T11:30:00Z')
        self.assertEqual(result,['2026-11-09T09:00:00+00:00','2026-11-09T11:00:00+00:00'])

    def test_tampered_values_or_masks_are_rejected(self):
        for key in ('feature_values','missing_masks','provenance'):
            p=encode_row(self.row,self.builder.schema,'v1')
            p[key]=p[key].replace('1','0',1) if key!='provenance' else '{"tampered":true}'
            with self.assertRaises(ValueError):decode_row(p,self.builder.schema)

    def test_schema_mode_and_future_hour_rejected(self):
        p=encode_row(self.row,self.builder.schema,'v1');p['mode']='RESEARCH'
        with self.assertRaises(ValueError):decode_row(p,self.builder.schema)
        self.assertEqual(closed_hours(['2026-11-09T10:00:00Z','2026-11-09T11:00:00Z'],'2026-11-09T10:35:00Z'),['2026-11-09T10:00:00Z'])

    def test_target_column_not_admitted(self):
        r=copy.deepcopy(self.row);r['features']['t1']=2
        with self.assertRaises(ValueError):encode_row(r,self.builder.schema,'v1')

    def test_late_source_correction_is_reported_without_missing_old_rows(self):
        old={'prices':[dict(date='2026-11-09',open=1.,close=1.,max=1.,min=1.,row_updated_at='old')]}
        new=copy.deepcopy(old);new['prices'][0]['row_updated_at']='new'
        self.assertEqual(source_changes(old,new)['changed_overlapping_identities'],0)
        new['prices'][0]['close']=1.01
        self.assertEqual(source_changes(old,new)['changed_overlapping_identities'],1)
        self.assertEqual(source_changes(old,{'prices':[]})['changed_overlapping_identities'],0)

    def test_mysql_numeric_roundtrip_regression_lossless_float_hex(self):
        b=FeatureBuilder({'prices':[dict(date='2026-11-09T09:00:00Z',row_updated_at='2026-11-09T10:00:00Z',
                         open=1.0351966619491577,close=1.0351966619491577,max=1.0351966619491577,min=1.0351966619491577)]},Config(groups=('price',)))
        row=b.build_at('2026-11-09T10:00:00Z');p=encode_row(row,b.schema,'v1')
        self.assertEqual(json.loads(p['feature_values'])[0],'0x1.0902a60000000p+0')
        self.assertEqual(decode_row(p,b.schema),row)

    def test_signed_zero_and_encoding_are_explicit(self):
        row=copy.deepcopy(self.row);key=self.builder.schema['feature_order'][0]
        row['features'][key]=-0.0;row['missing_masks'][key]=0
        from shared.model100_h1_features import digest
        row['row_hash']=digest({k:v for k,v in row.items() if k!='row_hash'})
        p=encode_row(row,self.builder.schema,'v1')
        self.assertEqual(decode_row(p,self.builder.schema)['features'][key].hex(),'-0x0.0p+0')
        p['value_encoding']='UNKNOWN'
        with self.assertRaisesRegex(ValueError,'UNSUPPORTED_VALUE_ENCODING'):decode_row(p,self.builder.schema)

    def test_duplicate_is_noop_and_changed_history_fails(self):
        class Cursor:
            def __init__(self,db):self.db=db;self.old=None
            def execute(self,sql,args):
                if sql.startswith('SELECT'):self.old=self.db.old
                else:
                    self.db.inserts+=1
                    p=encode_row(self.outer.row,self.outer.builder.schema,'v1')
                    self.db.old={k:p[k] for k in ('row_hash','payload_sha256')}
            def fetchone(self):return self.old
            def close(self):pass
        class DB:
            old=None;inserts=0;commits=0;rollbacks=0
            def cursor(self,**kw):
                c=Cursor(self);c.outer=outer;return c
            def commit(self):self.commits+=1
            def rollback(self):self.rollbacks+=1
        outer=self;db=DB()
        self.assertEqual(insert_rows(db,[self.row,self.row],self.builder.schema,'v1'),{'inserted':1,'unchanged':1})
        db.old['row_hash']='0'*64
        with self.assertRaisesRegex(ValueError,'IMMUTABLE_HISTORY_CONFLICT'):insert_rows(db,[self.row],self.builder.schema,'v1')
        self.assertEqual(db.inserts,1);self.assertEqual(db.rollbacks,1)


if __name__=='__main__':unittest.main()
