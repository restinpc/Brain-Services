import ast
import contextlib
from datetime import datetime,timezone
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

class QuoteCountTest(unittest.TestCase):
    def test_updates_are_not_new_keys(self):
        source=ast.parse((Path(__file__).resolve().parents[1]/'parser/WorldMonitor_market_quotes.py').read_text())
        cls=next(x for x in source.body if isinstance(x,ast.ClassDef) and x.name=='QuotesCollector')
        method=next(x for x in cls.body if isinstance(x,ast.FunctionDef) and x.name=='process')
        helpers=[x for x in source.body if isinstance(x,ast.FunctionDef) and x.name in ('_sf','_si')]
        tree=ast.Module(body=[method,*helpers],type_ignores=[])
        namespace=dict(datetime=datetime,timezone=timezone,json=json,SYMBOLS=['A','B','C'],
                       FINNHUB_REQUEST_DELAY=0,time=SimpleNamespace(sleep=lambda _:None),
                       finnhub_get_quote=lambda session,symbol:dict(symbol=symbol,c=1,t=1))
        exec(compile(tree,'quote-process-fixture','exec'),namespace)
        class Cursor:
            rowcount=6
            def execute(self,sql,params):
                self.sql,self.params=sql,params
            def fetchall(self):return [('A',)]
            def executemany(self,sql,rows):self.rows=rows
            def close(self):pass
        cursor=Cursor()
        connection=SimpleNamespace(cursor=lambda:cursor,commit=lambda:None,close=lambda:None)
        owner=SimpleNamespace(table_name='vlad_wm_market_quotes',session=None,
                              ensure_table=lambda:None,get_db_connection=lambda:connection)
        out=io.StringIO()
        with contextlib.redirect_stdout(out):namespace['process'](owner)
        self.assertIn('Новых ключей котировок: 2',out.getvalue())
        self.assertIn('Обновлено/записано строк: 6',out.getvalue())
        self.assertEqual(cursor.params[1:],('A','B','C'))
        self.assertIn('snapshot_hour=%s',cursor.sql)
        self.assertEqual(len(cursor.rows),3)

if __name__=='__main__':unittest.main()
