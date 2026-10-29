from contextlib import closing, redirect_stdout
import io
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tools import start
from mra.config import VERSION_ROOT
from mra.store import open_store


class GuidedStartup(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)/'installation';self.root.mkdir();(self.root/'examples').mkdir()
        shutil.copy2(VERSION_ROOT/'examples/accounts.example.json',self.root/'examples/accounts.example.json')
        self.folder=start.prepare_keys(self.root)

    def fill_selected(self):
        for name,value in [('ozon-deppa-msk-api-key.txt','synthetic-market-key'),('ozon-deppa-msk-client-id.txt','123456'),('openrouter-api-key.txt','synthetic-model-key')]:
            (self.folder/name).write_text(value)

    def test_creates_ten_empty_files_and_never_overwrites_filled_key(self):
        self.assertEqual(len(list(self.folder.glob('*.txt'))),10)
        self.assertTrue(all(p.stat().st_size==0 for p in self.folder.glob('*.txt')))
        key=self.folder/'openrouter-api-key.txt';key.write_text('fixture')
        start.prepare_keys(self.root)
        self.assertEqual(key.read_text(),'fixture')
        self.assertEqual(key.stat().st_mode & 0o777,0o600)

    def test_only_selected_account_and_provider_are_required(self):
        self.fill_selected()
        self.assertEqual(start.configure(self.root,'acct_deppa_ozon_msk','1'),[])
        runtime=json.loads((self.root/'data/runtime.json').read_text())
        accounts=json.loads((self.root/'data/accounts.json').read_text())['accounts']
        account=next(a for a in accounts if a['account_id']=='acct_deppa_ozon_msk')
        self.assertEqual(runtime['provider'],'openrouter')
        self.assertEqual(account['client_id_file'],str((self.folder/'ozon-deppa-msk-client-id.txt').resolve()))
        self.assertNotIn('synthetic-market-key',json.dumps(accounts))
        self.assertNotIn('synthetic-model-key',json.dumps(runtime))
        self.assertEqual((self.folder/'openai-api-key.txt').stat().st_size,0)

    def test_rejects_rtf_multiline_and_nonnumeric_client_id(self):
        p=self.folder/'openrouter-api-key.txt'
        for text in ('{\\rtf1 wrong}','key\nsecond','key with spaces',''):
            p.write_text(text);self.assertFalse(start.validate_file(p))
        p.write_text('not-a-number');self.assertFalse(start.validate_file(p,numeric=True))

    def test_prepare_only_is_offline_even_with_complete_keys(self):
        self.fill_selected()
        with patch.object(start,'ROOT',self.root),patch('tools.start.subprocess.run') as run,redirect_stdout(io.StringIO()):
            self.assertEqual(start.main(['--prepare-only','--account','acct_deppa_ozon_msk','--profile','1']),0)
        run.assert_not_called()

    def test_missing_keys_stops_before_network_and_model(self):
        with patch.object(start,'ROOT',self.root),patch('tools.start.subprocess.run') as run,redirect_stdout(io.StringIO()):
            self.assertEqual(start.main(['--prepare-only']),2)
        run.assert_not_called()
        self.assertFalse((self.root/'data/runtime.json').exists())

    def test_ready_setup_checks_access_but_waits_for_explicit_START(self):
        self.fill_selected()
        start.configure(self.root,'acct_deppa_ozon_msk','1')
        with closing(open_store(self.root/'data/mra.sqlite3')):pass
        health=json.dumps({k:True for k in ('database_ok','rules_ok','canon_ok','bundled_rules_connected')})
        result=subprocess.CompletedProcess([],0,stdout=health,stderr='')
        with patch.object(start,'ROOT',self.root),patch('tools.start.subprocess.run',return_value=result) as run,patch('builtins.input',return_value=''),patch('mra.market_api.CommunicationClient.probe',return_value={'read_access':True}) as probe,redirect_stdout(io.StringIO()):
            self.assertEqual(start.main([]),0)
        probe.assert_called_once_with('question')
        self.assertFalse(any('review-session' in call.args[0] for call in run.call_args_list))

    def test_START_launches_one_bounded_session_with_local_key_path(self):
        self.fill_selected()
        start.configure(self.root,'acct_deppa_ozon_msk','1')
        with closing(open_store(self.root/'data/mra.sqlite3')):pass
        health=json.dumps({k:True for k in ('database_ok','rules_ok','canon_ok','bundled_rules_connected')})
        result=subprocess.CompletedProcess([],0,stdout=health,stderr='')
        with patch.object(start,'ROOT',self.root),patch('tools.start.subprocess.run',return_value=result) as run,patch('builtins.input',return_value='СТАРТ'),patch('mra.market_api.CommunicationClient.probe',return_value={'read_access':True}),redirect_stdout(io.StringIO()):
            self.assertEqual(start.main([]),0)
        sessions=[call for call in run.call_args_list if 'review-session' in call.args[0]]
        self.assertEqual(len(sessions),1)
        self.assertIn('20',sessions[0].args[0])
        self.assertEqual(sessions[0].kwargs['env']['MRA_OPENROUTER_KEY_FILE'],str(self.folder/'openrouter-api-key.txt'))


if __name__=='__main__':unittest.main()
