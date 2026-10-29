from contextlib import closing
from importlib.util import module_from_spec, spec_from_file_location
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from mra.config import VERSION_ROOT
from mra.rate_limits import AccountLimiter, RateLimitWait, retry_seconds, Policy, policy
from mra.runtime import read_runtime
from mra.store import open_store


class SharedLimits(unittest.TestCase):
    def test_shared_lease_and_cooldown_survive_second_process_and_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/"state.sqlite3"
            clock=[1000.0]
            account={"marketplace":"ozon","account_id":"A"}
            with closing(open_store(path)) as first, closing(open_store(path)) as second:
                a,b=AccountLimiter(first,account,clock=lambda:clock[0]),AccountLimiter(second,account,clock=lambda:clock[0])
                token,wait=a.acquire("POST", "/v1/question/list")
                self.assertEqual(wait,0)
                with self.assertRaises(RateLimitWait):b.acquire("POST", "/v1/question/list")
                a.release(token,cooldown=120)
                with self.assertRaises(RateLimitWait):b.acquire("POST", "/v1/question/list")
            clock[0]=1121
            with closing(open_store(path)) as reopened:
                token,wait=AccountLimiter(reopened,account,clock=lambda:clock[0]).acquire("POST", "/v1/question/list")
                self.assertEqual(wait,0)

    def test_yandex_and_unknown_wb_use_conservative_limits(self):
        with tempfile.TemporaryDirectory() as temporary,closing(open_store(Path(temporary)/"db.sqlite3")) as store:
            clock=[1000.0]
            ym=AccountLimiter(store,{"marketplace":"yandex_market","account_id":"Y"},clock=lambda:clock[0])
            token,_=ym.acquire("POST", "/v1/businesses/7/goods-questions/update");ym.release(token)
            _,wait=ym.acquire("POST", "/v1/businesses/7/goods-questions/update")
            self.assertAlmostEqual(wait,7.3)
            wb=AccountLimiter(store,{"marketplace":"wildberries","account_id":"W"},clock=lambda:clock[0])
            token,_=wb.acquire("GET", "/api/v1/questions");wb.release(token)
            with self.assertRaises(RateLimitWait):wb.acquire("GET", "/api/v1/questions")
            self.assertEqual(retry_seconds("120"),120)
            self.assertEqual(retry_seconds("not-a-date"),60)
            self.assertEqual(retry_seconds("inf"),60)


    def test_method_budget_denial_does_not_consume_global_quota(self):
        with tempfile.TemporaryDirectory() as temporary, closing(open_store(Path(temporary)/"db")) as store:
            clock=[1000.0]
            limiter=AccountLimiter(store,{"marketplace":"ozon","account_id":"A"},clock=lambda:clock[0])
            with patch("mra.rate_limits.policy",return_value=Policy("limited_method",0,2,3600)):
                for moment in (1000,1002):
                    clock[0]=moment
                    token,_=limiter.acquire("POST","/v1/question/list");limiter.release(token,status=200)
                count=store._db.execute("SELECT count(*) FROM api_limit_events").fetchone()[0]
                with self.assertRaises(RateLimitWait) as error:limiter.acquire("POST","/v1/question/list")
                self.assertIn("скользящее окно",error.exception.reason)
                self.assertEqual(store._db.execute("SELECT count(*) FROM api_limit_events").fetchone()[0],count)
            clock[0]=1004
            token,wait=limiter.acquire("POST","/v2/review/list")
            self.assertEqual(wait,0)

    def test_429_exponential_backoff_and_successful_response_exhaustion(self):
        with tempfile.TemporaryDirectory() as temporary, closing(open_store(Path(temporary)/"db")) as store:
            clock=[1000.0]
            limiter=AccountLimiter(store,{"marketplace":"yandex_market","account_id":"A","business_id":7},clock=lambda:clock[0])
            token,_=limiter.acquire("POST","/v1/businesses/7/goods-questions")
            limiter.release(token,status=429,headers={"Retry-After":"2000"})
            self.assertGreaterEqual(limiter.snapshot()[0]["blocked_until"],3000)
            clock[0]=3001
            token,_=limiter.acquire("POST","/v1/businesses/7/goods-questions")
            limiter.release(token,status=420)
            self.assertGreaterEqual(limiter.snapshot()[0]["blocked_until"],4801)
            clock[0]=4802
            token,_=limiter.acquire("POST","/v1/businesses/7/goods-questions")
            limiter.release(token,status=200,headers={"X-RateLimit-Resource-Remaining":"0","X-RateLimit-Resource-Until":"Thu, 01 Jan 1970 01:40:00 GMT"})
            self.assertEqual(limiter.snapshot()[0]["consecutive_limits"],0)
            self.assertGreaterEqual(limiter.snapshot()[0]["blocked_until"],6000)
            with self.assertRaises(RateLimitWait):limiter.acquire("POST","/v2/businesses/7/goods-feedback")

    def test_same_seller_aliases_and_catalog_share_gate_other_accounts_do_not(self):
        with tempfile.TemporaryDirectory() as temporary, closing(open_store(Path(temporary)/"db")) as store:
            clock=lambda:1000.0
            a=AccountLimiter(store,{"marketplace":"ozon","account_id":"A","client_id":"7"},clock=clock)
            b=AccountLimiter(store,{"marketplace":"ozon","account_id":"B","client_id":"7"},clock=clock)
            c=AccountLimiter(store,{"marketplace":"ozon","account_id":"C","client_id":"8"},clock=clock)
            token,_=a.acquire("POST","/v3/product/list")
            with self.assertRaises(RateLimitWait):b.acquire("POST","/v1/question/list")
            self.assertEqual(c.acquire("POST","/v1/question/list")[1],0)
            a.release("wrong-owner",status=200)
            with self.assertRaises(RateLimitWait):b.acquire("POST","/v1/question/list")
            a.release(token,status=200)
            self.assertGreater(b.acquire("POST","/v1/question/list")[1],0)

    def test_wb_category_shared_across_questions_reviews_and_unknown_route_refused(self):
        account={"marketplace":"wildberries","account_id":"A","token_type":"personal"}
        self.assertEqual(policy(account,"GET","/api/v1/questions").family,policy(account,"POST","/api/v1/feedbacks/answer").family)
        with self.assertRaises(ValueError):policy(account,"GET","/unknown")
        self.assertEqual(policy({**account,"token_type":"base"},"GET","/api/v1/questions").requests,5)
        self.assertEqual(retry_seconds("Thu, 01 Jan 1970 01:00:00 GMT",clock=lambda:3500),100)

    def test_crashed_owner_expires_and_next_process_keeps_budget(self):
        with tempfile.TemporaryDirectory() as temporary, closing(open_store(Path(temporary)/"db")) as store:
            clock=[1000.0]
            limiter=AccountLimiter(store,{"marketplace":"ozon","account_id":"A"},clock=lambda:clock[0])
            limiter.acquire("POST","/v1/question/list")
            clock[0]=1121
            self.assertEqual(limiter.acquire("POST","/v1/question/list")[1],0)
            self.assertTrue(store.verify_audit()["ok"])


class InstallationSetup(unittest.TestCase):
    def module(self):
        spec=spec_from_file_location("mra_setup_tool",VERSION_ROOT/"tools/configure.py")
        module=module_from_spec(spec);spec.loader.exec_module(module)
        return module

    def test_new_installation_configures_paths_and_preserves_settings_on_repeat(self):
        with tempfile.TemporaryDirectory() as temporary:
            base=Path(temporary);root=base/"installation";root.mkdir();(root/"examples").mkdir()
            shutil.copy2(VERSION_ROOT/"examples/accounts.example.json",root/"examples/accounts.example.json")
            key=base/"synthetic-key.txt";key.write_text("synthetic credential used only by the test")
            tool=self.module()
            answers=iter(["3",str(key),"123456","1",str(key),"Colleague"])
            with patch.object(tool,"ROOT",root):
                self.assertEqual(tool.configure(read=lambda _:next(answers),write=lambda _:None),0)
                self.assertRaises(ValueError,tool.credential_path,str(root/"examples/accounts.example.json"))
                repeat=iter(["3","","",""])
                self.assertEqual(tool.configure(read=lambda _:next(repeat),write=lambda _:None),0)
            accounts=json.loads((root/"data/accounts.json").read_text())
            ozon=accounts["accounts"][2]
            self.assertEqual(ozon["client_id"],"123456")
            self.assertEqual(ozon["api_key_file"],str(key.resolve()))
            self.assertNotIn("api_key_env",ozon)
            runtime,error=read_runtime(root/"data/runtime.json")
            self.assertFalse(error)
            self.assertEqual(runtime["model"],"openai/gpt-6-luna")
            self.assertEqual(len(list((root/"data").glob("accounts.backup-*"))),1)
            self.assertNotIn(key.read_text(),(root/"data/runtime.json").read_text())
            env=dict(os.environ,MRA_DATA_DIR=str(root/"data"))
            for name in ["MRA_IGNORE_RUNTIME","MRA_PROVIDER","MRA_MODEL"]:env.pop(name,None)
            result=subprocess.run([sys.executable,"-B","-c","import json; from mra.config import PROVIDER,MODEL,RUNTIME_OPERATOR; print(json.dumps([PROVIDER,MODEL,RUNTIME_OPERATOR]))"],cwd=VERSION_ROOT,env=env,capture_output=True,text=True,check=True)
            self.assertEqual(json.loads(result.stdout),["openrouter","openai/gpt-6-luna","Colleague"])

    def test_invalid_runtime_never_silently_selects_another_provider(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/"runtime.json";path.write_text('{"api_key":"not allowed"}')
            runtime,error=read_runtime(path)
            self.assertEqual(runtime["provider"],"invalid")
            self.assertTrue(error)


if __name__=="__main__":unittest.main()
