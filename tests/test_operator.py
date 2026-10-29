from contextlib import closing
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mra.catalog import apply_snapshot, now
from mra.communications import sync_inbox
from mra.generate import GenerationResult
from mra.importing import import_snapshot
from mra.operator import terminal, review_session, next_message, postpone
from mra.store import open_store
from tests.test_communications import FixtureAPI


class OperatorMenu(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/"work.sqlite3"
        self.account={"marketplace":"ozon","account_id":"fixture","name":"Fixture account"}
        self.api=FixtureAPI("ozon","question")
        with closing(open_store(self.path)) as store:
            import_snapshot(store,{"schema_version":1,"products":[{"article":"44167","material":[["fact","Synthetic fact"]]}],
                "provenance":{"approved_by":"fixture","source":"synthetic"}})
            apply_snapshot(store,self.account,[{"listing_id":"11","article":"44167","aliases":[("sku","11")]}],observed_at=now(),complete=True)
            self.first=sync_inbox(store,self.account,transport=self.api,pause=lambda _:None)["messages"][0]
            self.api.raw.update(id="q2",text="Second question")
            self.second=sync_inbox(store,self.account,transport=self.api,pause=lambda _:None)["messages"][0]
        with closing(open_store(self.path)) as store:
            store._db.execute("UPDATE messages SET created_at=? WHERE message_id=?",("2026-10-28T00:00:00+00:00",self.first))
        self.addCleanup(patch.stopall)
        patch("mra.operator.load_accounts",return_value=[self.account]).start()
        patch("mra.catalog_api.load_accounts",return_value=[self.account]).start()

    def interact(self,answers):
        inputs=iter(answers);output=[]
        self.assertEqual(terminal(self.path,account_id="fixture",read=lambda _:next(inputs),write=output.append),0)
        return "\n".join(output)

    def test_automatic_session_prepares_before_first_prompt_and_only_asks_yes_no(self):
        answer=GenerationResult(text="Automatic fixture answer",model="fixture",source="fixture",decision="answer",basis=(("fact","Synthetic fact"),))
        output=[]
        def decide(prompt):
            self.assertIn("Да",prompt)
            self.assertTrue(any("Automatic fixture answer" in line for line in output))
            self.assertTrue(any("Question" in line for line in output))
            return "Нет"
        with patch("mra.pipeline.generate_reply",return_value=answer) as model:
            self.assertEqual(review_session(self.path,account_id="fixture",limit=1,read=decide,write=output.append),0)
        self.assertEqual(model.call_count,1)
        with closing(open_store(self.path)) as store:
            self.assertEqual(store.get_message(self.first)[11],"rejected")
            self.assertEqual(store.outbox_count(),0)

    def test_automatic_session_interrupt_never_prepares_ahead(self):
        answer=GenerationResult(text="Automatic fixture answer",model="fixture",source="fixture",decision="answer",basis=(("fact","Synthetic fact"),))
        with patch("mra.pipeline.generate_reply",return_value=answer) as model:
            self.assertEqual(review_session(self.path,account_id="fixture",limit=20,read=lambda _:(_ for _ in ()).throw(EOFError()),write=lambda _:None),0)
        self.assertEqual(model.call_count,1)
        with closing(open_store(self.path)) as store:
            self.assertEqual(store.get_message(self.second)[11],"new")
            self.assertEqual(store.outbox_count(),0)

    def test_category_and_canon_filter_exclude_untrained_products(self):
        with closing(open_store(self.path)) as store:
            store._db.execute("UPDATE products SET category='Headphones' WHERE article='44167'")
            self.assertEqual(next_message(store,self.account,"question",category="Headphones",canon_only=True),self.first)
            self.assertIsNone(next_message(store,self.account,"question",category="Chargers",canon_only=True))
            store._db.execute("DELETE FROM materials WHERE article='44167'")
            self.assertIsNone(next_message(store,self.account,"question",category="Headphones",canon_only=True))

    def test_feedback_regenerates_current_answer_and_keeps_previous_version(self):
        from mra.pipeline import Pipeline
        from mra.review import binary_review
        old=GenerationResult(text="Original draft",model="fixture",source="fixture",decision="answer",basis=(("fact","Synthetic fact"),))
        new=GenerationResult(text="Short revised draft",model="fixture",source="fixture",decision="answer",basis=old.basis)
        with closing(open_store(self.path)) as store:
            initial=Pipeline(store,generator=lambda *a,**kw:old).process(self.first)
            inputs=iter(["3","Сделай короче, не добавляй обещаний","Нет"])
            with patch("mra.pipeline.generate_reply",return_value=new) as model:
                from mra.review import binary_review
                binary_review(store,self.first,[self.account],read=lambda _:next(inputs),write=lambda _:None)
            self.assertEqual(model.call_count,1)
            self.assertEqual(model.call_args.kwargs["previous_answer"],old.text)
            self.assertEqual(model.call_args.kwargs["operator_feedback"],"Сделай короче, не добавляй обещаний")
            self.assertEqual(store._db.execute("SELECT text FROM drafts WHERE message_id=? ORDER BY rowid",(self.first,)).fetchall(),[(old.text,),(new.text,)])
            self.assertEqual(store.get_message(self.first)[11],"rejected")
            self.assertEqual(store.outbox_count(),0)
            event=next(e for e in store.review_history(self.first) if e["action"]=="regenerated")
            self.assertEqual(event["draft_id"],initial.draft_id)
            self.assertIsNotNone(event["replacement_draft_id"])

    def test_regeneration_revokes_hold_and_stale_feedback_cannot_call_model(self):
        from mra.pipeline import Pipeline
        old=GenerationResult(text="Original draft",model="fixture",source="fixture",decision="answer",basis=(("fact","Synthetic fact"),))
        new=GenerationResult(text="New draft",model="fixture",source="fixture",decision="answer",basis=old.basis)
        with closing(open_store(self.path)) as store:
            initial=Pipeline(store,generator=lambda *a,**kw:old).process(self.first)
            Pipeline(store).approve(self.first,expected_draft_id=initial.draft_id)
            with patch("mra.pipeline.generate_reply",return_value=new) as model:
                revised=Pipeline(store).regenerate(self.first,"Сократи",expected_draft_id=initial.draft_id,actor="Human")
                with self.assertRaises(ValueError):Pipeline(store).regenerate(self.first,"Устаревшее замечание",expected_draft_id=initial.draft_id,actor="Human")
            self.assertEqual(model.call_count,1)
            self.assertNotEqual(revised.draft_id,initial.draft_id)
            self.assertEqual(store._db.execute("SELECT state FROM outbox WHERE draft_id=?",(initial.draft_id,)).fetchone()[0],"superseded")
            self.assertIsNone(store._db.execute("SELECT 1 FROM approvals WHERE draft_id=?",(revised.draft_id,)).fetchone())

    def test_no_advances_to_next_ready_pair_without_ending_session(self):
        answer=GenerationResult(text="Fixture answer",model="fixture",source="fixture",decision="answer",basis=(("fact","Synthetic fact"),))
        inputs=iter(["Нет","Нет"])
        with patch("mra.pipeline.generate_reply",return_value=answer) as model:
            self.assertEqual(review_session(self.path,account_id="fixture",limit=2,read=lambda _:next(inputs),write=lambda _:None),0)
        self.assertEqual(model.call_count,2)
        with closing(open_store(self.path)) as store:
            self.assertEqual([store.get_message(mid)[11] for mid in (self.first,self.second)],["rejected","rejected"])
            self.assertEqual(store.outbox_count(),0)

    def test_startup_and_exit_do_not_call_marketplace_or_model(self):
        with patch("mra.operator.sync_inbox") as inbox,patch("mra.pipeline.generate_reply") as model:
            text=self.interact(["0"])
        inbox.assert_not_called();model.assert_not_called()
        self.assertIn("Fixture account",text)

    def test_skip_then_next_question_generates_once_and_can_be_rejected(self):
        answer=GenerationResult(text="Fixture answer",model="fixture",source="fixture",decision="answer",basis=(("fact","Synthetic fact"),))
        with patch("mra.pipeline.generate_reply",return_value=answer) as model:
            text=self.interact(["1","2","1","1","3","Не подходит","0"])
        self.assertEqual(model.call_count,1)
        self.assertEqual(model.call_args.args[0],"Second question")
        self.assertIn("Fixture answer",text)
        with closing(open_store(self.path)) as store:
            self.assertEqual(store.get_message(self.first)[11],"new")
            self.assertEqual(store.get_message(self.second)[11],"rejected")
            self.assertIsNone(next_message(store,self.account,"question"))
            self.assertEqual(store.outbox_count(),0)

    def test_completed_and_attempted_delivery_are_excluded_from_next(self):
        with closing(open_store(self.path)) as store:
            store.set_message_state(self.first,"done")
            self.assertEqual(next_message(store,self.account,"question"),self.second)
            postpone(store,self.second,"Human")
        with closing(open_store(self.path)) as store:
            self.assertIsNone(next_message(store,self.account,"question"))
            other={**self.account,"account_id":"other"}
            self.assertIsNone(next_message(store,other,"question"))

    def test_missing_article_never_spends_model_call(self):
        with closing(open_store(self.path)) as store:
            store._db.execute("UPDATE messages SET article='' WHERE message_id=?",(self.first,))
        with patch("mra.operator.refresh_for_message"),patch("mra.pipeline.generate_reply") as model:
            self.interact(["1","1","0"])
        model.assert_not_called()


if __name__=="__main__":unittest.main()
