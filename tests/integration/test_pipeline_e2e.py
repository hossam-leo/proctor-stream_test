import os, pytest

@pytest.mark.skipif(not (os.getenv('PROCTORSTREAM_TEST_REDIS_URL') and os.getenv('PROCTORSTREAM_TEST_POSTGRES_URL')), reason='live Redis and PostgreSQL URLs not configured')
def test_candidate_event_to_worker_to_risk_pipeline(tmp_path, monkeypatch):
    from services.api.storage import PostgresStore
    from services.realtime.stream import RedisStreamBus
    from services.workers.realtime import handle_once
    from services.api.risk import RiskEngine
    sid='e2e-pipeline'; store=PostgresStore(db_url=os.environ['PROCTORSTREAM_TEST_POSTGRES_URL'],report_dir=tmp_path); store.create_session(sid,'candidate-e2e',True)
    bus=RedisStreamBus(url=os.environ['PROCTORSTREAM_TEST_REDIS_URL'],stream='e2e.events',group='e2e-group'); bus.ensure_group(); event={'event_id':'e2e-event-1','session_id':sid,'ts_ms':1,'channel':'video','detector':'test','model_version':'test-v1','event_type':'MULTI_FACE','payload':{'face_count':2},'quality':{'usable':True}}
    bus.publish(event); assert handle_once(bus,store)==1; assert handle_once(bus,store)==0; events=store.events(sid); result=RiskEngine().score(events); assert result['risk_level']=='ELEVATED'; assert len(result['flags'])==1
