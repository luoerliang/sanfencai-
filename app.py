import os, re, csv, sqlite3, threading, math, time, hashlib, json, gzip
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from flask import Flask, jsonify, render_template_string, request, send_file
import requests

PERSIST_DIR = os.getenv("PERSIST_DIR", "").strip()
if not PERSIST_DIR:
    for _candidate in ("/var/data", "/data"):
        if os.path.isdir(_candidate) and os.access(_candidate, os.W_OK):
            PERSIST_DIR = _candidate
            break

DB = os.getenv("DB_PATH", "").strip()
if not DB:
    DB = os.path.join(PERSIST_DIR, "history.db") if PERSIST_DIR else "history.db"

CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "").strip()
if not CHECKPOINT_PATH:
    CHECKPOINT_PATH = os.path.join(PERSIST_DIR, "sanfen_ai_checkpoint.json.gz") if PERSIST_DIR else "sanfen_ai_checkpoint.json.gz"

PERSISTENT_MODE = bool(
    PERSIST_DIR and
    os.path.abspath(DB).startswith(os.path.abspath(PERSIST_DIR)) and
    os.path.abspath(CHECKPOINT_PATH).startswith(os.path.abspath(PERSIST_DIR))
)
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = "".join(os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").split())
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "sanfen-backup").strip() or "sanfen-backup"
SUPABASE_OBJECT = os.getenv("SUPABASE_OBJECT", "sanfen_ai_checkpoint.json.gz").strip() or "sanfen_ai_checkpoint.json.gz"
REMOTE_BACKUP_ENABLED = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY and SUPABASE_BUCKET and SUPABASE_OBJECT)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ALLOWED_CHAT_ID = os.getenv("ALLOWED_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "10000"))
RENDER_SERVICE_NAME = os.getenv("RENDER_SERVICE_NAME", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
if not WEBHOOK_BASE_URL:
    WEBHOOK_BASE_URL = RENDER_EXTERNAL_URL
if not WEBHOOK_BASE_URL and RENDER_SERVICE_NAME:
    WEBHOOK_BASE_URL = f"https://{RENDER_SERVICE_NAME}.onrender.com"
WEBHOOK_PATH = "/telegram/webhook"
WEBHOOK_SECRET = hashlib.sha256(BOT_TOKEN.encode("utf-8")).hexdigest()[:48] if BOT_TOKEN else ""
TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""


def _issue_add(issue, steps=1):
    """Add periods for YYYYMMDDNNN ids, 001..480 per day."""
    raw=str(issue or "").strip()
    try:
        if re.fullmatch(r"\d{11}",raw):
            day=raw[:8]
            seq=int(raw[8:])
            if 1 <= seq <= 480:
                dt=datetime.strptime(day,"%Y%m%d")
                zero=(seq-1)+int(steps)
                day_shift, idx=divmod(zero,480)
                dt += timedelta(days=day_shift)
                return dt.strftime("%Y%m%d")+f"{idx+1:03d}"
        return str(int(raw)+int(steps))
    except Exception:
        return raw

def _next_issue_id(issue):
    return _issue_add(issue,1)


# 你当前线上服务的历史接口。v13 首次部署到“新服务”时会自动同步进去。
DEFAULT_HISTORY_SOURCE = "https://sanfensidongyuce-2.onrender.com/api/history?limit=500"
HISTORY_SOURCE_URL = os.getenv("HISTORY_SOURCE_URL", DEFAULT_HISTORY_SOURCE).strip()

live_cache = {"issue": None, "data": None, "building": False}
model_state = {
    "profile": None,
    "profile_scores": {},
    "last_calibrated_issue": None,
    "recalc_started_at": "",
    "recalc_finished_at": "",
    "calibration_n": 0
}
background_state = {
    "profile_calibrating": False,
    "stats_building": False,
    "boot_ready": False
}

long_prior_lock = threading.RLock()
long_prior = {
    "ready": False,
    "building": False,
    "total": 0,
    "num_count": Counter(),
    "zodiac_count": Counter(),
    "trans_num_by_zodiac": defaultdict(Counter),
    "trans_zodiac_by_zodiac": defaultdict(Counter),
    "updated_at": ""
}


history_cache = {"total": 0, "items": [], "loaded_at": ""}
history_cache_lock = threading.RLock()

live_cache_lock = threading.Lock()
sync_state = {"source": HISTORY_SOURCE_URL, "imported": 0, "last_error": "", "last_sync": ""}

app = Flask(__name__)
db_lock = threading.RLock()
stats_cache = {"issue": None, "value": None}
learner_lock = threading.RLock()
learner_cache = {
    "ready": False,
    "best_profile": "平衡",
    "profiles": {},
    "settled": 0,
    "window": 60,
    "updated_at": ""
}

AI_FEATURES = [
    "bias","sp6","sp12","sp24","sp60","all12","all36","gap",
    "transition","tail","longprior","wave","size","parity","head",
    "cold","repeat","zodiac_pair",
    "wave_parity_8","wave_parity_16","wave_parity_36","wave_parity_80"
]
ai_lock = threading.RLock()
ai_state = {
    "weights": [0.0] * len(AI_FEATURES),
    "steps": 0,
    "trained": 0,
    "last_issue": "",
    "lr": 0.08,
    "ready": False,
    "bootstrapping": False,
    "historical_validation_n": 0,
    "historical_hit24": 0.0,
    "rolling_window": 100,
    "rolling_trained": 0,
    "refit_count": 0,
    "last_refit_seconds": 0.0,
    "updated_at": ""
}

correction_lock = threading.RLock()
correction_state = {
    "20": {"weights":[0.0]*len(AI_FEATURES),"steps":0,"trained":0,"updated_at":""},
    "27": {"weights":[0.0]*len(AI_FEATURES),"steps":0,"trained":0,"updated_at":""}
}

auto_state = {
    "last_draw_issue": "",
    "last_prediction_issue": "",
    "last_learning_issue": "",
    "last_stats_issue": "",
    "last_refresh_at": "",
    "last_error": "",
    "updating": False,
    "checkpoint_at": "",
    "checkpoint_error": "",
    "remote_backup_at": "",
    "remote_restore_at": "",
    "remote_backup_error": ""
}

fusion_lock = threading.RLock()
POOL_MODEL_PROFILES = {
    "T":"池T趋势",
    "Z":"池Z生肖",
    "C":"池C冷热",
    "W":"池W波色单双",
    "A":"池A纠错",
}
POOL_FINAL_PROFILE="池F最终"
TEN27_PROFILE="27码十期覆盖"
TEN27_V2_PROFILE="27十期23核4机动"
TEN27_CORE_PROFILE="27核心23"
TEN27_MOBILE_PROFILE="27机动4"
TEN27_28_PROFILE="27第28码"
TEN27_V3_PROFILE="27十期20窗变盘"
TEN27_V3_CORE_PROFILE="27V3核心23"
TEN27_V3_MOBILE_PROFILE="27V3机动4"
TEN27_V3_28_PROFILE="27V3第28码"
ten27_perf_lock=threading.RLock()
ten27_perf_cache={"ts":0.0,"data":None}
STABLE_SIGNAL_PROFILES={
    "双波":"稳双波",
    "7肖":"稳7肖",
    "大小":"稳大小",
    "单双":"稳单双",
    "波色单双3类":"稳波单双3",
    "0/4杀头":"稳0/4杀头",
}
pool_perf_lock=threading.RLock()
pool_perf_cache={"ts":0.0,"data":None}

fusion_cache = {
    "mix_pct": 35.0,
    "ai_rate60": 0.0,
    "stat_rate60": 0.0,
    "ai_rate12": 0.0,
    "stat_rate12": 0.0,
    "samples": 0,
    "benchmark_profile": "平衡",
    "advantage_pct": 0.0,
    "reason": "实盘样本不足，使用35%保守融合",
    "updated_at": ""
}


RED_NUMS = {1,2,7,8,12,13,18,19,23,24,29,30,34,35,40,45,46}
BLUE_NUMS = {3,4,9,10,14,15,20,25,26,31,36,37,41,42,47,48}
GREEN_NUMS = {5,6,11,16,17,21,22,27,28,32,33,38,39,43,44,49}
ALL_ZODIACS = ["鼠","牛","虎","兔","龙","蛇","马","羊","猴","鸡","狗","猪"]

INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#08101d">
<title>三分彩智能看板</title>
<style>
:root{
  --bg:#070b13; --panel:#101827; --line:#233149; --text:#f6f8fc; --muted:#8794aa;
  --accent:#6b8cff; --green:#35d491; --red:#ff566b; --blue:#4d8cff; --wavegreen:#34c77b;
}
*{box-sizing:border-box}
html,body{margin:0;background:radial-gradient(circle at 15% -10%,#1a2949 0,#0b1220 34%,#070b13 70%);color:var(--text);
font-family:-apple-system,BlinkMacSystemFont,"SF Pro Display","PingFang SC","Helvetica Neue",sans-serif}
body{min-height:100vh}
.wrap{max-width:780px;margin:auto;padding:calc(env(safe-area-inset-top) + 14px) 14px 36px}
.topbar{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin:2px 2px 14px}
.livebox{display:flex;align-items:center;gap:8px;font-size:12px;font-weight:700;padding:8px 11px;border-radius:999px;
background:#10231d;border:1px solid #1e5b45;color:#7ce4b4;white-space:nowrap}
.dot{width:8px;height:8px;border-radius:50%;background:#2fdb91;box-shadow:0 0 0 5px #2fdb9120;animation:pulse 1.6s infinite}
@keyframes pulse{50%{box-shadow:0 0 0 8px #2fdb9106}}
.title{font-size:27px;font-weight:850;letter-spacing:-.7px;line-height:1.05}
.subtitle{color:var(--muted);font-size:12px;margin-top:6px}
.card{background:linear-gradient(180deg,#111a2a,#0d1522);border:1px solid #1f2b40;border-radius:22px;padding:17px;
box-shadow:0 16px 40px #0000002c;margin-bottom:12px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.metricLabel{font-size:11px;color:var(--muted)}
.metric{font-size:20px;font-weight:850;margin-top:7px;letter-spacing:.2px}
.sectionHead{display:flex;align-items:flex-end;justify-content:space-between;gap:10px;margin-bottom:13px}
.sectionTitle{font-size:18px;font-weight:850}
.sectionHint{font-size:11px;color:var(--muted)}
.copyBtn{border:1px solid #38527f;background:linear-gradient(145deg,#223a66,#172a4b);color:#dbe8ff;
padding:7px 10px;border-radius:11px;font-size:11px;font-weight:800;cursor:pointer;-webkit-tap-highlight-color:transparent}
.copyBtn:active{transform:scale(.97)}
.toast{position:fixed;left:50%;bottom:calc(env(safe-area-inset-bottom) + 28px);transform:translateX(-50%);
background:#111a2a;border:1px solid #2c3a53;color:#fff;padding:10px 14px;border-radius:999px;font-size:12px;
box-shadow:0 12px 30px #0007;opacity:0;pointer-events:none;transition:.2s;z-index:99}
.toast.show{opacity:1}
.streakAlert{display:none;margin-bottom:12px;padding:11px 13px;border-radius:14px;
background:#2b1912;border:1px solid #8d4a2f;color:#ffd2b1;font-weight:800;font-size:12px;line-height:1.5}
.modelRow{display:grid;grid-template-columns:34px 1fr auto;gap:8px;align-items:center;padding:7px 0;border-bottom:1px solid #1f2b40}
.modelRow:last-child{border-bottom:0}.modelKey{font-weight:900;font-size:16px}.modelMeta{font-size:11px;color:var(--muted)}
.modelWeight{font-weight:850;font-size:13px}.tableWrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
.lockTable{width:100%;border-collapse:collapse;min-width:560px;font-size:11px}
.lockTable th,.lockTable td{padding:8px 7px;border-bottom:1px solid #223149;text-align:center;white-space:nowrap}
.lockTable th{color:var(--muted);font-weight:750}.hit{color:#43dda6;font-weight:900}.miss{color:#8d98aa;font-weight:850}
.balls{display:grid;grid-template-columns:repeat(7,1fr);gap:6px}
.ball,.smallball{display:flex;align-items:center;justify-content:center;font-weight:850;color:#fff}
.ball{aspect-ratio:1/1;border-radius:12px;font-size:14px;box-shadow:inset 0 1px 0 #ffffff20,0 4px 10px #00000018}
.smallball{width:31px;height:31px;border-radius:50%;font-size:12px}
.red{background:linear-gradient(145deg,#ff6b7b,#d93850)}
.blue{background:linear-gradient(145deg,#5a9cff,#3560d9)}
.green{background:linear-gradient(145deg,#48d997,#20a965)}
.special{outline:2px solid #fff;outline-offset:2px}
.latestRow{display:grid;grid-template-columns:repeat(7,1fr);gap:7px}
.combo{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.comboBox{background:#0c1421;border:1px solid #1e2a3f;border-radius:17px;padding:13px}
.comboTitle{font-size:13px;color:#aeb8c9;margin-bottom:10px;font-weight:750}
.code4{display:grid;grid-template-columns:repeat(4,1fr);gap:7px}
.zodiac4{display:grid;grid-template-columns:repeat(2,1fr);gap:7px}
.zodiac{padding:11px 6px;text-align:center;border-radius:13px;background:#172236;border:1px solid #263650;font-weight:850}
.zpairGrid{display:grid;grid-template-columns:repeat(4,1fr);gap:7px}
.zpair{background:#0c1421;border:1px solid #223149;border-radius:15px;padding:10px 6px;text-align:center;min-width:0}
.zpairName{font-size:17px;font-weight:900;margin-bottom:8px}
.zpairCodes{display:flex;gap:4px;justify-content:center;flex-wrap:wrap}
.microball{width:25px;height:25px;border-radius:50%;display:flex;align-items:center;justify-content:center;color:#fff;font-size:10px;font-weight:900}
.trendGrid{display:grid;grid-template-columns:repeat(3,1fr);gap:7px}
.strategyGrid{display:grid;grid-template-columns:repeat(2,1fr);gap:7px}
.strategyBox{background:#0c1421;border:1px solid #1f2d43;border-radius:14px;padding:10px}
.strategyTitle{font-size:10px;color:#8f9cb0;margin-bottom:6px}
.strategyMain{font-size:13px;font-weight:850;line-height:1.55}
.trendBox{background:#0c1421;border:1px solid #1f2d43;border-radius:14px;padding:10px}
.trendTitle{font-size:10px;color:#8f9cb0;margin-bottom:6px}
.trendMain{font-size:13px;font-weight:850;line-height:1.55}
.stats{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}
.stat{background:#0c1421;border:1px solid #1d2a40;border-radius:16px;padding:12px}
.statName{font-size:11px;color:#9ca8bb}.rate{font-size:22px;font-weight:900;margin-top:6px}.err{font-size:11px;color:#ff8492;margin-top:4px}
.historyCard{padding-bottom:10px}
.historyScroll{height:430px;overflow-y:auto;-webkit-overflow-scrolling:touch;overscroll-behavior:contain;padding-right:3px}
.historyItem{display:grid;grid-template-columns:112px 1fr;gap:10px;align-items:center;padding:11px 2px;border-bottom:1px solid #19253a}
.historyIssue{font-size:12px;font-weight:800;color:#c3ccda;word-break:break-all}
.historyNums{display:flex;gap:5px;flex-wrap:wrap}
.historyMeta{font-size:10px;color:#6f7d92;margin-top:4px}
.pillrow{display:flex;flex-wrap:wrap;gap:7px;margin-top:10px}
.pill{font-size:11px;padding:7px 9px;border-radius:999px;background:#101a2a;border:1px solid #26364e;color:#aeb9c9}
.pill.ok{color:#7de7b6;border-color:#225d49;background:#0f251f}
.foot{font-size:10px;color:#657389;line-height:1.55;text-align:center;padding:4px 6px 0}
@media(max-width:520px){
  .title{font-size:25px}
  .balls{grid-template-columns:repeat(7,1fr)}
  .ball{border-radius:11px;font-size:13px}
  .stats{grid-template-columns:1fr 1fr 1fr}
  .rate{font-size:20px}
  .historyItem{grid-template-columns:104px 1fr}
}

/* v34 compact mobile dashboard */
.wrap{max-width:860px;padding:calc(env(safe-area-inset-top) + 7px) 8px 22px}
.topbar{margin:0 1px 7px;gap:7px}.title{font-size:21px}.subtitle{font-size:9px;margin-top:3px}
.livebox{font-size:10px;padding:5px 8px}.card{border-radius:14px;padding:10px;margin-bottom:7px;box-shadow:0 9px 24px #00000020}
.sectionHead{margin-bottom:7px;gap:6px}.sectionTitle{font-size:14px}.sectionHint{font-size:9px}
.copyBtn{padding:5px 8px;border-radius:8px;font-size:9px}
.balls{gap:4px}.ball{border-radius:9px;font-size:12px}.smallball{width:25px;height:25px;font-size:10px}
.latestRow{gap:4px}.comboBox{padding:8px;border-radius:11px}.zpairGrid{gap:4px}.zpair{padding:6px 3px;border-radius:10px}
.zpairName{font-size:13px;margin-bottom:4px}.microball{width:21px;height:21px;font-size:9px}
.trendGrid{grid-template-columns:repeat(4,1fr);gap:4px}.strategyGrid{gap:4px}
.strategyBox,.trendBox{padding:6px;border-radius:10px}.strategyTitle,.trendTitle{font-size:8px;margin-bottom:3px}
.strategyMain,.trendMain{font-size:10px;line-height:1.35}.stats{gap:4px}.stat{padding:7px;border-radius:10px}
.statName{font-size:9px}.rate{font-size:17px;margin-top:3px}.err{font-size:9px;margin-top:2px}
.pillrow{gap:4px;margin-top:6px}.pill{font-size:9px;padding:4px 6px}.modelRow{grid-template-columns:25px 1fr auto;gap:5px;padding:4px 0}
.modelKey{font-size:13px}.modelMeta{font-size:9px}.modelWeight{font-size:11px}.lockTable{font-size:9px;min-width:500px}
.lockTable th,.lockTable td{padding:5px 4px}.historyScroll{height:260px}.historyItem{grid-template-columns:92px 1fr;padding:7px 1px;gap:6px}
.historyIssue{font-size:10px}.historyMeta{font-size:8px}.foot{font-size:8px;line-height:1.45}.streakAlert{padding:7px 9px;margin-bottom:7px;font-size:10px;border-radius:10px}
#pingteOne{font-size:26px!important}
.rescueModal{position:fixed;inset:0;background:#0009;display:none;align-items:center;justify-content:center;z-index:999;padding:18px}
.rescueModal.show{display:flex}.rescueBox{width:min(92vw,420px);background:#121b2b;border:1px solid #41577d;border-radius:16px;padding:16px;box-shadow:0 20px 60px #000a}
.rescueTitle{font-size:17px;font-weight:900;margin-bottom:8px}.rescueCode{font-size:32px;font-weight:950;text-align:center;margin:12px 0}
.rescueText{font-size:11px;color:#b6c2d4;line-height:1.55}.rescueClose{width:100%;margin-top:12px;padding:9px;border:0;border-radius:10px;background:#263d67;color:#fff;font-weight:850}
@media(max-width:430px){
  .trendGrid{grid-template-columns:repeat(4,1fr)}
  .grid2{gap:5px}.metric{font-size:16px}.metricLabel{font-size:9px}
}

/* v35 stable + denser layout */
.forecastCard{padding:11px}
.forecastCard .sectionTitle{font-size:15px}
.forecastGrid{grid-template-columns:repeat(2,1fr);align-items:stretch}
.forecastGrid .strategyBox{height:64px;min-height:64px;display:flex;flex-direction:column;justify-content:flex-start}
.forecastGrid .strategyMain{font-size:11px;line-height:1.35;height:31px;overflow:hidden}
.forecastGrid .strategyTitle{font-size:9px}
.poolCard{padding:8px}
#modelPoolRows{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:4px}
.modelMini{background:#0c1421;border:1px solid #1f2d43;border-radius:9px;padding:5px 3px;text-align:center;min-width:0}
.modelMiniKey{font-size:13px;font-weight:950;line-height:1}
.modelMiniWeight{font-size:11px;font-weight:900;margin-top:3px}
.modelMiniMeta{font-size:7.5px;color:#8f9cb0;line-height:1.25;margin-top:3px}
.poolSummary{margin-top:5px!important}
.poolSummary .pill{font-size:8px;padding:3px 5px}
.numberCard{padding:8px}
.numberCard .sectionHead{margin-bottom:5px}
.numberCard .sectionTitle{font-size:14px}
.numberCard .sectionHint{font-size:8.5px}
.compactBalls{grid-template-columns:repeat(10,1fr);gap:3px}
.compactBalls .ball{height:27px;aspect-ratio:auto;border-radius:7px;font-size:10px}
.miniInfoRow{display:flex;align-items:center;gap:6px;overflow-x:auto;white-space:nowrap;margin-top:5px;color:#91a0b6;font-size:8.5px;-webkit-overflow-scrolling:touch}
.miniInfoRow span{background:#101a2a;border:1px solid #26364e;border-radius:999px;padding:3px 5px}
.comboPredictCard{padding:8px}
.comboPredictGrid{display:grid;grid-template-columns:minmax(0,2.4fr) minmax(82px,.8fr);gap:7px;align-items:stretch}
.comboPredictCard .zpairGrid{grid-template-columns:repeat(4,1fr);gap:3px}
.comboPredictCard .zpair{padding:5px 2px;border-radius:8px}
.comboPredictCard .zpairName{font-size:11px;margin-bottom:3px}
.comboPredictCard .microball{width:19px;height:19px;font-size:8px}
.miniLabel{font-size:8px;color:#8f9cb0;margin-bottom:4px}
.pingteCompact{background:#0c1421;border:1px solid #223149;border-radius:9px;padding:6px;text-align:center;display:flex;flex-direction:column;justify-content:center}
.pingteValue{font-size:24px;font-weight:950;line-height:1.05}
#pingteSamples{margin-top:4px!important;font-size:7.5px}
@media(max-width:430px){
  .forecastGrid .strategyBox{height:66px;min-height:66px}
  .forecastGrid .strategyMain{font-size:10.5px}
  .compactBalls .ball{height:26px;font-size:9.5px}
}
</style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div class="livebox"><span class="dot"></span><span>实时录入</span></div>
    <div style="text-align:right">
      <div class="title">三分彩 · 智能看板</div>
      <div class="subtitle">TG自动入库 · 开奖秒显 · 极速窗口预测</div>
    </div>
  </div>

  <div class="grid2">
    <section class="card">
      <div class="metricLabel">最新期号</div>
      <div id="issue" class="metric">--</div>
    </section>
    <section class="card">
      <div class="metricLabel">下一期</div>
      <div id="nextIssue" class="metric">--</div>
    </section>
  </div>

  <section class="card">
    <div class="sectionHead"><div class="sectionTitle">开奖结果</div><div class="sectionHint">最新一期 · 前6码 + 特码</div></div>
    <div id="latestNums" class="latestRow"></div>
    <div class="pillrow">
      <span id="latestZodiac" class="pill"></span>
      <span id="lastIngest" class="pill ok"></span>
      <span id="tg" class="pill ok"></span><span id="adaptiveInfo" class="pill"></span><span id="calcState" class="pill"></span>
    </div>
  </section>

  <section class="card forecastCard">
    <div class="sectionHead">
      <div class="sectionTitle">下一期预测状态</div>
      <div class="sectionHint">不是把刚开奖号追进去</div>
    </div>
    <div class="strategyGrid forecastGrid">
      <div class="strategyBox"><div class="strategyTitle">预测目标</div><div id="forecastTarget" class="strategyMain">--</div></div>
      <div class="strategyBox"><div class="strategyTitle">前瞻模型</div><div id="forecastMode" class="strategyMain">--</div></div>
      <div class="strategyBox"><div class="strategyTitle">AI持续学习</div><div id="learningState" class="strategyMain">--</div></div>
      <div class="strategyBox"><div class="strategyTitle">60期校准</div><div id="learningProgress" class="strategyMain">--</div></div>
    </div>
  </section>

  <section class="card">
    <div class="sectionHead">
      <div class="sectionTitle">旧互补对照（保留历史验证）</div>
      <div class="sectionHint">真实前瞻记录 · 不倒推开奖结果</div>
    </div>
    <div class="strategyGrid">
      <div class="strategyBox"><div class="strategyTitle">双方都中</div><div id="compBoth" class="strategyMain">--</div></div>
      <div class="strategyBox"><div class="strategyTitle">AI独中</div><div id="compAIOnly" class="strategyMain">--</div></div>
      <div class="strategyBox"><div class="strategyTitle">趋势独中</div><div id="compTrendOnly" class="strategyMain">--</div></div>
      <div class="strategyBox"><div class="strategyTitle">双方都错</div><div id="compMiss" class="strategyMain">--</div></div>
    </div>
    <div class="pillrow">
      <span id="compSlots" class="pill"></span>
      <span id="compFinal" class="pill"></span>
    </div>
  </section>

  <div id="streakAlert" class="streakAlert"></div>

  <section class="card poolCard">
    <div class="sectionHead">
      <div>
        <div class="sectionTitle">多策略模型池</div>
        <div class="sectionHint">T趋势 / Z生肖 / C冷热 / W波色单双 / A纠错 · 全部开奖前锁单</div>
      </div>
    </div>
    <div id="modelPoolRows"></div>
    <div class="pillrow poolSummary">
      <span id="stableSummary" class="pill"></span>
      <span id="poolMaturity" class="pill"></span>
      <span id="legacyErrorInfo" class="pill"></span>
      <span id="fErrorRescueInfo" class="pill"></span>
    </div>
  </section>

  <section class="card">
    <div class="sectionHead">
      <div>
        <div class="sectionTitle">最近10期正式锁单结算</div>
        <div class="sectionHint">✓ 命中 · × 未中 · F为最终20码</div>
      </div>
    </div>
    <div class="tableWrap">
      <table class="lockTable">
        <thead><tr><th>期号</th><th>特码</th><th>T</th><th>Z</th><th>C</th><th>W</th><th>A</th><th>F</th></tr></thead>
        <tbody id="lockRows"><tr><td colspan="8">等待真实前瞻样本</td></tr></tbody>
      </table>
    </div>
  </section>

  <section class="card numberCard">
    <div class="sectionHead">
      <div>
        <div id="fDynamicTitle" class="sectionTitle">F动态 · 当前--码</div>
        <div class="sectionHint">默认20码 · 多模型共识优先 · 19–23动态伸缩</div>
      </div>
      <button class="copyBtn" onclick="copySpecial()">一键复制</button>
    </div>
    <div id="sp" class="balls compactBalls"></div>
    <div class="miniInfoRow">
      <span id="code20Brief">每期重算</span>
      <span id="code20Fusion">F自纠错</span>
    </div>
  </section>

  <section class="card numberCard">
    <div class="sectionHead">
      <div>
        <div class="sectionTitle">27码 · 10期长码</div>
        <div id="code27Hint" class="sectionHint">前20期找弱波色单双/0·4头 · 逆势连续2期才变码</div>
      </div>
      <button class="copyBtn" onclick="copy27()">一键复制</button>
    </div>
    <div id="sp27" class="balls compactBalls"></div>
    <div class="miniInfoRow">
      <span id="code27Block"></span>
      <span id="code27Kill"></span>
      <span id="code27Stats"></span>
    </div>
  </section>

  <section class="card comboPredictCard">
    <div class="sectionHead">
      <div class="sectionTitle">4肖4码 · 平特一肖</div>
      <div class="sectionHint">每期重算</div>
    </div>
    <div class="comboPredictGrid">
      <div>
        <div class="miniLabel">4肖 · 一肖一码</div>
        <div id="zpair" class="zpairGrid"></div>
      </div>
      <div class="pingteCompact">
        <div class="miniLabel">平特一肖</div>
        <div id="pingteOne" class="pingteValue">--</div>
        <div id="pingteSamples" class="sectionHint">--</div>
      </div>
    </div>
  </section>

  <section class="card">
    <div class="sectionHead">
      <div class="sectionTitle">走势指数</div>
      <div class="sectionHint">波色 / 大小 / 单双 / 红蓝绿×单双</div>
    </div>
    <div class="trendGrid">
      <div class="trendBox"><div class="trendTitle">波色走势</div><div id="waveTrend" class="trendMain">--</div></div>
      <div class="trendBox"><div class="trendTitle">大小指数</div><div id="sizeTrend" class="trendMain">--</div></div>
      <div class="trendBox"><div class="trendTitle">单双走势</div><div id="parityTrend" class="trendMain">--</div></div>
      <div class="trendBox"><div class="trendTitle">波色×单双</div><div id="waveParityTrend" class="trendMain">--</div></div>
    </div>
    <div class="pillrow"><span id="profileInfo" class="pill"></span></div>
  </section>

  <section class="card">
    <div class="sectionHead">
      <div>
        <div class="sectionTitle">趋势主攻 · AI纠错诊断</div>
        <div class="sectionHint">只用开奖前锁定记录判断错误发生在哪一层</div>
      </div>
    </div>
    <div class="strategyGrid">
      <div class="strategyBox"><div class="strategyTitle">当前状态</div><div id="regimeState" class="strategyMain">--</div></div>
      <div class="strategyBox"><div class="strategyTitle">20码纠错AI</div><div id="corr20State" class="strategyMain">--</div></div>
      <div class="strategyBox"><div class="strategyTitle">20码排名落点</div><div id="rank20State" class="strategyMain">--</div></div>
      <div class="strategyBox"><div class="strategyTitle">27码杀头验证</div><div id="head27State" class="strategyMain">--</div></div>
    </div>
    <div class="pillrow">
      <span id="zodiacTransitionState" class="pill"></span>
      <span id="failureState" class="pill"></span>
    </div>
  </section>

  <section class="card">
    <div class="sectionHead">
      <div class="sectionTitle">冷热 · 头数策略</div>
      <div class="sectionHint">历史条件成立才启用</div>
    </div>
    <div class="strategyGrid">
      <div class="strategyBox">
        <div class="strategyTitle">冷码防守</div>
        <div id="coldSignal" class="strategyMain">--</div>
      </div>
      <div class="strategyBox">
        <div class="strategyTitle">冷肖观察</div>
        <div id="coldZodiac" class="strategyMain">--</div>
      </div>
      <div class="strategyBox">
        <div class="strategyTitle">0头 / 4头</div>
        <div id="headSignal" class="strategyMain">--</div>
      </div>
      <div class="strategyBox">
        <div class="strategyTitle">牛马羊条件验证</div>
        <div id="nmySignal" class="strategyMain">--</div>
      </div>
    </div>
  </section>

  <section class="card">
    <div class="sectionHead"><div class="sectionTitle">滚动验证（后台更新）</div><div id="statsHint" class="sectionHint">等待样本</div></div>
    <div class="stats">
      <div class="stat"><div class="statName">F动态19–23</div><div id="hit22" class="rate">--</div><div id="err22" class="err"></div></div>
      <div class="stat"><div class="statName">4肖1码</div><div id="hit4" class="rate">--</div><div id="err4" class="err"></div></div>
      <div class="stat"><div class="statName">4肖</div><div id="hitZ" class="rate">--</div><div id="errZ" class="err"></div></div>
      <div class="stat"><div class="statName">平特一肖</div><div id="hitPingte" class="rate">--</div><div id="errPingte" class="err"></div></div>
    </div>
  </section>

  <section class="card historyCard">
    <div class="sectionHead">
      <div class="sectionTitle">历史开奖</div>
      <div id="historyCount" class="sectionHint">--</div>
    </div>
    <div id="historyScroll" class="historyScroll"></div>
  </section>

  <div id="rescueModal" class="rescueModal">
    <div class="rescueBox">
      <div class="rescueTitle">⚠️ 28码补位提醒</div>
      <div id="rescueModalCode" class="rescueCode">--</div>
      <div id="rescueModalText" class="rescueText">未来10期模型发现核心23+机动4之外的强覆盖号码。</div>
      <button class="rescueClose" onclick="closeRescueModal()">知道了</button>
    </div>
  </div>
  <div id="toast" class="toast">已复制</div>
  <div class="foot">号码颜色按红 / 蓝 / 绿波显示。v41的27码按长码10期运行：每轮先看前20期特码的红单/红双/蓝单/蓝双/绿单/绿双与0头/4头，弱结构进入排除；正常整轮不换码，只有被杀结构连续出现2期以上时判定可能变盘，再重新回看最新20期并按新的弱结构换码。另修复SQLite统计异常和每天480期后的跨日续期，旧预测缓存过期时会自动隐藏并重算。</div>
</div>

<script>
const RED=new Set([1,2,7,8,12,13,18,19,23,24,29,30,34,35,40,45,46]);
const BLUE=new Set([3,4,9,10,14,15,20,25,26,31,36,37,41,42,47,48]);
function cls(n){n=Number(n);return RED.has(n)?'red':(BLUE.has(n)?'blue':'green')}
let SPECIAL20=[];
let SPECIAL27=[];
function fmt(n){return String(n).padStart(2,'0')}
async function copySpecial(){
  const text=SPECIAL20.join(',');
  try{
    await navigator.clipboard.writeText(text);
  }catch(e){
    const ta=document.createElement('textarea');
    ta.value=text;document.body.appendChild(ta);ta.select();document.execCommand('copy');ta.remove();
  }
  const t=document.getElementById('toast');
  t.textContent='已复制：'+text;
  t.classList.add('show');
  setTimeout(()=>t.classList.remove('show'),1800);
}
async function copy27(){
  const text=SPECIAL27.join(',');
  try{ await navigator.clipboard.writeText(text); }
  catch(e){
    const ta=document.createElement('textarea');
    ta.value=text;document.body.appendChild(ta);ta.select();document.execCommand('copy');ta.remove();
  }
  const t=document.getElementById('toast');
  t.textContent='已复制27码：'+text;
  t.classList.add('show');
  setTimeout(()=>t.classList.remove('show'),1800);
}
function balls(nums,small=false){
  return (nums||[]).map((n,i)=>`<span class="${small?'smallball':'ball'} ${cls(n)} ${small&&i===6?'special':''}">${fmt(n)}</span>`).join('')
}
function closeRescueModal(){
  rescueModal.classList.remove('show');
}
function maybeShowRescue28(meta,targetIssue){
  const r=(meta||{}).rescue28||{};
  if(!r.active || !r.code) return;
  const key='rescue28_'+String(targetIssue||'')+'_'+String(r.code);
  if(localStorage.getItem(key)) return;
  rescueModalCode.textContent=fmt(r.code);
  rescueModalText.textContent=`${r.message||'发现强外码'} · ${r.support_models??0}/5模型支持 · 综合排名${r.rank??'--'}`;
  rescueModal.classList.add('show');
  localStorage.setItem(key,'1');
}
function hm(v){
  if(v===1) return '<span class="hit">✓</span>';
  if(v===0) return '<span class="miss">×</span>';
  return '--';
}
async function loadMain(){
  try{
    const r=await fetch('/api/prediction?_='+Date.now(),{cache:'no-store'});
    const d=await r.json();
    issue.textContent=d.issue||'暂无';
    nextIssue.textContent=d.next_issue||'--';
    latestNums.innerHTML=balls(d.latest_numbers||[]);
    if(latestNums.lastElementChild) latestNums.lastElementChild.classList.add('special');
    SPECIAL20=d.special20||d.special24||[]; sp.innerHTML=balls(SPECIAL20);
    SPECIAL27=d.special27||[]; sp27.innerHTML=balls(SPECIAL27);
    fDynamicTitle.textContent=d.stale_prediction?'F动态 · 重算中':`F动态 · 当前${SPECIAL20.length||'--'}码`;
    const mp=d.model_pool||{}, ms=mp.stats||{}, stable=d.stable_signals||{};
    const labels={T:'趋势',Z:'生肖',C:'冷热',W:'波色单双',A:'AI纠错'};
    modelPoolRows.innerHTML=['T','Z','C','W','A'].map(k=>{
      const x=ms[k]||{};
      return `<div class="modelMini">
        <div class="modelMiniKey">${k}</div>
        <div class="modelMiniWeight">${x.weight_pct??20}%</div>
        <div class="modelMiniMeta">错10:${x.e10??0}<br>错30:${x.e30??0}<br>连错:${x.miss_streak??0}</div>
      </div>`;
    }).join('');
    poolMaturity.textContent=`样本 ${mp.mature_n??0}/30`;
    const le=mp.legacy_errors||{}, lai=le.AI||{}, ltr=le.Trend||{};
    legacyErrorInfo.textContent=`旧AI错 ${(lai['60']||{}).misses??0}/${(lai['60']||{}).n??0} · ${le.trend_profile||'趋势'}错 ${(ltr['60']||{}).misses??0}/${(ltr['60']||{}).n??0}`;
    const si=(stable.items||{});
    stableSummary.textContent=`双波连中 ${(si['双波']||{}).current_streak??0} · 7肖连中 ${(si['7肖']||{}).current_streak??0}`;
    const warns=stable.warnings||[];
    if(warns.length){
      streakAlert.style.display='block';
      streakAlert.textContent='🔔 连中预警：'+warns.slice(0,4).map(x=>`${x.name} 已连续命中${x.streak}期`).join(' · ')+(warns.length>4?` · 另有${warns.length-4}项`:'');
    }else{
      streakAlert.style.display='none';
    }
    const recent=mp.recent10||[];
    lockRows.innerHTML=recent.length?recent.map(x=>`<tr><td>${x.issue}</td><td>${x.actual??'--'}</td><td>${hm(x.T)}</td><td>${hm(x.Z)}</td><td>${hm(x.C)}</td><td>${hm(x.W)}</td><td>${hm(x.A)}</td><td>${hm(x.F)}</td></tr>`).join(''):'<tr><td colspan="8">等待真实前瞻样本</td></tr>';
    const m20=d.strategy20||{}, m27=d.strategy27||{}, s20=d.stats20||{}, s27=d.stats27||{};
    const cs=m20.consensus||{};
    code20Brief.textContent=`F=${m20.dynamic_count??SPECIAL20.length}码 · 3/5保护 ${cs.protected3_count??0}码 · 2/5保 ${cs.protected2_count??0}/4`;
    code20Fusion.textContent=`模型池 ${cs.pool_primary_pct??m20.pool_mix_pct??0}% · 辅助 ${cs.aux_ai_trend_pct??0}% · 修正 ${cs.blind_rescue_pct??m20.error_rescue_pct??0}%`;
    const fed=d.f_error_diag||{};
    fErrorRescueInfo.textContent=`F错${fed.f_misses??0} · 融合漏${fed.fusion_miss??0} · 全池错${fed.pool_all_miss??0}`;
    const cur27=s27.current||{}, last27=s27.last_complete||null;
    const r28=m27.rescue28||{}, th=m27.ten_horizon||{}, f20=m27.filter20||{}, trg=m27.trend_trigger||{};
    const kw=(m27.killed_wave_parity||[]).join('、')||'无';
    code27Block.textContent=`第${m27.round_position??0}/10 · 核23 ${cur27.core23_hits??0}/${cur27.n??0} · 总${cur27.final_hits??0}/${cur27.n??0}`;
    code27Kill.textContent=`杀波 ${kw} · 杀头 ${m27.killed_head||'无'} · 段${m27.trend_segment??1}`;
    code27Stats.textContent=`机救${cur27.mobile4_rescues??0} · 28救${cur27.rescue28??0} · 变盘${cur27.trend_switches??m27.trend_switches??0}${r28.active?' · +28':''}`;
    maybeShowRescue28(m27,d.next_issue);
    const pairs=d.zodiac_pairs||[];
    zpair.innerHTML=pairs.map(p=>`<div class="zpair">
      <div class="zpairName">${p.zodiac}</div>
      <div class="zpairCodes"><span class="microball ${cls(p.code)}">${p.code}</span></div>
    </div>`).join('');
    pingteOne.textContent=d.pingte_yixiao||'--';
    pingteSamples.textContent=`转移样本 ${d.pingte_samples??0}`;
    const tr=d.trend||{};
    const w=tr.wave||{}, sz=tr.size||{}, pa=tr.parity||{}, wp=tr.wave_parity||{};
    waveTrend.innerHTML=`红 ${w['红']??0}%<br>蓝 ${w['蓝']??0}%<br>绿 ${w['绿']??0}%`;
    sizeTrend.innerHTML=`大 ${sz['大']??0}%<br>小 ${sz['小']??0}%`;
    parityTrend.innerHTML=`单 ${pa['单']??0}%<br>双 ${pa['双']??0}%`;
    waveParityTrend.innerHTML=`红单 ${wp['红单']??0}% · 红双 ${wp['红双']??0}%<br>蓝单 ${wp['蓝单']??0}% · 蓝双 ${wp['蓝双']??0}%<br>绿单 ${wp['绿单']??0}% · 绿双 ${wp['绿双']??0}%`;
    const ps=d.profile_scores||{};
    profileInfo.textContent=`当前模型 ${d.profile||'--'} · 校准${d.calibration_n??0}期 · 得分 ${ps[d.profile]??0}%`;
    const fc=d.forecast||{};
    forecastTarget.textContent=d.stale_prediction?`正在重算 ${d.next_issue||'--'} 期`:(fc.target_issue?`预测 ${fc.target_issue} 期`:'--');
    forecastMode.innerHTML=`多策略前瞻<br>转移 ${fc.transition_samples??0} · 长期 ${fc.long_prior_ready?'✓':'…'}`;
    const lr=d.learning||{};
    const ail=lr.ai_live||{};
    const au=lr.auto||{};
    const fu=lr.fusion||{};
    learningState.innerHTML=`模型池主导F<br>动态19–23码`;
    learningProgress.innerHTML=`${fu.reason||'动态评估中'}<br>AI实盘 ${fu.ai_rate60??ail.hit24??0}% · 最近12期 ${fu.ai_rate12??0}%`;
    const cp=d.complement||{};
    compBoth.textContent=`${cp.both_hit??0}/${cp.n??0}`;
    compAIOnly.textContent=`${cp.ai_only??0}/${cp.n??0}`;
    compTrendOnly.textContent=`${cp.trend_only??0}/${cp.n??0}`;
    compMiss.textContent=`${cp.both_miss??0}/${cp.n??0}`;
    compSlots.textContent=`第二码席位：AI ${cp.ai_second_slots??6} · 趋势 ${cp.trend_second_slots??6}`;
    compFinal.textContent=(cp.final_n??0)>0?`互补在线 ${cp.final_hits??0}/${cp.final_n} = ${cp.final_rate??0}%`:'互补在线：从本版开始独立验证';
    const dg20=d.diagnostics20||{}, dg27=d.diagnostics27||{}, cr=d.correction||{};
    const rb=dg20.rank_buckets||{}, fr=dg20.failure_reasons||{};
    regimeState.innerHTML=`20码：${m20.regime||'平衡'}<br>27码：${m27.regime||'平衡'}`;
    corr20State.innerHTML=`已学错题 ${(cr['20']||{}).trained??0} 次<br>纠错 ${m20.correction_weight_pct??0}% · 模型池 ${m20.pool_mix_pct??0}%`;
    rank20State.innerHTML=`1-10 ${rb['1-10']??0} · 11-20 ${rb['11-20']??0}<br>21-27 ${rb['21-27']??0} · 28+ ${rb['28+']??0}`;
    const kh=dg27.head_kill_by_head||{}, k0=kh['0头']||{}, k4=kh['4头']||{};
    head27State.innerHTML=`总 ${dg27.head_kill_success??0}/${dg27.head_kill_n??0} = ${dg27.head_kill_rate??0}%<br>杀0 ${k0.hits??0}/${k0.n??0} · 杀4 ${k4.hits??0}/${k4.n??0}`;
    const rescued=(m20.rescued_cold_zodiacs||[]);
    const swaps=(m20.edge_rescue_swaps||[]);
    zodiacTransitionState.textContent=`生肖转移：${(m20.zodiac_transition_top||[]).join('、')||'--'} · 冷肖救回 ${rescued.join('、')||'无'}`;
    failureState.textContent=`21-27复审替换 ${swaps.length}码 · 冷三肖误杀 ${dg20.cold_zodiac_errors??0} · 底层排序 ${fr['底层排序']??0}`;
    const sg=d.strategy||{};
    coldSignal.innerHTML=sg.cold_rebound_now?'冷反弹信号：启用':'冷反弹信号：普通';
    coldZodiac.innerHTML=(sg.cold_zodiacs||[]).length?`偏冷：${sg.cold_zodiacs.join('、')}`:'暂无';
    headSignal.innerHTML=sg.head_advice||'暂无';
    nmySignal.innerHTML=`样本 ${sg.nmy_samples??0}<br>条件 ${sg.nmy_conditional_pct??0}% / 基准 ${sg.nmy_baseline_pct??0}%`;
    latestZodiac.textContent=d.latest_special_zodiac?`特码生肖 ${d.latest_special_zodiac}`:'特码生肖 --';
    lastIngest.textContent=d.latest_created_at?`最后录入 ${d.latest_created_at}`:'实时录入';
    tg.textContent=d.telegram?'Telegram 已连接':'Telegram 未配置';
    adaptiveInfo.textContent=`前瞻预测：${d.next_issue||'--'}期 · F每期变 · 27码长码10期/变盘才换`;
    calcState.textContent=d.recalculating?'新期开奖已入库 · 模型重算中':'模型已更新';
    calcState.className=d.recalculating?'pill':'pill ok';

  }catch(e){}
}
async function loadStats(){
  try{
    const r=await fetch('/api/stats?_='+Date.now(),{cache:'no-store'});
    const st=await r.json();
    const c20=st.code20||{}, c27=st.code27||{}, ov27=c27.overall||{};
    const d20=st.diag20||{}, d27=st.diag27||{};
    statsHint.textContent=`20码 ${c20.hits??0}/${c20.n??0} · 27码 ${ov27.hits??0}/${ov27.n??0} · 冷三肖误杀 ${d20.cold_zodiac_errors??0}`;
    if(st.building){
      hit22.textContent='计算中'; err22.textContent='';
      hit4.textContent='计算中'; err4.textContent='';
      hitZ.textContent='计算中'; errZ.textContent='';
      hitPingte.textContent='计算中'; errPingte.textContent='';
    }else{
      hit22.textContent=(c20.rate??0).toFixed(1)+'%'; err22.textContent=`${c20.hits??0}中${c20.n??0}`;
      hit4.textContent=(st.hitMain??0).toFixed(1)+'%'; err4.textContent='错误 '+(st.errMain??0).toFixed(1)+'%';
      hitZ.textContent=(st.hitZ??0).toFixed(1)+'%'; errZ.textContent='错误 '+(st.errZ??0).toFixed(1)+'%';
      hitPingte.textContent=(st.hitPingte??0).toFixed(1)+'%'; errPingte.textContent='错误 '+(st.errPingte??0).toFixed(1)+'%';
    }
  }catch(e){}
}
async function loadAutoStatus(){
  try{
    const r=await fetch('/api/auto-status?_='+Date.now(),{cache:'no-store'});
    const a=await r.json();
    const n=a.ai_live||{};
    const f=a.fusion||{};
    learningState.innerHTML=`模型池主导F<br>动态19–23码`;
    learningProgress.innerHTML=`${f.reason||'动态评估中'}<br>${a.remote_backup_enabled?'学习数据：Supabase免费外部备份':(a.persistent?'学习数据：持久盘自动备份':'⚠ 学习数据：仅临时盘，重部署有丢失风险')}`;
  }catch(e){}
}
async function loadHistory(){
  try{
    const r=await fetch('/api/history?limit=200&_='+Date.now(),{cache:'no-store'});
    const d=await r.json();
    historyCount.textContent=`共 ${Number(d.total||0).toLocaleString()} 期 · 显示最近 ${d.items.length} 期`;
    historyScroll.innerHTML=d.items.map(x=>`
      <div class="historyItem">
        <div><div class="historyIssue">${x.issue}</div><div class="historyMeta">${x.zodiac||''}</div></div>
        <div class="historyNums">${balls(x.numbers,true)}</div>
      </div>`).join('');
  }catch(e){}
}
loadMain(); loadStats(); loadAutoStatus(); loadHistory();
setInterval(loadMain,500);
setInterval(loadAutoStatus,1000);
setInterval(loadStats,10000);
setInterval(loadHistory,5000);
</script>
</body>
</html>"""

def ensure_db_dir():
    parent = os.path.dirname(os.path.abspath(DB))
    os.makedirs(parent, exist_ok=True)

def connect():
    ensure_db_dir()
    c = sqlite3.connect(DB, timeout=20, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=20000")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("PRAGMA temp_store=MEMORY")
    return c

def _db_retry(fn, attempts=8):
    last=None
    for i in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            last=e
            if "locked" not in str(e).lower() and "busy" not in str(e).lower():
                raise
            time.sleep(min(0.08*(2**i),1.2))
    raise last

def init_db():
    with db_lock:
        c=connect()
        # WAL lets readers continue while Telegram writes the next draw.
        _db_retry(lambda: c.execute("PRAGMA journal_mode=WAL").fetchone())
        c.execute("PRAGMA wal_autocheckpoint=800")
        c.execute("""CREATE TABLE IF NOT EXISTS draws(
          issue TEXT PRIMARY KEY,
          n1 INTEGER,n2 INTEGER,n3 INTEGER,n4 INTEGER,n5 INTEGER,n6 INTEGER,special INTEGER,
          z1 TEXT,z2 TEXT,z3 TEXT,z4 TEXT,z5 TEXT,z6 TEXT,z7 TEXT,
          c1 TEXT,c2 TEXT,c3 TEXT,c4 TEXT,c5 TEXT,c6 TEXT,c7 TEXT,
          raw TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

        c.execute("""CREATE TABLE IF NOT EXISTS prediction_log(
          target_issue TEXT NOT NULL,
          profile TEXT NOT NULL,
          special24 TEXT NOT NULL,
          main4 TEXT NOT NULL,
          zodiac4 TEXT NOT NULL,
          pingte TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          settled INTEGER DEFAULT 0,
          hit24 INTEGER,
          hitmain INTEGER,
          hitz INTEGER,
          hitping INTEGER,
          actual_special INTEGER,
          actual_zodiac TEXT,
          PRIMARY KEY(target_issue,profile)
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS learner_scores(
          profile TEXT PRIMARY KEY,
          weight REAL DEFAULT 1.0,
          n INTEGER DEFAULT 0,
          hit24 INTEGER DEFAULT 0,
          hitmain INTEGER DEFAULT 0,
          hitz INTEGER DEFAULT 0,
          hitping INTEGER DEFAULT 0,
          score REAL DEFAULT 0.0,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS ai_model(
          id INTEGER PRIMARY KEY CHECK(id=1),
          weights TEXT NOT NULL,
          steps INTEGER DEFAULT 0,
          trained INTEGER DEFAULT 0,
          last_issue TEXT,
          lr REAL DEFAULT 0.08,
          historical_validation_n INTEGER DEFAULT 0,
          historical_hit24 REAL DEFAULT 0.0,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS correction_model(
          strategy TEXT PRIMARY KEY,
          weights TEXT NOT NULL,
          steps INTEGER DEFAULT 0,
          trained INTEGER DEFAULT 0,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        for _strategy in ("20","27"):
            c.execute("""INSERT OR IGNORE INTO correction_model
              (strategy,weights,steps,trained)
              VALUES (?,?,0,0)""",(_strategy,json.dumps([0.0]*len(AI_FEATURES))))
        c.execute("""CREATE TABLE IF NOT EXISTS strategy_audit(
          target_issue TEXT NOT NULL,
          profile TEXT NOT NULL,
          ranked49 TEXT,
          selected_codes TEXT,
          excluded_zodiacs TEXT,
          killed_head TEXT,
          regime TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          settled INTEGER DEFAULT 0,
          actual_special INTEGER,
          actual_zodiac TEXT,
          actual_rank INTEGER,
          selected_hit INTEGER,
          cold_zodiac_error INTEGER,
          head_kill_success INTEGER,
          failure_reason TEXT,
          PRIMARY KEY(target_issue,profile)
        )""")
        c.execute("""INSERT OR IGNORE INTO ai_model
          (id,weights,steps,trained,last_issue,lr)
          VALUES (1,?,0,0,'',0.08)""",(json.dumps([0.0]*len(AI_FEATURES)),))
        for _sql in [
            "ALTER TABLE ai_model ADD COLUMN rolling_window INTEGER DEFAULT 100",
            "ALTER TABLE ai_model ADD COLUMN rolling_trained INTEGER DEFAULT 0",
            "ALTER TABLE ai_model ADD COLUMN refit_count INTEGER DEFAULT 0",
            "ALTER TABLE ai_model ADD COLUMN last_refit_seconds REAL DEFAULT 0.0"
        ]:
            try:
                c.execute(_sql)
            except sqlite3.OperationalError:
                pass
        for _p in ["趋势快","平衡","热码","结构","均衡覆盖"]:
            c.execute("INSERT OR IGNORE INTO learner_scores(profile,weight) VALUES (?,1.0)",(_p,))
        c.commit(); c.close()

def normalize_z(z):
    return {"馬":"马","龍":"龙","雞":"鸡","豬":"猪"}.get(z,z)

def import_history_once():
    if not os.path.exists("history.csv"): return
    with db_lock:
        c=connect()
        existing=c.execute("SELECT COUNT(*) FROM draws").fetchone()[0]
        if existing:
            c.close(); return
        with open("history.csv","r",encoding="utf-8-sig",newline="") as f:
            for r in csv.DictReader(f):
                issue=(r.get("期号") or "").strip()
                try:
                    nums=[int(r[k]) for k in ["正码1","正码2","正码3","正码4","正码5","正码6","特码"]]
                except Exception:
                    continue
                if not issue or not all(1<=n<=49 for n in nums): continue
                zs=[normalize_z((r.get(k) or "").strip()) for k in
                    ["正码1生肖","正码2生肖","正码3生肖","正码4生肖","正码5生肖","正码6生肖"]]
                zs.append(normalize_z((r.get("生肖") or "").strip()))
                c.execute("""INSERT OR IGNORE INTO draws
                (issue,n1,n2,n3,n4,n5,n6,special,z1,z2,z3,z4,z5,z6,z7,raw)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",[issue,*nums,*zs,"history.csv"])
        c.commit(); c.close()


def _insert_migrated_draw(c, item):
    """Accept v12 /api/history item or v13 /api/export full row."""
    issue=str(item.get("issue") or item.get("期号") or "").strip()
    if not issue:
        return 0

    # Full export format.
    if all(k in item for k in ["n1","n2","n3","n4","n5","n6","special"]):
        try:
            nums=[int(item[f"n{i}"]) for i in range(1,7)] + [int(item["special"])]
        except Exception:
            return 0
        zs=[normalize_z(str(item.get(f"z{i}") or "")) for i in range(1,8)]
        cs=[str(item.get(f"c{i}") or "") for i in range(1,8)]
        raw=str(item.get("raw") or "remote-export")
        created=str(item.get("created_at") or "")
    else:
        # Old /api/history format: issue + numbers[7] + special zodiac only.
        try:
            nums=[int(x) for x in item.get("numbers",[])]
        except Exception:
            return 0
        if len(nums)!=7:
            return 0
        zs=["","","","","","",normalize_z(str(item.get("zodiac") or ""))]
        cs=[""]*7
        raw="remote-history-migration"
        created=str(item.get("created_at") or "")

    if len(set(nums))!=7 or not all(1<=n<=49 for n in nums):
        return 0

    before=c.total_changes
    c.execute("""INSERT OR IGNORE INTO draws
      (issue,n1,n2,n3,n4,n5,n6,special,z1,z2,z3,z4,z5,z6,z7,
       c1,c2,c3,c4,c5,c6,c7,raw,created_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))""",
      [issue,*nums,*zs,*cs,raw,created])
    return 1 if c.total_changes>before else 0

def sync_remote_history(url=None):
    """Merge history from the currently-live old service before switching versions."""
    url=(url or HISTORY_SOURCE_URL or "").strip()
    if not url:
        return 0
    try:
        # Try the exact supplied URL first.
        r=requests.get(url,timeout=25,headers={"User-Agent":"SanfenMigration/13"})
        r.raise_for_status()
        payload=r.json()
        items=payload.get("items") if isinstance(payload,dict) else payload
        if not isinstance(items,list):
            raise ValueError("remote history response has no items list")

        imported=0
        with db_lock:
            c=connect()
            for item in items:
                if isinstance(item,dict):
                    imported += _insert_migrated_draw(c,item)
            c.commit()
            c.close()

        sync_state["source"]=url
        sync_state["imported"]=imported
        sync_state["last_error"]=""
        sync_state["last_sync"]=time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[SYNC] imported={imported} from {url}",flush=True)
        return imported
    except Exception as e:
        sync_state["source"]=url
        sync_state["last_error"]=f"{type(e).__name__}: {e}"
        sync_state["last_sync"]=time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[SYNC] failed from {url}: {type(e).__name__}: {e}",flush=True)
        return 0

def sync_best_available_history():
    """Migrate from the deployment that is live right now, then configured fallbacks.
    On Render zero-downtime deploys, our public URL normally still serves the old
    deployment while this new worker is booting. This keeps bot-collected history
    across v13 -> v14 and later same-service upgrades."""
    urls=[]

    # Best source: this service's currently-live previous deployment.
    if WEBHOOK_BASE_URL:
        urls += [
          WEBHOOK_BASE_URL + "/api/export?limit=30000",
          WEBHOOK_BASE_URL + "/api/history?limit=500"
        ]

    # Configured explicit fallback/source.
    if HISTORY_SOURCE_URL:
        if "/api/" in HISTORY_SOURCE_URL:
            base=HISTORY_SOURCE_URL.split("/api/",1)[0]
            urls += [base+"/api/export?limit=30000"]
        urls += [HISTORY_SOURCE_URL]

    seen=set()
    total=0
    for u in urls:
        if not u or u in seen:
            continue
        seen.add(u)
        n=sync_remote_history(u)
        total += n
        if not sync_state["last_error"]:
            # Success includes 0 imported (everything already present).
            break
    return total

def parse_draw(text):
    m=re.search(r"第\s*[:：]?\s*(\d{8,})\s*期",text)
    if not m:return None
    issue=m.group(1)
    nums=None
    for line in [x.strip() for x in text.splitlines() if x.strip()]:
        vals=re.findall(r"(?<!\d)(?:0?[1-9]|[1-4]\d)(?!\d)",line)
        if len(vals)==7:
            cand=[int(x) for x in vals]
            if len(set(cand))==7 and all(1<=n<=49 for n in cand):
                nums=cand; break
    if not nums:return None
    zs=re.findall(r"[鼠牛虎兔龙龍蛇马馬羊猴鸡雞狗猪豬]",text)
    zs=[normalize_z(x) for x in zs[-7:]] if len(zs)>=7 else [""]*7
    colors=re.findall(r"[🔴🟢🔵]",text)
    colors=colors[-7:] if len(colors)>=7 else [""]*7
    return issue,nums,zs,colors

def latest_row():
    def _read():
        c=connect()
        try:
            return c.execute("""SELECT * FROM draws
                                ORDER BY CAST(issue AS INTEGER) DESC LIMIT 1""").fetchone()
        finally:
            c.close()
    return _db_retry(_read)

def _patch_live_cache_latest(issue, nums, zs):
    """Update visible latest result immediately, without waiting for the heavy model."""
    with live_cache_lock:
        data = dict(live_cache.get("data") or {})
        try:
            next_issue = str(int(issue) + 1)
        except Exception:
            next_issue = ""
        data.update({
            "issue": issue,
            "next_issue": next_issue,
            "latest_numbers": list(nums),
            "latest_special_zodiac": normalize_z(zs[6] if len(zs) >= 7 else ""),
            "latest_created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "recalculating": True
        })
        live_cache["issue"] = issue
        live_cache["data"] = data
        live_cache["building"] = False


def _refresh_learning_and_stats():
    """Refresh all derived learning/status values after AI/model work."""
    try:
        refresh_learner_cache(60)
        auto_state["last_stats_issue"]=str(learner_cache.get("settled",0))
    except Exception as e:
        auto_state["last_error"]=f"learner refresh: {e}"

def _auto_update_after_draw(issue, nums):
    """One unified post-draw pipeline. Nothing here requires a manual browser refresh."""
    auto_state["updating"]=True
    auto_state["last_draw_issue"]=str(issue)
    auto_state["last_error"]=""
    try:
        # 1) Train AI on the just-finished issue.
        train_ai_after_new_draw(issue, nums)
        auto_state["last_learning_issue"]=str(issue)

        # 2) Rebuild the live forecast for the NEXT issue.
        refresh_all_caches()

        # 3) Refresh learner/60-issue stats.
        _refresh_learning_and_stats()

        # 4) Record next-issue shadow predictions if cache rebuild did not already do it.
        try:
            rr=recent_rows(1200)
            if rr:
                auto_state["last_prediction_issue"]=_next_issue_id(rr[0]["issue"])
        except Exception:
            pass

        auto_state["last_refresh_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"[AUTO] draw={auto_state['last_draw_issue']} "
            f"learned={auto_state['last_learning_issue']} "
            f"next={auto_state['last_prediction_issue']} "
            f"at={auto_state['last_refresh_at']}",
            flush=True
        )
    except Exception as e:
        auto_state["last_error"]=f"{type(e).__name__}: {e}"
        print(f"[AUTO] failed issue={issue}: {auto_state['last_error']}",flush=True)
    finally:
        auto_state["updating"]=False

def add_draw(issue,nums,zs,colors,raw):
    if len(nums)!=7 or len(set(nums))!=7 or not all(1<=x<=49 for x in nums): return False
    prev_before_insert=latest_row()
    with db_lock:
        c=connect()
        before=c.total_changes
        c.execute("""INSERT OR IGNORE INTO draws
        (issue,n1,n2,n3,n4,n5,n6,special,z1,z2,z3,z4,z5,z6,z7,c1,c2,c3,c4,c5,c6,c7,raw)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [issue,*nums,*zs,*colors,raw[:4000]])
        c.commit()
        changed=c.total_changes>before
        c.close()
        if changed:
            update_long_prior_incremental(prev_before_insert, nums, zs)
            settle_predictions(issue, nums, zs)
            stats_cache["issue"]=None
            stats_cache["value"]=None
            _patch_live_cache_latest(issue, nums, zs)
            threading.Thread(
                target=_auto_update_after_draw,
                args=(issue, nums),
                daemon=True,
                name="auto-update-after-draw"
            ).start()
        return changed

def all_rows():
    def _read():
        c=connect()
        try:
            return c.execute("SELECT * FROM draws ORDER BY CAST(issue AS INTEGER) DESC").fetchall()
        finally:
            c.close()
    return _db_retry(_read)

def recent_rows(limit=1200):
    """Read only the recent history needed by the live predictor."""
    def _read():
        c=connect()
        try:
            return c.execute("""SELECT * FROM draws
                                ORDER BY CAST(issue AS INTEGER) DESC
                                LIMIT ?""",(int(limit),)).fetchall()
        finally:
            c.close()
    return _db_retry(_read)

def build_long_prior():
    """Build full-history priors once in the background.
    Live predictions never wait for this."""
    with long_prior_lock:
        if long_prior["building"]:
            return
        long_prior["building"] = True
    t0=time.time()
    try:
        rows=all_rows()  # one background scan of the full DB
        num_count=Counter()
        zodiac_count=Counter()
        trans_num=defaultdict(Counter)
        trans_z=defaultdict(Counter)

        for x in rows:
            n=x["special"]
            z=normalize_z(x["z7"] or "")
            if n:
                num_count[n]+=1
            if z:
                zodiac_count[z]+=1

        # rows newest -> oldest. state rows[j] -> next outcome rows[j-1]
        for j in range(1,len(rows)):
            state=rows[j]
            outcome=rows[j-1]
            sz=normalize_z(state["z7"] or "")
            on=outcome["special"]
            oz=normalize_z(outcome["z7"] or "")
            if sz and on:
                trans_num[sz][on]+=1
            if sz and oz:
                trans_z[sz][oz]+=1

        with long_prior_lock:
            long_prior["num_count"]=num_count
            long_prior["zodiac_count"]=zodiac_count
            long_prior["trans_num_by_zodiac"]=trans_num
            long_prior["trans_zodiac_by_zodiac"]=trans_z
            long_prior["total"]=len(rows)
            long_prior["ready"]=True
            long_prior["updated_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[PRIOR] full-history prior ready rows={len(rows)} in {time.time()-t0:.2f}s",flush=True)
    except Exception as e:
        print(f"[PRIOR] failed: {type(e).__name__}: {e}",flush=True)
    finally:
        with long_prior_lock:
            long_prior["building"]=False

def update_long_prior_incremental(prev_row, nums, zs):
    """O(1) update after each new draw; no full rescan."""
    with long_prior_lock:
        if not long_prior["ready"]:
            return
        new_special=int(nums[6])
        new_z=normalize_z(zs[6] if len(zs)>=7 else "")
        long_prior["num_count"][new_special]+=1
        if new_z:
            long_prior["zodiac_count"][new_z]+=1
        if prev_row is not None:
            prev_z=normalize_z(prev_row["z7"] or "")
            if prev_z:
                long_prior["trans_num_by_zodiac"][prev_z][new_special]+=1
                if new_z:
                    long_prior["trans_zodiac_by_zodiac"][prev_z][new_z]+=1
        long_prior["total"]+=1
        long_prior["updated_at"]=time.strftime("%Y-%m-%d %H:%M:%S")

def _long_prior_bonus(r):
    """Return tiny, stable full-history priors as normalized 0..1 scores."""
    num_bonus=defaultdict(float)
    z_bonus=defaultdict(float)
    if not r:
        return num_bonus,z_bonus,False

    with long_prior_lock:
        if not long_prior["ready"]:
            return num_bonus,z_bonus,False
        numc=Counter(long_prior["num_count"])
        latest_z=normalize_z(r[0]["z7"] or "")
        trans_num=Counter(long_prior["trans_num_by_zodiac"].get(latest_z,{}))
        trans_z=Counter(long_prior["trans_zodiac_by_zodiac"].get(latest_z,{}))

    def normalize_counter(c, keys):
        vals=[c.get(k,0) for k in keys]
        if not vals:
            return {}
        lo=min(vals); hi=max(vals)
        if hi==lo:
            return {k:.5 for k in keys}
        return {k:(c.get(k,0)-lo)/(hi-lo) for k in keys}

    base=normalize_counter(numc, range(1,50))
    trn=normalize_counter(trans_num, range(1,50))
    trz=normalize_counter(trans_z, ALL_ZODIACS)
    for n in range(1,50):
        # Long history is a stabilizer, not the driver.
        num_bonus[n]=.42*base.get(n,.5)+.58*trn.get(n,.5)
    for z in ALL_ZODIACS:
        z_bonus[z]=trz.get(z,.5)
    return num_bonus,z_bonus,True


def refresh_history_cache(limit=500):
    def _read():
        c=connect()
        try:
            total=c.execute("SELECT COUNT(*) FROM draws").fetchone()[0]
            rr=c.execute("""SELECT issue,n1,n2,n3,n4,n5,n6,special,z7,created_at
                            FROM draws ORDER BY CAST(issue AS INTEGER) DESC LIMIT ?""",(limit,)).fetchall()
            return total,rr
        finally:
            c.close()
    total,rr=_db_retry(_read)
    items=[]
    for x in rr:
        items.append({
          "issue":x["issue"],
          "numbers":[x[f"n{i}"] for i in range(1,7)]+[x["special"]],
          "zodiac":normalize_z(x["z7"] or ""),
          "created_at":x["created_at"] or ""
        })
    with history_cache_lock:
        history_cache["total"]=total
        history_cache["items"]=items
        history_cache["loaded_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
    return total

def refresh_all_caches():
    try:
        refresh_history_cache()
    except Exception as e:
        print(f"[CACHE] history refresh failed: {type(e).__name__}: {e}",flush=True)
    try:
        rebuild_live_cache()
    except Exception as e:
        print(f"[CACHE] model refresh failed: {type(e).__name__}: {e}",flush=True)
    # Backtest is never part of the critical update path.
    # Stats are deliberately not started here; /api/stats will start them lazily
    # after the live forecast is already available.

def exp_weight(i,half_life):
    return 0.5 ** (i/max(half_life,1))

def wave_of(n):
    if n in RED_NUMS: return "红"
    if n in BLUE_NUMS: return "蓝"
    return "绿"

def size_of(n):
    return "大" if n >= 25 else "小"

def parity_of(n):
    return "单" if n % 2 else "双"

def _weighted_category(seq, mapper, horizon, half):
    out=defaultdict(float)
    total=0.0
    for i,n in enumerate(seq[:min(horizon,len(seq))]):
        w=exp_weight(i,half)
        out[mapper(n)] += w
        total += w
    if total <= 0:
        return {}
    return {k:v/total for k,v in out.items()}

def _trend_profiles(r):
    sp=[x["special"] for x in r if x["special"]]
    wave=defaultdict(float); size=defaultdict(float); parity=defaultdict(float)
    cfg=[(8,3,1.70),(16,5,1.35),(36,11,1.00),(80,25,.55)]
    totalcoef=sum(c for _,_,c in cfg)
    for h,half,c in cfg:
        for k,v in _weighted_category(sp,wave_of,h,half).items(): wave[k]+=c*v
        for k,v in _weighted_category(sp,size_of,h,half).items(): size[k]+=c*v
        for k,v in _weighted_category(sp,parity_of,h,half).items(): parity[k]+=c*v
    for d in (wave,size,parity):
        for k in list(d): d[k]/=totalcoef

    def accel(mapper, keys):
        a=Counter(mapper(x["special"]) for x in r[:8])
        b=Counter(mapper(x["special"]) for x in r[8:32])
        av={k:a[k]/max(1,min(8,len(r))) for k in keys}
        bv={k:b[k]/max(1,min(24,max(0,len(r)-8))) for k in keys}
        return {k:av[k]-bv[k] for k in keys}

    def blend(base, ac, keys):
        raw={k:max(.001,base.get(k,0)+.46*ac.get(k,0)) for k in keys}
        z=sum(raw.values())
        return {k:raw[k]/z for k in keys}

    wp_keys=["红单","红双","蓝单","蓝双","绿单","绿双"]
    wp_mapper=lambda n: f"{wave_of(n)}{parity_of(n)}"
    wp=defaultdict(float)
    for h,half,c in cfg:
        for k,v in _weighted_category(sp,wp_mapper,h,half).items():
            wp[k]+=c*v
    for k in list(wp):
        wp[k]/=totalcoef

    return {
      "wave":blend(wave,accel(wave_of,["红","蓝","绿"]),["红","蓝","绿"]),
      "size":blend(size,accel(size_of,["大","小"]),["大","小"]),
      "parity":blend(parity,accel(parity_of,["单","双"]),["单","双"]),
      "wave_parity":blend(wp,accel(wp_mapper,wp_keys),wp_keys)
    }

def _number_zodiac_map(r):
    counts={n:Counter() for n in range(1,50)}
    for x in r[:min(300,len(r))]:
        for j in range(1,7):
            n=x[f"n{j}"]; z=normalize_z(x[f"z{j}"] or "")
            if n and z: counts[n][z]+=1
        n=x["special"]; z=normalize_z(x["z7"] or "")
        if n and z: counts[n][z]+=2
    return {n:c.most_common(1)[0][0] for n,c in counts.items() if c}

PROFILE_LIBRARY = {
  "趋势快": {"recent":1.30,"accel":1.35,"wave":1.20,"size":1.05,"parity":1.00,"zodiac":1.10,"long":.55,"omit":.45},
  "平衡":   {"recent":1.00,"accel":1.00,"wave":1.00,"size":1.00,"parity":1.00,"zodiac":1.00,"long":1.00,"omit":.75},
  "热码":   {"recent":1.45,"accel":.90,"wave":.70,"size":.65,"parity":.65,"zodiac":.80,"long":.45,"omit":.25},
  "结构":   {"recent":.88,"accel":1.05,"wave":1.45,"size":1.30,"parity":1.25,"zodiac":1.35,"long":.70,"omit":.45},
  "均衡覆盖":{"recent":.82,"accel":.82,"wave":1.10,"size":1.10,"parity":1.10,"zodiac":1.40,"long":1.10,"omit":.90}
}

FORECAST_BLEND = {
  "趋势快": {"transition":2.55,"hazard":.55,"tail":.50,"anti_chase":.72},
  "平衡": {"transition":2.20,"hazard":.75,"tail":.58,"anti_chase":.65},
  "热码": {"transition":1.85,"hazard":.42,"tail":.42,"anti_chase":.52},
  "结构": {"transition":2.35,"hazard":.68,"tail":.72,"anti_chase":.68},
  "均衡覆盖": {"transition":2.05,"hazard":1.05,"tail":.55,"anti_chase":.60}
}

def _zodiac_scores_profile(r, profile):
    p=PROFILE_LIBRARY[profile]
    score=defaultdict(float)
    for horizon,half,coef in [(6,2,2.30),(12,4,1.90),(24,8,1.38),(50,16,.90),(120,38,.45)]:
        for i,x in enumerate(r[:min(horizon,len(r))]):
            w=coef*exp_weight(i,half)*p["recent"]
            if x["z7"]:
                score[normalize_z(x["z7"])]+=1.75*w
            for k in ["z1","z2","z3","z4","z5","z6"]:
                if x[k]:
                    score[normalize_z(x[k])]+=.26*w

    a=Counter(normalize_z(x["z7"] or "") for x in r[:8] if x["z7"])
    b=Counter(normalize_z(x["z7"] or "") for x in r[8:32] if x["z7"])
    for z in set(score)|set(a)|set(b):
        score[z]+=p["accel"]*2.15*(a[z]/max(1,min(8,len(r)))-b[z]/max(1,min(24,max(0,len(r)-8))))
    return score

def _number_scores_profile(r, profile):
    p=PROFILE_LIBRARY[profile]
    numbers=range(1,50)
    score={n:0.0 for n in numbers}
    trend=_trend_profiles(r)
    zmap=_number_zodiac_map(r)
    zscore=_zodiac_scores_profile(r,profile)

    for horizon,half,coef in [(8,3,2.40),(16,5,1.95),(36,11,1.45),(80,25,1.00),(200,65,.55),(800,250,.22)]:
        rr=r[:min(horizon,len(r))]
        freq=Counter()
        for i,x in enumerate(rr):
            freq[x["special"]]+=exp_weight(i,half)
        mean=sum(freq.values())/49.0 if rr else 0
        factor=p["recent"] if horizon<=80 else p["long"]
        for n in numbers:
            score[n]+=coef*factor*(freq[n]-mean)/math.sqrt(mean+1.0)

    for short,base,coef in [(8,24,1.55),(16,48,1.00)]:
        a=Counter(x["special"] for x in r[:short])
        b=Counter(x["special"] for x in r[short:short+base])
        for n in numbers:
            score[n]+=coef*p["accel"]*(a[n]/max(short,1)-b[n]/max(base,1))

    for n in numbers:
        score[n]+=1.12*p["wave"]*(trend["wave"].get(wave_of(n),0)-1/3)
        score[n]+=.82*p["size"]*(trend["size"].get(size_of(n),0)-1/2)
        score[n]+=.76*p["parity"]*(trend["parity"].get(parity_of(n),0)-1/2)
        score[n]+=.88*p["wave"]*(trend["wave_parity"].get(f"{wave_of(n)}{parity_of(n)}",0)-1/6)
        z=zmap.get(n)
        if z:
            score[n]+=.012*p["zodiac"]*zscore.get(z,0)

    full=Counter(x["special"] for x in r)
    expected=len(r)/49.0
    for n in numbers:
        score[n]+=.13*p["long"]*(full[n]-expected)/math.sqrt(expected+7.0)

    last={n:len(r) for n in numbers}
    for i,x in enumerate(r):
        if last[x["special"]]==len(r): last[x["special"]]=i
    for n in numbers:
        score[n]+=.11*p["omit"]*min(last[n],50)/50.0
    return score

def head_of(n):
    if 1 <= n <= 9: return "0头"
    if 10 <= n <= 19: return "1头"
    if 20 <= n <= 29: return "2头"
    if 30 <= n <= 39: return "3头"
    return "4头"

HEAD_BASE = {"0头":9/49,"1头":10/49,"2头":10/49,"3头":10/49,"4头":10/49}

def _cold_metrics(r):
    """Current omission/coldness for numbers and zodiacs."""
    num_gap={n:len(r) for n in range(1,50)}
    z_gap={z:len(r) for z in ALL_ZODIACS}

    for i,x in enumerate(r):
        n=x["special"]
        if num_gap[n]==len(r):
            num_gap[n]=i
        z=normalize_z(x["z7"] or "")
        if z in z_gap and z_gap[z]==len(r):
            z_gap[z]=i

    # Recent occurrence rates.
    c12=Counter(x["special"] for x in r[:12])
    c36=Counter(x["special"] for x in r[:36])
    z12=Counter(normalize_z(x["z7"] or "") for x in r[:12] if x["z7"])
    z36=Counter(normalize_z(x["z7"] or "") for x in r[:36] if x["z7"])

    num_cold={}
    for n in range(1,50):
        gap=min(num_gap[n],40)/40.0
        scarcity=1.0-min(c12[n]/max(1,12/49),1.8)/1.8
        medium=1.0-min(c36[n]/max(1,36/49),1.8)/1.8
        num_cold[n]=0.55*gap+0.30*scarcity+0.15*medium

    z_cold={}
    for z in ALL_ZODIACS:
        gap=min(z_gap[z],24)/24.0
        scarcity=1.0-min(z12[z]/max(1,12/12),1.8)/1.8
        medium=1.0-min(z36[z]/max(1,36/12),1.8)/1.8
        z_cold[z]=0.55*gap+0.30*scarcity+0.15*medium

    return num_cold,z_cold,num_gap,z_gap

def _nmy_cold_rebound_lift(r, lookback=300, cold_window=12):
    """Empirically test the user's '牛/马/羊后冷码' idea.
    A transition counts as a cold rebound if the next special had not appeared
    in the preceding cold_window special draws. Returns conditional lift vs all transitions."""
    nmy={"牛","马","羊"}
    maxj=min(len(r)-cold_window-2, lookback)
    if maxj <= 20:
        return {"samples":0,"conditional":0.0,"baseline":0.0,"lift":0.0,"active":False}

    cond_hits=cond_n=base_hits=base_n=0
    for j in range(maxj):
        outcome=r[j]["special"]
        prior_z=normalize_z(r[j+1]["z7"] or "")
        older=[r[k]["special"] for k in range(j+2,min(j+2+cold_window,len(r)))]
        is_cold = outcome not in older
        base_n += 1
        base_hits += int(is_cold)
        if prior_z in nmy:
            cond_n += 1
            cond_hits += int(is_cold)

    baseline=base_hits/base_n if base_n else 0.0
    conditional=cond_hits/cond_n if cond_n else 0.0
    lift=conditional-baseline
    active=cond_n>=18 and lift>=0.07
    return {
      "samples":cond_n,
      "conditional":round(conditional,3),
      "baseline":round(baseline,3),
      "lift":round(lift,3),
      "active":active
    }

def _head_trend(r):
    """Head-number strength normalized by theoretical head sizes.
    Returns weak-head guidance; algorithm only downweights, never hard-excludes."""
    cfg=[(8,3,1.55),(16,5,1.25),(36,11,.90),(80,25,.55)]
    score={h:0.0 for h in HEAD_BASE}
    totalcoef=sum(c for _,_,c in cfg)
    sp=[x["special"] for x in r]

    for horizon,half,coef in cfg:
        wsum=0.0
        tmp=defaultdict(float)
        for i,n in enumerate(sp[:min(horizon,len(sp))]):
            w=exp_weight(i,half)
            tmp[head_of(n)] += w
            wsum += w
        for h in HEAD_BASE:
            share=(tmp[h]/wsum) if wsum else 0.0
            # Normalize by theoretical probability so 0头 isn't unfairly penalized.
            score[h] += coef*(share/HEAD_BASE[h])

    for h in score:
        score[h] /= totalcoef

    # Acceleration: last 8 vs previous 24.
    a=Counter(head_of(x["special"]) for x in r[:8])
    b=Counter(head_of(x["special"]) for x in r[8:32])
    for h in score:
        recent=a[h]/max(1,min(8,len(r)))
        prev=b[h]/max(1,min(24,max(0,len(r)-8)))
        score[h] += 0.55*(recent-prev)/HEAD_BASE[h]

    ranked=sorted(score,key=lambda h:(score[h],h))
    weakest=ranked[0]
    weak04=min(["0头","4头"],key=lambda h:score[h])
    weak04_strength=score[weak04]
    # Only call it a "kill/downweight" signal if it is meaningfully weak.
    if weak04_strength < 0.72:
        advice=f"{weak04}偏弱（降权）"
        active=weak04
    else:
        advice="0头/4头暂无明显弱势"
        active=""
    return {
      "strength":{h:round(score[h],2) for h in score},
      "weakest":weakest,
      "active_04":active,
      "advice":advice
    }

def _strategy_context(r):
    num_cold,z_cold,num_gap,z_gap=_cold_metrics(r)
    rebound=_nmy_cold_rebound_lift(r)
    head=_head_trend(r)
    latest_z=normalize_z(r[0]["z7"] or "") if r else ""
    nmy_now=latest_z in {"牛","马","羊"}
    cold_rebound_now=bool(nmy_now and rebound["active"])
    cold_zodiacs=sorted(ALL_ZODIACS,key=lambda z:(-z_cold.get(z,0),z))[:3]
    return {
      "num_cold":num_cold,
      "z_cold":z_cold,
      "num_gap":num_gap,
      "z_gap":z_gap,
      "nmy_rebound":rebound,
      "latest_zodiac":latest_z,
      "cold_rebound_now":cold_rebound_now,
      "cold_zodiacs":cold_zodiacs,
      "head":head
    }

def _forward_transition_scores(r):
    """One-step-ahead transition model.
    Uses historical state(t) -> result(t+1) pairs. The current latest draw is only
    used as the conditioning state; its number is NOT counted as a 'hot hit' for itself."""
    num_score=defaultdict(float)
    z_score=defaultdict(float)
    head_score=defaultdict(float)
    wave_score=defaultdict(float)
    size_score=defaultdict(float)
    parity_score=defaultdict(float)

    if len(r) < 80:
        return num_score,z_score,head_score,wave_score,size_score,parity_score,{"samples":0}

    cur=r[0]
    cur_n=cur["special"]
    cur_z=normalize_z(cur["z7"] or "")
    cur_head=head_of(cur_n)
    cur_wave=wave_of(cur_n)
    cur_size=size_of(cur_n)
    cur_parity=parity_of(cur_n)
    cur_tail=cur_n % 10
    cur_zset={normalize_z(cur[f"z{k}"] or "") for k in range(1,8) if cur[f"z{k}"]}

    samples=0
    exact_samples=0
    # r is newest -> oldest. Historical transition: r[j] (state) -> r[j-1] (next outcome).
    maxj=min(len(r)-1,700)
    for j in range(1,maxj):
        prev=r[j]
        out=r[j-1]
        pn=prev["special"]
        pz=normalize_z(prev["z7"] or "")
        on=out["special"]
        oz=normalize_z(out["z7"] or "")

        # Recency decay over historical transitions.
        w=exp_weight(j,260)
        match=0.0
        if pn==cur_n:
            match += 2.10
            exact_samples += 1
        if pz==cur_z:
            match += 1.65
        if head_of(pn)==cur_head:
            match += .70
        if wave_of(pn)==cur_wave:
            match += .55
        if size_of(pn)==cur_size:
            match += .45
        if parity_of(pn)==cur_parity:
            match += .40
        if pn % 10 == cur_tail:
            match += .48

        # Similarity of all 7 zodiacs in the state draw.
        pzset={normalize_z(prev[f"z{k}"] or "") for k in range(1,8) if prev[f"z{k}"]}
        if cur_zset and pzset:
            inter=len(cur_zset & pzset)
            union=max(1,len(cur_zset | pzset))
            match += .70*(inter/union)

        # Two-step context: previous zodiac/head sequence when available.
        if j+1 < len(r):
            older=r[j+1]
            if normalize_z(older["z7"] or "") == normalize_z(r[1]["z7"] or ""):
                match += .55
            if head_of(older["special"]) == head_of(r[1]["special"]):
                match += .25

        if match <= 0:
            continue

        ww=w*match
        samples += 1
        num_score[on] += ww
        if oz:
            z_score[oz] += ww
        head_score[head_of(on)] += ww
        wave_score[wave_of(on)] += ww
        size_score[size_of(on)] += ww
        parity_score[parity_of(on)] += ww

    # Normalize each family so it contributes comparably.
    def norm(d):
        if not d: return d
        vals=list(d.values())
        lo=min(vals); hi=max(vals)
        if hi-lo < 1e-9:
            return defaultdict(float,{k:0.5 for k in d})
        return defaultdict(float,{k:(v-lo)/(hi-lo) for k,v in d.items()})

    return (
      norm(num_score),norm(z_score),norm(head_score),
      norm(wave_score),norm(size_score),norm(parity_score),
      {"samples":samples,"exact_samples":exact_samples}
    )

def _base_hot_score_without_latest(r, profile):
    """Trend score for next-period prediction.
    Excludes r[0] from direct frequency counts to avoid simply chasing what just opened."""
    p=PROFILE_LIBRARY[profile]
    hist=r[1:] if len(r)>1 else r
    score={n:0.0 for n in range(1,50)}
    trend=_trend_profiles(hist if hist else r)

    for horizon,half,coef in [(6,2,2.05),(12,4,1.75),(24,8,1.35),(60,18,.92),(120,38,.48)]:
        for i,x in enumerate(hist[:min(horizon,len(hist))]):
            w=coef*exp_weight(i,half)
            score[x["special"]] += 1.50*w
            for j in range(1,7):
                score[x[f"n{j}"]] += .48*w

    # Acceleration also ends at r[1], not the just-opened r[0].
    now=Counter(); old=Counter()
    for x in hist[:6]:
        now[x["special"]]+=1.7
        for j in range(1,7): now[x[f"n{j}"]]+=.50
    for x in hist[6:24]:
        old[x["special"]]+=1.7
        for j in range(1,7): old[x[f"n{j}"]]+=.50

    for n in range(1,50):
        score[n] += .90*p["accel"]*(now[n]/6.0-old[n]/18.0)
        score[n] += .28*p["wave"]*trend["wave"].get(wave_of(n),0)
        score[n] += .17*p["size"]*trend["size"].get(size_of(n),0)
        score[n] += .16*p["parity"]*trend["parity"].get(parity_of(n),0)
        score[n] += .30*p["wave"]*trend["wave_parity"].get(f"{wave_of(n)}{parity_of(n)}",0)
    return score

def _gap_hazard_profile(r):
    """Empirical gap hazard from special-number intervals.
    This asks: after a number has been absent about g periods, how often did a
    number historically appear on the next period? It is a statistical modifier,
    not a guarantee that overdue numbers are 'due'."""
    if len(r) < 300:
        return {n:0.5 for n in range(1,50)}

    seq=[x["special"] for x in reversed(r)]  # oldest -> newest
    last={}
    intervals=[]
    for t,n in enumerate(seq):
        if n in last:
            intervals.append(t-last[n])
        last[n]=t

    # Hazard by gap bucket using completed intervals.
    buckets=[(0,2),(3,5),(6,9),(10,14),(15,21),(22,35),(36,9999)]
    hazard={}
    for lo,hi in buckets:
        at_risk=sum(1 for d in intervals if d>lo)
        events=sum(1 for d in intervals if lo < d <= hi)
        hazard[(lo,hi)]=(events/at_risk) if at_risk else 0.0

    # Current omission gaps.
    gaps={n:len(seq) for n in range(1,50)}
    for i,x in enumerate(r):
        n=x["special"]
        if gaps[n]==len(seq):
            gaps[n]=i

    raw={}
    for n,g in gaps.items():
        val=0.0
        for (lo,hi),h in hazard.items():
            if lo <= g <= hi:
                val=h
                break
        raw[n]=val

    vals=list(raw.values())
    lo=min(vals); hi=max(vals)
    if hi-lo < 1e-9:
        return {n:0.5 for n in raw}
    return {n:(v-lo)/(hi-lo) for n,v in raw.items()}

def _tail_transition_score(r):
    """Current special tail -> next special number empirical transition."""
    out=defaultdict(float)
    if len(r)<80:
        return out
    cur_tail=r[0]["special"] % 10
    for j in range(1,min(len(r)-1,700)):
        state=r[j]["special"]
        nxt=r[j-1]["special"]
        if state % 10 == cur_tail:
            out[nxt]+=exp_weight(j,260)
    if not out:
        return out
    vals=list(out.values()); lo=min(vals); hi=max(vals)
    if hi-lo<1e-9:
        return defaultdict(float,{k:.5 for k in out})
    return defaultdict(float,{k:(v-lo)/(hi-lo) for k,v in out.items()})

def _predictive_number_scores(r, profile, ctx=None):
    """Forecast score for NEXT issue.
    Ensemble: one-step transition + recent trend excluding latest + gap hazard
    + tail transition + cold/head safeguards."""
    ctx=ctx or _strategy_context(r)
    score=_base_hot_score_without_latest(r,profile)
    num_t,z_t,head_t,wave_t,size_t,par_t,meta=_forward_transition_scores(r)
    hazard=_gap_hazard_profile(r)
    tail_t=_tail_transition_score(r)
    blend=FORECAST_BLEND.get(profile,FORECAST_BLEND["平衡"])
    long_num,long_z,long_ready=_long_prior_bonus(r)

    weak04=ctx["head"].get("active_04","")
    latest_n=r[0]["special"] if r else None

    for n in range(1,50):
        score[n] += blend["transition"]*num_t[n]
        score[n] += .78*head_t[head_of(n)]
        score[n] += .65*wave_t[wave_of(n)]
        score[n] += .54*size_t[size_of(n)]
        score[n] += .50*par_t[parity_of(n)]
        score[n] += blend["hazard"]*hazard.get(n,.5)
        score[n] += blend["tail"]*tail_t[n]
        if long_ready:
            score[n] += .48*long_num[n]

        cold=ctx["num_cold"].get(n,0)
        score[n] += (0.72 if ctx["cold_rebound_now"] else 0.10)*cold

        if weak04 and head_of(n)==weak04:
            score[n] -= .62

        # Repeat is not banned. We only remove the artificial "just opened = hot"
        # effect; transition evidence may still put the same number back in.
        if latest_n is not None and n==latest_n:
            score[n] -= blend["anti_chase"]

    meta=dict(meta)
    meta["ensemble"]="转移+遗漏风险+尾数转移+结构"
    return score,meta,z_t

def _within_zodiac_pair_bonus(r, zmap):
    """Stabilized within-zodiac ranking for the 2-of-each-zodiac rule.
    Uses only past draws before the latest issue and Bayesian smoothing so one
    short streak cannot dominate the two selected numbers."""
    hist=r[1:] if len(r)>1 else r
    bonus=defaultdict(float)

    for horizon,half,coef in [(24,8,1.35),(80,24,1.00),(240,75,.70),(700,220,.35)]:
        counts=defaultdict(lambda: defaultdict(float))
        totals=defaultdict(float)
        rr=hist[:min(horizon,len(hist))]
        for i,x in enumerate(rr):
            n=x["special"]
            z=zmap.get(n)
            if not z:
                continue
            w=exp_weight(i,half)
            counts[z][n]+=w
            totals[z]+=w

        for z in ALL_ZODIACS:
            pool=[n for n in range(1,50) if zmap.get(n)==z]
            if not pool:
                continue
            # Symmetric pseudo-count prevents overreacting to sparse recent windows.
            alpha=.80
            denom=totals[z]+alpha*len(pool)
            for n in pool:
                share=(counts[z].get(n,0.0)+alpha)/max(denom,1e-9)
                bonus[n]+=coef*share

    return bonus

def _norm_pool(values, pool):
    if not pool:
        return {}
    arr=[values.get(n,0.0) for n in pool]
    lo=min(arr); hi=max(arr)
    if hi-lo<1e-9:
        return {n:.5 for n in pool}
    return {n:(values.get(n,0.0)-lo)/(hi-lo) for n in pool}


def load_ai_state():
    with db_lock:
        c=connect()
        try:
            row=c.execute("SELECT * FROM ai_model WHERE id=1").fetchone()
        finally:
            c.close()
    if not row:
        return
    try:
        w=json.loads(row["weights"] or "[]")
        if len(w)<len(AI_FEATURES):
            w=list(w)+[0.0]*(len(AI_FEATURES)-len(w))
        elif len(w)>len(AI_FEATURES):
            w=list(w)[:len(AI_FEATURES)]
    except Exception:
        w=[0.0]*len(AI_FEATURES)
    with ai_lock:
        ai_state["weights"]=[float(x) for x in w]
        ai_state["steps"]=int(row["steps"] or 0)
        ai_state["trained"]=int(row["trained"] or 0)
        ai_state["last_issue"]=str(row["last_issue"] or "")
        ai_state["lr"]=float(row["lr"] or .08)
        ai_state["ready"]=ai_state["trained"]>0
        ai_state["historical_validation_n"]=int(row["historical_validation_n"] or 0)
        ai_state["historical_hit24"]=float(row["historical_hit24"] or 0.0)
        ai_state["rolling_window"]=int(row["rolling_window"] or 100) if "rolling_window" in row.keys() else 100
        ai_state["rolling_trained"]=int(row["rolling_trained"] or 0) if "rolling_trained" in row.keys() else 0
        ai_state["refit_count"]=int(row["refit_count"] or 0) if "refit_count" in row.keys() else 0
        ai_state["last_refit_seconds"]=float(row["last_refit_seconds"] or 0.0) if "last_refit_seconds" in row.keys() else 0.0
        ai_state["updated_at"]=str(row["updated_at"] or "")

def save_ai_state():
    with ai_lock:
        payload=(
            json.dumps(ai_state["weights"],separators=(",",":")),
            int(ai_state["steps"]),int(ai_state["trained"]),
            str(ai_state["last_issue"]),float(ai_state["lr"]),
            int(ai_state["historical_validation_n"]),
            float(ai_state["historical_hit24"]),
            int(ai_state.get("rolling_window",100)),
            int(ai_state.get("rolling_trained",0)),
            int(ai_state.get("refit_count",0)),
            float(ai_state.get("last_refit_seconds",0.0))
        )
    with db_lock:
        c=connect()
        try:
            c.execute("""UPDATE ai_model SET
              weights=?,steps=?,trained=?,last_issue=?,lr=?,
              historical_validation_n=?,historical_hit24=?,
              rolling_window=?,rolling_trained=?,refit_count=?,last_refit_seconds=?,
              updated_at=CURRENT_TIMESTAMP WHERE id=1""",payload)
            c.commit()
        finally:
            c.close()

def _safe_ratio(v, denom):
    return float(v)/float(denom) if denom else 0.0


WAVE_PARITY_KEYS=["红单","红双","蓝单","蓝双","绿单","绿双"]

def wave_parity_of(n):
    return f"{wave_of(n)}{parity_of(n)}"

def _wave_parity_feature_maps(r):
    out={}
    base=Counter(wave_parity_of(n) for n in range(1,50))
    for horizon,half in [(8,3),(16,5),(36,11),(80,25)]:
        raw=defaultdict(float)
        totalw=0.0
        for i,x in enumerate(r[:min(horizon,len(r))]):
            w=exp_weight(i,half)
            raw[wave_parity_of(x["special"])]+=w
            totalw+=w
        strength={}
        for k in WAVE_PARITY_KEYS:
            share=raw[k]/totalw if totalw else 0.0
            baseline=base[k]/49.0
            strength[k]=share/max(baseline,1e-9)
        vals=list(strength.values())
        lo=min(vals); hi=max(vals)
        if hi-lo<1e-9:
            out[horizon]={k:.5 for k in WAVE_PARITY_KEYS}
        else:
            out[horizon]={k:(strength[k]-lo)/(hi-lo) for k in WAVE_PARITY_KEYS}
    return out

def _ai_feature_matrix(r):
    """49 candidate feature vectors built only from information available before target draw."""
    if not r:
        return {n:[1.0]+[0.0]*(len(AI_FEATURES)-1) for n in range(1,50)}

    # Special-number frequencies
    spc={}
    for h in (6,12,24,60):
        spc[h]=Counter(x["special"] for x in r[:min(h,len(r))])

    # All-seven-number frequencies
    all12=Counter()
    all36=Counter()
    for x in r[:min(12,len(r))]:
        for j in range(1,7): all12[x[f"n{j}"]]+=1
        all12[x["special"]]+=1
    for x in r[:min(36,len(r))]:
        for j in range(1,7): all36[x[f"n{j}"]]+=1
        all36[x["special"]]+=1

    # Gap / coldness
    num_cold,_zcold,num_gap,_zgap=_cold_metrics(r)

    # Forward transition features
    num_t,_z_t,_head_t,_wave_t,_size_t,_par_t,_meta=_forward_transition_scores(r)
    tail_t=_tail_transition_score(r)
    long_num,_long_z,long_ready=_long_prior_bonus(r)
    trend=_trend_profiles(r)
    head_ctx=_head_trend(r)

    # Within-zodiac stabilized history
    zmap=_number_zodiac_map(r)
    pair=_within_zodiac_pair_bonus(r,zmap)
    pair_norm={}
    for z in ALL_ZODIACS:
        pool=[n for n in range(1,50) if zmap.get(n)==z]
        pair_norm.update(_norm_pool(pair,pool))

    latest=r[0]["special"]
    head_strength=head_ctx.get("strength",{})
    wp_maps=_wave_parity_feature_maps(r)

    X={}
    for n in range(1,50):
        gap=min(num_gap.get(n,0),60)/60.0
        wp=wave_parity_of(n)
        X[n]=[
          1.0,
          _safe_ratio(spc[6][n],6),
          _safe_ratio(spc[12][n],12),
          _safe_ratio(spc[24][n],24),
          _safe_ratio(spc[60][n],60),
          _safe_ratio(all12[n],12*7),
          _safe_ratio(all36[n],36*7),
          gap,
          float(num_t[n]),
          float(tail_t[n]),
          float(long_num[n] if long_ready else .5),
          float(trend["wave"].get(wave_of(n),1/3)),
          float(trend["size"].get(size_of(n),.5)),
          float(trend["parity"].get(parity_of(n),.5)),
          float(head_strength.get(head_of(n),1.0)/2.0),
          float(num_cold.get(n,0.0)),
          1.0 if n==latest else 0.0,
          float(pair_norm.get(n,.5)),
          float(wp_maps[8].get(wp,.5)),
          float(wp_maps[16].get(wp,.5)),
          float(wp_maps[36].get(wp,.5)),
          float(wp_maps[80].get(wp,.5))
        ]
    return X

def _ai_logits_and_probs(r, weights=None):
    X=_ai_feature_matrix(r)
    with ai_lock:
        w=list(ai_state["weights"] if weights is None else weights)
    logits={}
    for n in range(1,50):
        x=X[n]
        logits[n]=sum(a*b for a,b in zip(w,x))
    mx=max(logits.values())
    ex={n:math.exp(max(-30,min(30,logits[n]-mx))) for n in logits}
    z=sum(ex.values()) or 1.0
    probs={n:ex[n]/z for n in ex}
    return X,logits,probs

def _normalize_ai_probs(probs):
    vals=list(probs.values())
    lo=min(vals); hi=max(vals)
    if hi-lo<1e-12:
        return {n:.5 for n in probs}
    return {n:(v-lo)/(hi-lo) for n,v in probs.items()}

def ai_train_one(state_rows, actual_special, issue="", persist=True):
    """Online softmax ranker: predict 1-of-49, then update by cross-entropy gradient."""
    if not state_rows:
        return
    X,_logits,probs=_ai_logits_and_probs(state_rows)
    y=int(actual_special)
    with ai_lock:
        w=list(ai_state["weights"])
        steps=int(ai_state["steps"])
        base_lr=float(ai_state["lr"])
    lr=base_lr/math.sqrt(1.0+steps/50.0)

    # Fallback online update keeps exact special as the main target.
    q={n:(0.70 if n==y else 0.30/48.0) for n in range(1,50)}
    model_expected=[0.0]*len(AI_FEATURES)
    target_expected=[0.0]*len(AI_FEATURES)
    for n,p in probs.items():
        for j,v in enumerate(X[n]):
            model_expected[j]+=p*v
            target_expected[j]+=q[n]*v
    grad=[target_expected[j]-model_expected[j] for j in range(len(AI_FEATURES))]

    # L2 shrink + clipping for stability.
    for j in range(len(w)):
        w[j]=(1.0-0.0008*lr)*w[j] + lr*grad[j]
        w[j]=max(-6.0,min(6.0,w[j]))

    with ai_lock:
        ai_state["weights"]=w
        ai_state["steps"]=steps+1
        ai_state["trained"]=int(ai_state["trained"])+1
        ai_state["last_issue"]=str(issue or "")
        ai_state["ready"]=True
        ai_state["updated_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
    if persist:
        save_ai_state()

def ai_live_validation_stats(window=60):
    """AI's own locked pre-draw predictions from prediction_log profile=AI在线."""
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT hit24,hitmain,hitz,hitping
                              FROM prediction_log
                              WHERE profile='AI在线' AND settled=1
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT ?""",(window,)).fetchall()
        finally:
            c.close()
    n=len(rows)
    if not n:
        return {"n":0,"hit24":0.0,"hitMain":0.0,"hitZ":0.0,"hitPingte":0.0}
    h24=sum(int(x["hit24"] or 0) for x in rows)
    hm=sum(int(x["hitmain"] or 0) for x in rows)
    hz=sum(int(x["hitz"] or 0) for x in rows)
    hp=sum(int(x["hitping"] or 0) for x in rows)
    return {
      "n":n,
      "hit24":round(100*h24/n,1),
      "hitMain":round(100*hm/n,1),
      "hitZ":round(100*hz/n,1),
      "hitPingte":round(100*hp/n,1)
    }



def _normalize_distribution(d):
    total=sum(max(0.0,float(v)) for v in d.values())
    if total <= 1e-12:
        return {n:1.0/49.0 for n in range(1,50)}
    return {n:max(0.0,float(d.get(n,0.0)))/total for n in range(1,50)}

def _uniform_over_numbers(nums):
    nums=list(dict.fromkeys(int(n) for n in nums if 1 <= int(n) <= 49))
    if not nums:
        return {n:0.0 for n in range(1,50)}
    p=1.0/len(nums)
    return {n:(p if n in nums else 0.0) for n in range(1,50)}

def _multitask_target_distribution(state_rows, outcome_row):
    """Shared AI target.

    58% trains exact special number.
    The remaining 42% teaches structures that the user also follows:
    special-zodiac, 7-position zodiac presence (平特一肖), wave, size,
    parity, head, and hot/cold regime. Because all heads update the SAME
    number-ranking weights, their learned signal is synchronized back into
    the final 24-code ranking.
    """
    y=int(outcome_row["special"])
    zmap=_number_zodiac_map(state_rows)
    actual_z=normalize_z(outcome_row["z7"] or "")
    draw_z=_draw_zodiac_set(outcome_row)
    actual_wave=wave_of(y)
    actual_size=size_of(y)
    actual_parity=parity_of(y)
    actual_head=head_of(y)

    exact={n:(1.0 if n==y else 0.0) for n in range(1,50)}
    special_z=_uniform_over_numbers([n for n in range(1,50) if zmap.get(n)==actual_z])
    pingte_z=_uniform_over_numbers([n for n in range(1,50) if zmap.get(n) in draw_z])
    wave_d=_uniform_over_numbers([n for n in range(1,50) if wave_of(n)==actual_wave])
    size_d=_uniform_over_numbers([n for n in range(1,50) if size_of(n)==actual_size])
    parity_d=_uniform_over_numbers([n for n in range(1,50) if parity_of(n)==actual_parity])
    combo_d=_uniform_over_numbers([n for n in range(1,50) if wave_parity_of(n)==wave_parity_of(y)])
    head_d=_uniform_over_numbers([n for n in range(1,50) if head_of(n)==actual_head])

    num_cold,_zc,_gap,_zg=_cold_metrics(state_rows)
    actual_cold=float(num_cold.get(y,0.0))
    cold_raw={n:max(0.0,1.0-abs(float(num_cold.get(n,0.0))-actual_cold)) for n in range(1,50)}
    cold_d=_normalize_distribution(cold_raw)

    parts=[
      (.56,exact),       # 特码
      (.10,special_z),   # 四肖/特码生肖
      (.06,pingte_z),    # 平特一肖
      (.10,combo_d),     # 红单/红双/蓝单/蓝双/绿单/绿双
      (.04,wave_d),      # 波色
      (.03,size_d),      # 大小
      (.02,parity_d),    # 单双
      (.04,head_d),      # 头数
      (.05,cold_d)       # 冷热
    ]
    q={n:0.0 for n in range(1,50)}
    for weight,dist in parts:
        for n in range(1,50):
            q[n]+=weight*float(dist.get(n,0.0))
    return _normalize_distribution(q)

def _ai_zodiac_mass(r):
    """Convert learned 01-49 AI probabilities into zodiac probabilities."""
    if not r:
        return {z:0.0 for z in ALL_ZODIACS}
    zmap=_number_zodiac_map(r)
    with ai_lock:
        ready=bool(ai_state.get("ready",False))
    if not ready:
        return {z:0.0 for z in ALL_ZODIACS}
    _X,_lg,probs=_ai_logits_and_probs(r)
    out={z:0.0 for z in ALL_ZODIACS}
    for n,p in probs.items():
        z=zmap.get(n)
        if z in out:
            out[z]+=float(p)
    total=sum(out.values()) or 1.0
    return {z:out[z]/total for z in out}

def _softmax_probs_from_X(X, weights):
    logits={}
    for n in range(1,50):
        logits[n]=sum(a*b for a,b in zip(weights,X[n]))
    mx=max(logits.values())
    ex={n:math.exp(max(-30,min(30,logits[n]-mx))) for n in logits}
    z=sum(ex.values()) or 1.0
    return {n:ex[n]/z for n in ex}

def retrain_ai_rolling_100(issue=""):
    """Re-fit the short-term AI from the latest 100 COMPLETED draws every issue.

    For each historical target inside the window, features are built only from
    rows older than that target. The target itself is never present in its input.
    This avoids repeatedly double-counting one new issue and makes the AI adapt
    to the latest 100-period regime.
    """
    with ai_lock:
        if ai_state.get("bootstrapping"):
            return
        ai_state["bootstrapping"]=True

    t0=time.time()
    try:
        # 100 target draws + enough older context for feature calculation.
        rows=recent_rows(360)
        if len(rows)<180:
            return

        target_count=min(100, len(rows)-140)
        if target_count < 30:
            return

        # Refit from a neutral seed each time. Long-history knowledge is already
        # present as an input feature, so old regimes do not dominate forever.
        weights=[0.0]*len(AI_FEATURES)
        base_lr=0.095
        steps=0

        # Oldest -> newest inside the latest 100 completed outcomes.
        for k in range(target_count-1,-1,-1):
            state=rows[k+1:]
            if len(state)<120:
                continue
            X=_ai_feature_matrix(state)
            probs=_softmax_probs_from_X(X,weights)
            target=_multitask_target_distribution(state, rows[k])

            model_expected=[0.0]*len(AI_FEATURES)
            target_expected=[0.0]*len(AI_FEATURES)
            for n,p in probs.items():
                xn=X[n]
                q=float(target.get(n,0.0))
                for j,v in enumerate(xn):
                    model_expected[j]+=p*v
                    target_expected[j]+=q*v
            grad=[target_expected[j]-model_expected[j] for j in range(len(AI_FEATURES))]

            # Give newer outcomes modestly more influence while still using all 100.
            recency=0.68 + 0.32*((target_count-k)/max(1,target_count))
            lr=(base_lr*recency)/math.sqrt(1.0+steps/70.0)
            for j in range(len(weights)):
                weights[j]=(1.0-0.0008*lr)*weights[j] + lr*grad[j]
                weights[j]=max(-6.0,min(6.0,weights[j]))
            steps+=1

        elapsed=time.time()-t0
        with ai_lock:
            ai_state["weights"]=weights
            ai_state["steps"]=int(ai_state.get("steps",0))+steps
            ai_state["trained"]=max(int(ai_state.get("trained",0)),target_count)
            ai_state["rolling_window"]=100
            ai_state["rolling_trained"]=target_count
            ai_state["refit_count"]=int(ai_state.get("refit_count",0))+1
            ai_state["last_refit_seconds"]=round(elapsed,2)
            ai_state["last_issue"]=str(issue or (rows[0]["issue"] if rows else ""))
            ai_state["ready"]=steps>0
            ai_state["updated_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
        save_ai_state()
        print(
            f"[AI100] refit issue={issue or rows[0]['issue']} "
            f"window={target_count} steps={steps} in {elapsed:.2f}s",
            flush=True
        )
    except Exception as e:
        print(f"[AI100] refit failed: {type(e).__name__}: {e}",flush=True)
    finally:
        with ai_lock:
            ai_state["bootstrapping"]=False

def bootstrap_ai_history():
    """On deploy/restart, rebuild the current rolling-100 AI in the background."""
    retrain_ai_rolling_100("boot")



def train_correction_after_new_draw(issue, actual_special):
    """Use the exact state that existed before the just-arrived draw."""
    try:
        rows=recent_rows(900)
        if not rows or str(rows[0]["issue"])!=str(issue):
            return
        state=rows[1:]
        if len(state)<120:
            return
        profile,_=_select_profile(state)
        actual=int(actual_special)
        actual_z=normalize_z(rows[0]["z7"] or "")
        updated=[]

        for strategy in ("20","27"):
            diag=_trend_diagnostics(state,profile,strategy)
            cutoff=20 if strategy=="20" else 27
            ranked=diag["ranked49"]
            rank=(ranked.index(actual)+1) if actual in ranked else 99
            cold_err=actual_z in set(diag["coldest3"])
            head_err=bool(diag["killed_head"]) and head_of(actual)==diag["killed_head"]
            trend_failed=(rank>cutoff) or cold_err or head_err
            if trend_failed:
                severity=1.0
                if rank>27: severity+=.55
                elif rank>20: severity+=.25
                if cold_err: severity+=.20
                if head_err: severity+=.20
                if _update_correction_model(state,actual,strategy,persist=False,sample_weight=severity):
                    updated.append(strategy)

        for strategy in updated:
            save_correction_state(strategy)
        if updated:
            print(f"[CORRECT] issue={issue} trained={','.join(updated)}",flush=True)
    except Exception as e:
        print(f"[CORRECT] online failed: {type(e).__name__}: {e}",flush=True)

def bootstrap_correction_models():
    """Small walk-forward bootstrap; validation numbers are NOT backfilled.

    It only initializes the correction brain from historical trend failures.
    Real forward validation still starts when the version is actually running.
    """
    with correction_lock:
        need=[k for k,v in correction_state.items() if int(v.get("trained",0))==0]
    if not need:
        return
    try:
        rows=recent_rows(320)
        if len(rows)<180:
            return
        # 30 historical walk-forward targets keeps startup cost controlled.
        for k in range(29,-1,-1):
            state=rows[k+1:]
            if len(state)<140:
                continue
            actual=int(rows[k]["special"])
            actual_z=normalize_z(rows[k]["z7"] or "")
            profile="趋势快"
            for strategy in list(need):
                diag=_trend_diagnostics(state,profile,strategy)
                cutoff=20 if strategy=="20" else 27
                ranked=diag["ranked49"]
                rank=(ranked.index(actual)+1) if actual in ranked else 99
                cold_err=actual_z in set(diag["coldest3"])
                head_err=bool(diag["killed_head"]) and head_of(actual)==diag["killed_head"]
                failed=(rank>cutoff or cold_err or head_err)
                if failed:
                    severity=1.0+(.55 if rank>27 else (.25 if rank>20 else 0.0))
                    severity+=.20 if cold_err else 0.0
                    severity+=.20 if head_err else 0.0
                    _update_correction_model(state,actual,strategy,persist=False,sample_weight=severity)
        for strategy in need:
            save_correction_state(strategy)
        print(f"[CORRECT] bootstrap complete status={_correction_status()}",flush=True)
    except Exception as e:
        print(f"[CORRECT] bootstrap failed: {type(e).__name__}: {e}",flush=True)

def train_ai_after_new_draw(issue, nums):
    """After each draw: base AI rolling refit + error-correction AI update."""
    retrain_ai_rolling_100(str(issue))
    train_correction_after_new_draw(str(issue),int(nums[6]))




def load_correction_state():
    with db_lock:
        c=connect()
        try:
            rows=c.execute("SELECT * FROM correction_model").fetchall()
        finally:
            c.close()
    with correction_lock:
        for row in rows:
            strategy=str(row["strategy"])
            if strategy not in correction_state:
                continue
            try:
                w=json.loads(row["weights"] or "[]")
            except Exception:
                w=[]
            if len(w)<len(AI_FEATURES):
                w=list(w)+[0.0]*(len(AI_FEATURES)-len(w))
            elif len(w)>len(AI_FEATURES):
                w=list(w)[:len(AI_FEATURES)]
            correction_state[strategy]={
              "weights":[float(x) for x in w],
              "steps":int(row["steps"] or 0),
              "trained":int(row["trained"] or 0),
              "updated_at":str(row["updated_at"] or "")
            }

def save_correction_state(strategy):
    if strategy not in correction_state:
        return
    with correction_lock:
        st=dict(correction_state[strategy])
    with db_lock:
        c=connect()
        try:
            c.execute("""INSERT INTO correction_model(strategy,weights,steps,trained,updated_at)
                         VALUES (?,?,?,?,CURRENT_TIMESTAMP)
                         ON CONFLICT(strategy) DO UPDATE SET
                           weights=excluded.weights,
                           steps=excluded.steps,
                           trained=excluded.trained,
                           updated_at=CURRENT_TIMESTAMP""",
                      (strategy,json.dumps(st["weights"],separators=(",",":")),
                       int(st["steps"]),int(st["trained"])))
            c.commit()
        finally:
            c.close()

def _correction_probs(r,strategy):
    strategy=str(strategy)
    with correction_lock:
        st=correction_state.get(strategy) or {"weights":[0.0]*len(AI_FEATURES),"trained":0}
        w=list(st["weights"])
        trained=int(st["trained"])
    if trained<=0:
        return {n:1/49.0 for n in range(1,50)}
    X=_ai_feature_matrix(r)
    return _softmax_probs_from_X(X,w)

def _update_correction_model(state_rows, actual_special, strategy, persist=True, sample_weight=1.0):
    """Train ONLY on trend failures, with rank-severity weighting.

    The objective is still to raise the actual special number in the 01-49
    ranking, but a 28+ miss receives more learning pressure than a 21-27 miss.
    """
    strategy=str(strategy)
    if not state_rows or strategy not in correction_state:
        return False
    X=_ai_feature_matrix(state_rows)
    with correction_lock:
        st=correction_state[strategy]
        w=list(st["weights"])
        steps=int(st["steps"])
        trained=int(st["trained"])
    probs=_softmax_probs_from_X(X,w)
    y=int(actual_special)

    expected=[0.0]*len(AI_FEATURES)
    for n,p in probs.items():
        xn=X[n]
        for j,v in enumerate(xn):
            expected[j]+=p*v
    grad=[X[y][j]-expected[j] for j in range(len(AI_FEATURES))]

    base_lr=.072 if strategy=="20" else .058
    lr=base_lr/((1.0+steps/45.0)**0.5)
    lr*=max(.65,min(1.85,float(sample_weight)))
    for j in range(len(w)):
        w[j]=(1.0-0.0007*lr)*w[j]+lr*grad[j]
        w[j]=max(-5.5,min(5.5,w[j]))

    with correction_lock:
        correction_state[strategy]={
          "weights":w,
          "steps":steps+1,
          "trained":trained+1,
          "updated_at":time.strftime("%Y-%m-%d %H:%M:%S")
        }
    if persist:
        save_correction_state(strategy)
    return True

def _correction_status():
    with correction_lock:
        return {
          k:{
            "trained":int(v.get("trained",0)),
            "steps":int(v.get("steps",0)),
            "updated_at":v.get("updated_at","")
          } for k,v in correction_state.items()
        }

def _rate_for_profile(profile, window):
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT hit24
                              FROM prediction_log
                              WHERE profile=? AND settled=1
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT ?""",(profile,int(window))).fetchall()
        finally:
            c.close()
    n=len(rows)
    h=sum(int(x["hit24"] or 0) for x in rows)
    return n,h,(100.0*h/n if n else 0.0)

def refresh_dynamic_ai_mix():
    """Automatically raise/lower AI influence from real locked forward results.

    Range:
      20% minimum
      70% maximum

    It compares AI在线 against the strongest non-AI profile on the same
    rolling history. 60-period performance is the main signal; the last 12
    periods provide a smaller fast-reaction signal.
    """
    profiles=list(PROFILE_LIBRARY.keys())

    ai_n60,ai_h60,ai_raw60=_rate_for_profile("AI在线",60)
    ai_n12,ai_h12,ai_raw12=_rate_for_profile("AI在线",12)

    stat_candidates=[]
    for p in profiles:
        n60,h60,r60=_rate_for_profile(p,60)
        n12,h12,r12=_rate_for_profile(p,12)

        # Bayesian smoothing toward the structural 24/49 coverage baseline.
        base=24/49
        sm60=(h60 + 4*base)/(n60+4) if n60>=0 else base
        sm12=(h12 + 4*base)/(n12+4) if n12>=0 else base
        combined=.72*sm60 + .28*sm12
        stat_candidates.append((combined,p,n60,h60,r60,n12,h12,r12,sm60,sm12))

    best=max(stat_candidates,key=lambda x:(x[0],x[1])) if stat_candidates else (24/49,"平衡",0,0,0,0,0,0,24/49,24/49)
    _combined,best_p,st_n60,st_h60,st_raw60,st_n12,st_h12,st_raw12,st_sm60,st_sm12=best

    base=24/49
    ai_sm60=(ai_h60 + 4*base)/(ai_n60+4) if ai_n60>=0 else base
    ai_sm12=(ai_h12 + 4*base)/(ai_n12+4) if ai_n12>=0 else base

    # 60-period evidence dominates; 12-period result makes it respond faster.
    advantage=.72*(ai_sm60-st_sm60) + .28*(ai_sm12-st_sm12)

    # Confidence grows with real pre-draw AI samples.
    confidence=min(1.0,ai_n60/60.0)

    # Neutral center is 40%. A 10 percentage-point confirmed advantage is worth
    # roughly +18 blend points once the 60-period window is mature.
    mix=40.0 + 180.0*advantage*confidence

    # With very little real data, do not let the value swing wildly.
    if ai_n60 < 5:
        mix=35.0
        reason="实盘不足5期，AI融合固定35%"
    else:
        mix=max(20.0,min(70.0,mix))
        adv_pct=advantage*100
        if adv_pct >= 2.0:
            reason=f"AI近期优于{best_p}，自动提高融合"
        elif adv_pct <= -2.0:
            reason=f"AI近期弱于{best_p}，自动降低融合"
        else:
            reason=f"AI与{best_p}接近，保持中等融合"

    with fusion_lock:
        fusion_cache.update({
            "mix_pct":round(mix,1),
            "ai_rate60":round(ai_raw60,1),
            "stat_rate60":round(st_raw60,1),
            "ai_rate12":round(ai_raw12,1),
            "stat_rate12":round(st_raw12,1),
            "samples":ai_n60,
            "benchmark_profile":best_p,
            "advantage_pct":round(advantage*100,2),
            "reason":reason,
            "updated_at":time.strftime("%Y-%m-%d %H:%M:%S")
        })
    return dict(fusion_cache)

def get_dynamic_ai_mix():
    with fusion_lock:
        cached=dict(fusion_cache)
    # Keep this cheap, but refresh if cache is empty/stale after a new issue.
    if not cached.get("updated_at"):
        return refresh_dynamic_ai_mix()
    return cached

def complement_matrix(window=60, benchmark_profile=None):
    """Measure how AI在线 and the trend benchmark complement each other.

    This uses ONLY settled, pre-draw prediction_log rows.
    Existing historical rows are not rewritten, so the accumulated validation
    remains honest across upgrades.
    """
    fusion=get_dynamic_ai_mix()
    benchmark_profile=benchmark_profile or fusion.get("benchmark_profile") or learner_cache.get("best_profile","趋势快")
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""
                SELECT a.target_issue,
                       a.hit24 AS ai_hit,
                       b.hit24 AS trend_hit
                FROM prediction_log a
                JOIN prediction_log b
                  ON a.target_issue=b.target_issue
                WHERE a.profile='AI在线'
                  AND b.profile=?
                  AND a.settled=1 AND b.settled=1
                ORDER BY CAST(a.target_issue AS INTEGER) DESC
                LIMIT ?
            """,(benchmark_profile,int(window))).fetchall()
            final_rows=c.execute("""
                SELECT hit24
                FROM prediction_log
                WHERE profile='互补在线' AND settled=1
                ORDER BY CAST(target_issue AS INTEGER) DESC
                LIMIT ?
            """,(int(window),)).fetchall()
        finally:
            c.close()

    both=ai_only=trend_only=miss=0
    for x in rows:
        ah=int(x["ai_hit"] or 0)
        th=int(x["trend_hit"] or 0)
        if ah and th: both+=1
        elif ah: ai_only+=1
        elif th: trend_only+=1
        else: miss+=1

    n=len(rows)
    # Smoothed allocation of the 12 "second slots" in 12肖×2码.
    # Neither side may monopolize the complementary seats.
    unique_total=ai_only+trend_only
    ai_share=(ai_only+2.0)/(unique_total+4.0) if unique_total>=0 else 0.5
    ai_share=max(0.35,min(0.65,ai_share))
    if n < 8:
        ai_share=0.50
    ai_slots=max(4,min(8,round(12*ai_share)))
    trend_slots=12-ai_slots

    final_n=len(final_rows)
    final_hits=sum(int(x["hit24"] or 0) for x in final_rows)
    return {
      "n":n,
      "window":int(window),
      "benchmark_profile":benchmark_profile,
      "both_hit":both,
      "ai_only":ai_only,
      "trend_only":trend_only,
      "both_miss":miss,
      "union_hit":both+ai_only+trend_only,
      "union_rate":round(100*(both+ai_only+trend_only)/n,1) if n else 0.0,
      "ai_unique_rate":round(100*ai_only/n,1) if n else 0.0,
      "trend_unique_rate":round(100*trend_only/n,1) if n else 0.0,
      "ai_second_slots":ai_slots,
      "trend_second_slots":trend_slots,
      "ai_side_share":round(ai_share*100,1),
      "trend_side_share":round((1-ai_share)*100,1),
      "final_n":final_n,
      "final_hits":final_hits,
      "final_rate":round(100*final_hits/final_n,1) if final_n else 0.0
    }



def _zodiac_transition_model(r):
    """Independent zodiac transition model for the next SPECIAL zodiac.

    It looks at:
    - latest special zodiac
    - all 7 zodiacs in the latest draw
    - previous special-zodiac context
    - head / wave similarity
    and learns historical state -> next-special-zodiac transitions.
    """
    score=defaultdict(float)
    if len(r)<60:
        return {z:.5 for z in ALL_ZODIACS},{"samples":0}

    cur=r[0]
    cur_z=normalize_z(cur["z7"] or "")
    cur_set=_draw_zodiac_set(cur)
    prev_z=normalize_z(r[1]["z7"] or "") if len(r)>1 else ""
    cur_head=head_of(cur["special"])
    cur_wave=wave_of(cur["special"])
    samples=0

    for j in range(1,min(len(r)-1,650)):
        state=r[j]
        outcome=r[j-1]
        out_z=normalize_z(outcome["z7"] or "")
        if out_z not in ALL_ZODIACS:
            continue
        match=0.0
        if normalize_z(state["z7"] or "")==cur_z:
            match+=2.00
        stset=_draw_zodiac_set(state)
        if cur_set and stset:
            match+=1.35*(len(cur_set & stset)/max(1,len(cur_set | stset)))
        if head_of(state["special"])==cur_head:
            match+=.42
        if wave_of(state["special"])==cur_wave:
            match+=.32
        if j+1<len(r) and normalize_z(r[j+1]["z7"] or "")==prev_z:
            match+=.70
        if match<=0:
            continue
        score[out_z]+=exp_weight(j,220)*match
        samples+=1

    norm=_norm_values(score,ALL_ZODIACS)
    return norm,{"samples":samples}

def _detect_regime(r):
    """Lightweight state switch. It changes weights, not the historical record."""
    if not r:
        return {"name":"平衡","confidence":0.0}
    last12=r[:min(12,len(r))]
    heads=Counter(head_of(x["special"]) for x in last12)
    combos=Counter(wave_parity_of(x["special"]) for x in last12)
    zs=[normalize_z(x["z7"] or "") for x in last12 if x["z7"]]
    zc=Counter(zs)
    n=max(1,len(last12))

    head_share=max(heads.values(),default=0)/n
    combo_share=max(combos.values(),default=0)/n
    unique_z=len(set(zs))
    top_z=max(zc.values(),default=0)/max(1,len(zs))
    rebound=_nmy_cold_rebound_lift(r)

    if rebound.get("active"):
        return {"name":"冷码反弹","confidence":round(min(1.0,.55+max(0,rebound.get("lift",0))*2),2)}
    if head_share>=.50:
        return {"name":"头数集中","confidence":round(head_share,2)}
    if combo_share>=.42:
        return {"name":"波色单双偏态","confidence":round(combo_share,2)}
    if unique_z>=9:
        return {"name":"生肖轮动","confidence":round(unique_z/12,2)}
    if top_z>=.25:
        return {"name":"热肖延续","confidence":round(top_z,2)}
    if head_share<.34 and combo_share<.30 and unique_z>=8:
        return {"name":"高随机","confidence":.65}
    return {"name":"平衡","confidence":.55}

def _norm_values(d, keys):
    vals=[float(d.get(k,0.0)) for k in keys]
    lo=min(vals) if vals else 0.0
    hi=max(vals) if vals else 1.0
    if hi-lo<1e-9:
        return {k:.5 for k in keys}
    return {k:(float(d.get(k,0.0))-lo)/(hi-lo) for k in keys}

def _hot_zodiac_scores(r, profile):
    """Upgraded zodiac layer.

    The coldest-3 decision is no longer just recent hot/cold:
    recent heat + AI zodiac mass + special-zodiac transition
    + 7-position state transition + omission/rebound are combined.
    """
    trend_z=_zodiac_scores_profile(r,profile)
    trend_n=_norm_values(trend_z,ALL_ZODIACS)
    ai_z=_ai_zodiac_mass(r)
    ai_n=_norm_values(ai_z,ALL_ZODIACS)
    trans,trans_meta=_zodiac_transition_model(r)
    _nc,z_cold,_ng,_zg=_cold_metrics(r)
    ctx=_strategy_context(r)

    recent=Counter()
    for i,x in enumerate(r[:24]):
        w=exp_weight(i,8)
        z=normalize_z(x["z7"] or "")
        if z:
            recent[z]+=1.65*w
        # The 7-number zodiac structure matters, but less than special zodiac.
        for k in range(1,7):
            zz=normalize_z(x[f"z{k}"] or "")
            if zz:
                recent[zz]+=.25*w
    recent_n=_norm_values(recent,ALL_ZODIACS)

    regime=_detect_regime(r)
    score={}
    for z in ALL_ZODIACS:
        # A cold zodiac with strong transition support is treated as rebound,
        # not blindly thrown into the coldest three.
        rebound=float(z_cold.get(z,.5))*float(trans.get(z,.5))
        score[z]=(
          .25*trend_n.get(z,.5)
          +.18*ai_n.get(z,.5)
          +.28*trans.get(z,.5)
          +.15*recent_n.get(z,.5)
          +.09*rebound
          +.05*(1.0-float(z_cold.get(z,.5)))
        )
        if ctx.get("cold_rebound_now"):
            score[z]+=.05*rebound
        if regime["name"]=="生肖轮动":
            score[z]+=.04*trans.get(z,.5)
        elif regime["name"]=="热肖延续":
            score[z]+=.03*recent_n.get(z,.5)

    return score



def _zodiac_mass_from_number_probs(probs,r):
    zmap=_number_zodiac_map(r)
    mass={z:0.0 for z in ALL_ZODIACS}
    for n,p in probs.items():
        z=zmap.get(int(n))
        if z in mass:
            mass[z]+=float(p)
    return mass

def _final_coldest3(r, profile, strategy, zheat):
    """Choose the FINAL coldest 3 after a reversal-rescue check.

    A zodiac is not rescued merely because it is cold. Rescue requires
    independent forward support from transition + correction AI / 7-position
    structure. The final result still excludes exactly 3 zodiacs.
    """
    base_rank=sorted(ALL_ZODIACS,key=lambda z:(zheat.get(z,0.0),z))
    raw_cold=base_rank[:3]

    trans,_tm=_zodiac_transition_model(r)
    corr=_correction_probs(r,strategy)
    corr_z=_zodiac_mass_from_number_probs(corr,r)
    corr_n=_norm_values(corr_z,ALL_ZODIACS)

    # Current 7-position structure: zodiacs absent recently get no free rescue;
    # zodiacs with transition support and recent structural presence gain support.
    presence=Counter()
    for i,x in enumerate(r[:12]):
        w=exp_weight(i,4)
        for z in _draw_zodiac_set(x):
            presence[z]+=w
    presence_n=_norm_values(presence,ALL_ZODIACS)

    rescue_score={
      z:.50*trans.get(z,.5)+.32*corr_n.get(z,.5)+.18*presence_n.get(z,.5)
      for z in ALL_ZODIACS
    }
    trans_top=set(sorted(ALL_ZODIACS,key=lambda z:(-trans.get(z,0),z))[:3])
    corr_top=set(sorted(ALL_ZODIACS,key=lambda z:(-corr_n.get(z,0),z))[:4])

    rescued=[]
    for z in raw_cold:
        # Need at least two independent reasons, not just one hot-looking feature.
        reasons=int(z in trans_top)+int(z in corr_top)+int(presence_n.get(z,.5)>=.62)
        if reasons>=2 and rescue_score.get(z,0)>=.60:
            rescued.append(z)

    # Re-rank coldness after applying only a modest rescue bump.
    adjusted=dict(zheat)
    for z in rescued:
        adjusted[z]+=0.18+0.10*rescue_score.get(z,.5)

    final3=sorted(ALL_ZODIACS,key=lambda z:(adjusted.get(z,0.0),z))[:3]
    # Exactly three are always excluded; rescued zodiacs can still remain if
    # they are overwhelmingly the weakest after the rescue test.
    return final3,{
      "raw_coldest3":raw_cold,
      "rescued_zodiacs":[z for z in rescued if z not in final3],
      "rescue_scores":{z:round(rescue_score.get(z,0),3) for z in raw_cold},
      "final_coldest3":final3
    }

def _ai_head_strength(r):
    with ai_lock:
        ready=bool(ai_state.get("ready",False))
    if not ready:
        return {h:1.0 for h in HEAD_BASE}
    _X,_lg,probs=_ai_logits_and_probs(r)
    mass={h:0.0 for h in HEAD_BASE}
    for n,p in probs.items():
        mass[head_of(n)]+=float(p)
    return {h:mass[h]/max(HEAD_BASE[h],1e-9) for h in HEAD_BASE}

def _combined_04_head_decision(r, force=False):
    """AI + trend jointly choose the weaker of 0头/4头."""
    trend_raw=_head_trend(r).get("strength",{})
    ai_raw=_ai_head_strength(r)

    tn=_norm_values(trend_raw,list(HEAD_BASE))
    an=_norm_values(ai_raw,list(HEAD_BASE))
    combined={h:.58*tn.get(h,.5)+.42*an.get(h,.5) for h in HEAD_BASE}

    h0=combined["0头"]; h4=combined["4头"]
    killed="0头" if h0<=h4 else "4头"
    spread=abs(h0-h4)

    # 27码 always kills one. 20码 only kills when one side is meaningfully weaker
    # or the old head-trend engine already flagged a weak 0/4 head.
    active = force or spread>=.08 or bool(_head_trend(r).get("active_04"))
    return {
      "killed_head":killed if active else "",
      "forced":bool(force),
      "spread":round(spread,3),
      "combined":{"0头":round(h0,3),"4头":round(h4,3)},
      "trend":{"0头":round(float(trend_raw.get("0头",0)),3),"4头":round(float(trend_raw.get("4头",0)),3)},
      "ai":{"0头":round(float(ai_raw.get("0头",0)),3),"4头":round(float(ai_raw.get("4头",0)),3)}
    }


def _head_kill_history(profile="27码十期",window=40):
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT killed_head,head_kill_success
                              FROM strategy_audit
                              WHERE profile=? AND settled=1
                                AND killed_head IN ('0头','4头')
                                AND head_kill_success IS NOT NULL
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT ?""",(profile,int(window))).fetchall()
        finally:
            c.close()
    stats={}
    for h in ("0头","4头"):
        vals=[int(x["head_kill_success"] or 0) for x in rows if str(x["killed_head"])==h]
        n=len(vals); hits=sum(vals)
        # Beta(2,2) smoothing avoids a tiny sample dominating a ten-period block.
        rate=(hits+2)/(n+4)
        stats[h]={"n":n,"hits":hits,"rate":rate}
    return stats

def _correction_head_strength(r,strategy="27"):
    probs=_correction_probs(r,strategy)
    mass={h:0.0 for h in HEAD_BASE}
    for n,p in probs.items():
        mass[head_of(int(n))]+=float(p)
    # Normalize for different theoretical head sizes.
    return {h:mass[h]/max(HEAD_BASE[h],1e-9) for h in HEAD_BASE}

def _vote_04_head_decision(r, force=True, profile="27码十期"):
    """Three-way head decision.

    Vote 1: trend head model
    Vote 2: correction AI
    Vote 3: actual historical kill success for killing 0 vs 4
    """
    trend_raw=_head_trend(r).get("strength",{})
    corr_raw=_correction_head_strength(r,"27")
    hist=_head_kill_history(profile,40)

    trend_vote="0头" if float(trend_raw.get("0头",1)) <= float(trend_raw.get("4头",1)) else "4头"
    corr_vote="0头" if float(corr_raw.get("0头",1)) <= float(corr_raw.get("4头",1)) else "4头"
    hist_vote="0头" if hist["0头"]["rate"] >= hist["4头"]["rate"] else "4头"

    votes=Counter([trend_vote,corr_vote,hist_vote])
    if votes["0头"]!=votes["4头"]:
        killed="0头" if votes["0头"]>votes["4头"] else "4头"
    else:
        # Theoretically unreachable with 3 votes, but keep deterministic fallback.
        killed=trend_vote

    # Confidence is consensus + historical separation.
    hist_gap=abs(hist["0头"]["rate"]-hist["4头"]["rate"])
    confidence=(max(votes.values())/3.0)*.72+min(.28,hist_gap)
    return {
      "killed_head":killed if force else (killed if confidence>=.58 else ""),
      "forced":bool(force),
      "method":"三票制",
      "votes":{"趋势":trend_vote,"纠错AI":corr_vote,"历史成功率":hist_vote},
      "vote_count":{"0头":votes["0头"],"4头":votes["4头"]},
      "confidence":round(confidence,3),
      "history":{
        h:{"n":hist[h]["n"],"hits":hist[h]["hits"],"rate":round(hist[h]["rate"]*100,1)}
        for h in ("0头","4头")
      }
    }

def _trend_only_number_scores(r, profile, strategy="20"):
    """Trend-side score without general AI or correction AI."""
    zmap=_number_zodiac_map(r)
    ctx=_strategy_context(r)
    raw,_meta,_zt=_predictive_number_scores(r,profile,ctx)
    trend_n=_norm_values(raw,range(1,50))

    pair=_within_zodiac_pair_bonus(r,zmap)
    pair_n={}
    for z in ALL_ZODIACS:
        pool=[n for n in range(1,50) if zmap.get(n)==z]
        pair_n.update(_norm_pool(pair,pool))

    trend=_trend_profiles(r)
    combo_n=_norm_values(trend.get("wave_parity",{}),WAVE_PARITY_KEYS)
    zheat=_hot_zodiac_scores(r,profile)
    zheat_n=_norm_values(zheat,ALL_ZODIACS)
    regime=_detect_regime(r)

    if str(strategy)=="27":
        weights={"trend":.68,"pair":.10,"combo":.08,"zodiac":.14}
    else:
        weights={"trend":.62,"pair":.11,"combo":.11,"zodiac":.16}

    if regime["name"]=="生肖轮动":
        weights["zodiac"]+=.05; weights["trend"]-=.05
    elif regime["name"]=="波色单双偏态":
        weights["combo"]+=.05; weights["trend"]-=.05
    elif regime["name"]=="头数集中":
        weights["trend"]+=.04; weights["zodiac"]-=.04

    score={}
    for n in range(1,50):
        z=zmap.get(n)
        score[n]=(
          weights["trend"]*trend_n.get(n,.5)
          +weights["pair"]*pair_n.get(n,.5)
          +weights["combo"]*combo_n.get(wave_parity_of(n),.5)
          +weights["zodiac"]*zheat_n.get(z,.5)
        )
    return score,zmap,zheat,ctx,regime


def _specialist_model_scores(r,profile,strategy="20"):
    """Five deliberately different model personalities.

    T = structural trend
    Z = zodiac transition / 7-position structure
    C = cold-rebound specialist
    W = wave x parity specialist
    A = correction AI

    They all output a 01-49 ranking so they can be compared on the same target.
    """
    nums=list(range(1,50))
    zmap=_number_zodiac_map(r)

    # T: pure-ish trend structure.
    t_raw,_zm,_zh,_ctx,_reg=_trend_only_number_scores(r,profile,strategy)
    T=_norm_values(t_raw,nums)

    # Z: zodiac transition is dominant; number-within-zodiac structure is secondary.
    zheat=_hot_zodiac_scores(r,profile)
    zheat_n=_norm_values(zheat,ALL_ZODIACS)
    ztrans,_ztm=_zodiac_transition_model(r)
    pair=_within_zodiac_pair_bonus(r,zmap)
    pair_n={}
    for z in ALL_ZODIACS:
        pool=[n for n in nums if zmap.get(n)==z]
        pair_n.update(_norm_pool(pair,pool))
    Z={
      n:.46*ztrans.get(zmap.get(n),.5)
        +.34*zheat_n.get(zmap.get(n),.5)
        +.20*pair_n.get(n,.5)
      for n in nums
    }
    Z=_norm_values(Z,nums)

    # C: intentionally different from T/Z. During cold-rebound states it prefers
    # overdue numbers; otherwise it prefers medium-cold rather than the extremes.
    num_cold,z_cold,num_gap,_zg=_cold_metrics(r)
    rebound=_nmy_cold_rebound_lift(r)
    gaps=_norm_values(num_gap,nums)
    C={}
    for n in nums:
        cold=float(num_cold.get(n,.5))
        zc=float(z_cold.get(zmap.get(n),.5))
        if rebound.get("active"):
            C[n]=.52*cold+.25*zc+.23*gaps.get(n,.5)
        else:
            mid=max(0.0,1.0-abs(cold-.58)*1.9)
            C[n]=.50*mid+.25*(1.0-abs(zc-.55))+.25*gaps.get(n,.5)
    C=_norm_values(C,nums)

    # W: red/blue/green x odd/even is the main signal, with wave/size/parity support.
    tr=_trend_profiles(r)
    wp=_norm_values(tr.get("wave_parity",{}),WAVE_PARITY_KEYS)
    wv=_norm_values(tr.get("wave",{}),["红","蓝","绿"])
    sz=_norm_values(tr.get("size",{}),["大","小"])
    pa=_norm_values(tr.get("parity",{}),["单","双"])
    W={
      n:.56*wp.get(wave_parity_of(n),.5)
        +.18*wv.get(wave_of(n),.5)
        +.13*sz.get(size_of(n),.5)
        +.13*pa.get(parity_of(n),.5)
      for n in nums
    }
    W=_norm_values(W,nums)

    # A: correction AI is dominant; base AI prevents tiny correction samples
    # from becoming too erratic.
    corr=_norm_values(_correction_probs(r,strategy),nums)
    with ai_lock:
        ai_ready=bool(ai_state.get("ready",False))
    if ai_ready:
        _X,_lg,p=_ai_logits_and_probs(r)
        base=_normalize_ai_probs(p)
    else:
        base={n:.5 for n in nums}
    A={n:.72*corr.get(n,.5)+.28*base.get(n,.5) for n in nums}
    A=_norm_values(A,nums)

    return {"T":T,"Z":Z,"C":C,"W":W,"A":A}

def _rows_rate(rows,k):
    part=rows[:int(k)]
    n=len(part)
    h=sum(int(x["hit24"] or 0) for x in part)
    return n,h,(h/n if n else 0.0)

def _streak_from_rows(rows):
    """Rows must be newest first."""
    current=0
    for x in rows:
        if int(x["hit24"] or 0)==1:
            current+=1
        else:
            break
    asc=list(reversed(rows))
    best=run=0
    for x in asc:
        if int(x["hit24"] or 0)==1:
            run+=1
            best=max(best,run)
        else:
            run=0
    return current,best

def _normalize_capped_weights(raw,lo=.10,hi=.35):
    keys=list(raw)
    if not keys:
        return {}
    total=sum(max(0.0,float(raw[k])) for k in keys) or 1.0
    w={k:max(0.0,float(raw[k]))/total for k in keys}
    # repeated clamp / redistribute
    for _ in range(8):
        low=[k for k in keys if w[k]<lo]
        high=[k for k in keys if w[k]>hi]
        if not low and not high:
            break
        fixed={}
        for k in low: fixed[k]=lo
        for k in high: fixed[k]=hi
        free=[k for k in keys if k not in fixed]
        remain=max(0.0,1.0-sum(fixed.values()))
        free_total=sum(w[k] for k in free) or 1.0
        nw=dict(fixed)
        for k in free:
            nw[k]=remain*w[k]/free_total
        w=nw
    total=sum(w.values()) or 1.0
    return {k:w[k]/total for k in keys}


def _current_miss_streak(rows):
    """Rows newest -> oldest."""
    run=0
    for x in rows:
        if int(x["hit24"] or 0)==0:
            run+=1
        else:
            break
    return run

def _profile_error_stats(profile,limit=60):
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT target_issue,hit24,actual_special,actual_zodiac
                              FROM prediction_log
                              WHERE profile=? AND settled=1
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT ?""",(profile,int(limit))).fetchall()
        finally:
            c.close()
    def window(k):
        p=rows[:k]
        n=len(p)
        misses=sum(1 for x in p if int(x["hit24"] or 0)==0)
        return {"n":n,"misses":misses,"rate":round(100*misses/n,1) if n else 0.0}
    return {
      "10":window(10),
      "30":window(30),
      "60":window(60),
      "miss_streak":_current_miss_streak(rows)
    }

def _legacy_ai_trend_errors():
    fusion=get_dynamic_ai_mix()
    trend_profile=fusion.get("benchmark_profile") or learner_cache.get("best_profile","趋势快")
    return {
      "AI":_profile_error_stats("AI在线",60),
      "Trend":_profile_error_stats(trend_profile,60),
      "trend_profile":trend_profile
    }

def _miss_pattern_score_for_profile(r,profile,limit=60):
    """Learn a SMALL blind-spot correction from the categories of real misses.

    This is not a new prediction model. It only detects repeated blind spots
    (zodiac/head/wave-parity/number) in locked pre-draw misses and produces a
    capped rescue score for F=20.
    """
    zmap=_number_zodiac_map(r)
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT target_issue,hit24,actual_special,actual_zodiac
                              FROM prediction_log
                              WHERE profile=? AND settled=1
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT ?""",(profile,int(limit))).fetchall()
        finally:
            c.close()

    misses=[x for x in rows if int(x["hit24"] or 0)==0 and x["actual_special"] is not None]
    if len(misses)<5:
        return {n:.5 for n in range(1,50)},{"n":len(misses),"ready":False}

    zc=Counter(); hc=Counter(); wc=Counter(); nc=Counter()
    # newer mistakes count more, but old mistakes still matter
    for i,x in enumerate(misses):
        n=int(x["actual_special"])
        z=normalize_z(x["actual_zodiac"] or "") or zmap.get(n)
        w=exp_weight(i,22)
        if z: zc[z]+=w
        hc[head_of(n)]+=w
        wc[wave_parity_of(n)]+=w
        nc[n]+=w

    zn=_norm_values(zc,ALL_ZODIACS)
    hn=_norm_values(hc,list(HEAD_BASE))
    wn=_norm_values(wc,WAVE_PARITY_KEYS)
    nn=_norm_values(nc,range(1,50))

    score={}
    for n in range(1,50):
        score[n]=(
          .42*zn.get(zmap.get(n),.5)
          +.25*hn.get(head_of(n),.5)
          +.23*wn.get(wave_parity_of(n),.5)
          +.10*nn.get(n,.5)
        )
    return _norm_values(score,range(1,50)),{
      "n":len(misses),
      "ready":True,
      "top_zodiacs":[x[0] for x in zc.most_common(3)],
      "top_heads":[x[0] for x in hc.most_common(2)],
      "top_wave_parity":[x[0] for x in wc.most_common(3)]
    }

def _pool_error_rescue_score(r,profile):
    """Combine T/Z/C/W/A + legacy AI/trend blind spots for F=20 only."""
    perf=_pool_performance()
    pweights=perf.get("weights") or {k:.20 for k in POOL_MODEL_PROFILES}
    pieces=[]
    meta={"models":{}}

    for key,prof in POOL_MODEL_PROFILES.items():
        sc,mm=_miss_pattern_score_for_profile(r,prof,60)
        if mm.get("ready"):
            pieces.append((float(pweights.get(key,.20)),sc))
        meta["models"][key]=mm

    # Legacy AI and best trend are included with smaller influence because
    # their candidate-list sizes differ from the 20-code pool.
    legacy=_legacy_ai_trend_errors()
    trend_prof=legacy.get("trend_profile") or profile
    for key,prof,w in (("AI","AI在线",.10),("Trend",trend_prof,.10)):
        sc,mm=_miss_pattern_score_for_profile(r,prof,60)
        if mm.get("ready"):
            pieces.append((w,sc))
        meta["models"][key]=mm

    if not pieces:
        return {n:.5 for n in range(1,50)},dict(meta,ready=False,total_misses=0)

    total=sum(w for w,_ in pieces) or 1.0
    rescue={
      n:sum(w*sc.get(n,.5) for w,sc in pieces)/total
      for n in range(1,50)
    }
    rescue=_norm_values(rescue,range(1,50))
    total_misses=sum(int((m or {}).get("n",0)) for m in meta["models"].values())
    meta["ready"]=True
    meta["total_misses"]=total_misses
    return rescue,meta

def _pool_performance(force=False):
    """Real pre-draw results determine model weights.

    10/30/60 windows = 30% / 35% / 35%, with Bayesian shrinkage.
    """
    now=time.time()
    with pool_perf_lock:
        cached=pool_perf_cache.get("data")
        if cached is not None and not force and now-float(pool_perf_cache.get("ts",0))<8:
            return cached

    baseline=20/49
    stats={}
    for key,prof in POOL_MODEL_PROFILES.items():
        with db_lock:
            c=connect()
            try:
                rows=c.execute("""SELECT target_issue,hit24
                                  FROM prediction_log
                                  WHERE profile=? AND settled=1
                                  ORDER BY CAST(target_issue AS INTEGER) DESC
                                  LIMIT 60""",(prof,)).fetchall()
            finally:
                c.close()
        n10,h10,r10=_rows_rate(rows,10)
        n30,h30,r30=_rows_rate(rows,30)
        n60,h60,r60=_rows_rate(rows,60)
        def smooth(h,n):
            return (h+6*baseline)/(n+6)
        composite=.30*smooth(h10,n10)+.35*smooth(h30,n30)+.35*smooth(h60,n60)
        cur,best=_streak_from_rows(rows)
        miss_streak=_current_miss_streak(rows)
        # Small penalty only: a short losing streak should adapt weight,
        # but must not make the model disappear because random streaks happen.
        adjusted=max(0.0,composite-.008*min(4,miss_streak))
        stats[key]={
          "profile":prof,
          "n10":n10,"h10":h10,"r10":round(100*r10,1) if n10 else 0.0,
          "e10":max(0,n10-h10),
          "n30":n30,"h30":h30,"r30":round(100*r30,1) if n30 else 0.0,
          "e30":max(0,n30-h30),
          "n60":n60,"h60":h60,"r60":round(100*r60,1) if n60 else 0.0,
          "e60":max(0,n60-h60),
          "current_streak":cur,"max_streak":best,
          "miss_streak":miss_streak,
          "composite":adjusted
        }

    mature=max((v["n60"] for v in stats.values()),default=0)
    confidence=min(1.0,mature/30.0)
    raw={k:math.exp(8.0*(v["composite"]-baseline)) for k,v in stats.items()}
    perf_total=sum(raw.values()) or 1.0
    perf={k:raw[k]/perf_total for k in raw}
    equal=1.0/max(1,len(perf))
    blended={k:(1-confidence)*equal+confidence*perf[k] for k in perf}
    weights=_normalize_capped_weights(blended,.10,.35)
    for k in stats:
        stats[k]["weight_pct"]=round(100*weights.get(k,equal),1)

    data={"stats":stats,"weights":weights,"mature_n":mature}
    with pool_perf_lock:
        pool_perf_cache["ts"]=now
        pool_perf_cache["data"]=data
    return data

def _pool_ensemble_score(r,profile,strategy="20"):
    models=_specialist_model_scores(r,profile,strategy)
    perf=_pool_performance()
    weights=perf.get("weights") or {k:.20 for k in models}
    ens={}
    for n in range(1,50):
        ens[n]=sum(float(weights.get(k,.20))*float(models[k].get(n,.5)) for k in models)
    return _norm_values(ens,range(1,50)),models,perf

def _stable_signal_predictions(r,profile):
    """High-coverage trend signals, all locked BEFORE the draw."""
    tr=_trend_profiles(r)
    zmap=_number_zodiac_map(r)
    zheat=_hot_zodiac_scores(r,profile)

    top2w=sorted(["红","蓝","绿"],key=lambda x:(-tr["wave"].get(x,0),x))[:2]
    top7z=sorted(ALL_ZODIACS,key=lambda z:(-zheat.get(z,0),z))[:7]
    best_size=max(["大","小"],key=lambda x:(tr["size"].get(x,0),x))
    best_parity=max(["单","双"],key=lambda x:(tr["parity"].get(x,0),x))
    top3wp=sorted(WAVE_PARITY_KEYS,key=lambda x:(-tr["wave_parity"].get(x,0),x))[:3]
    head=_vote_04_head_decision(r,force=True,profile="27码十期")
    killed=head.get("killed_head","")

    return {
      "稳双波":[n for n in range(1,50) if wave_of(n) in set(top2w)],
      "稳7肖":[n for n in range(1,50) if zmap.get(n) in set(top7z)],
      "稳大小":[n for n in range(1,50) if size_of(n)==best_size],
      "稳单双":[n for n in range(1,50) if parity_of(n)==best_parity],
      "稳波单双3":[n for n in range(1,50) if wave_parity_of(n) in set(top3wp)],
      "稳0/4杀头":[n for n in range(1,50) if (not killed or head_of(n)!=killed)],
    }

def _generic_profile_stats(profile,limit=120):
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT target_issue,hit24,actual_special
                              FROM prediction_log
                              WHERE profile=? AND settled=1
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT ?""",(profile,int(limit))).fetchall()
        finally:
            c.close()
    n10,h10,r10=_rows_rate(rows,10)
    n30,h30,r30=_rows_rate(rows,30)
    n60,h60,r60=_rows_rate(rows,60)
    cur,best=_streak_from_rows(rows)
    return {
      "n10":n10,"h10":h10,"r10":round(100*r10,1) if n10 else 0.0,
      "n30":n30,"h30":h30,"r30":round(100*r30,1) if n30 else 0.0,
      "n60":n60,"h60":h60,"r60":round(100*r60,1) if n60 else 0.0,
      "current_streak":cur,"max_streak":best
    }

def _pool_dashboard():
    perf=_pool_performance()
    stats=perf.get("stats",{})

    # Unique value: the only specialist that hit that issue.
    profiles=list(POOL_MODEL_PROFILES.values())
    placeholders=",".join("?" for _ in profiles)
    with db_lock:
        c=connect()
        try:
            rows=c.execute(f"""SELECT target_issue,profile,hit24,actual_special
                               FROM prediction_log
                               WHERE profile IN ({placeholders}) AND settled=1
                               ORDER BY CAST(target_issue AS INTEGER) DESC
                               LIMIT 360""",tuple(profiles)).fetchall()
            frows=c.execute("""SELECT target_issue,hit24,actual_special
                               FROM prediction_log
                               WHERE profile=? AND settled=1
                               ORDER BY CAST(target_issue AS INTEGER) DESC
                               LIMIT 60""",(POOL_FINAL_PROFILE,)).fetchall()
        finally:
            c.close()

    by_issue={}
    prof_to_key={v:k for k,v in POOL_MODEL_PROFILES.items()}
    for x in rows:
        d=by_issue.setdefault(str(x["target_issue"]),{"actual":x["actual_special"],"hits":{}})
        k=prof_to_key.get(str(x["profile"]))
        if k:
            d["hits"][k]=int(x["hit24"] or 0)

    unique={k:0 for k in POOL_MODEL_PROFILES}
    for issue in sorted(by_issue,key=lambda x:int(x),reverse=True)[:60]:
        h=[k for k,v in by_issue[issue]["hits"].items() if v]
        if len(h)==1:
            unique[h[0]]+=1
    for k in stats:
        stats[k]["unique_hits60"]=unique.get(k,0)

    f_by_issue={str(x["target_issue"]):int(x["hit24"] or 0) for x in frows}
    recent=[]
    for issue in sorted(by_issue,key=lambda x:int(x),reverse=True)[:10]:
        d=by_issue[issue]
        recent.append({
          "issue":issue,"actual":d.get("actual"),
          "T":d["hits"].get("T"),"Z":d["hits"].get("Z"),
          "C":d["hits"].get("C"),"W":d["hits"].get("W"),
          "A":d["hits"].get("A"),"F":f_by_issue.get(issue)
        })
    return {
      "stats":stats,
      "recent10":recent,
      "mature_n":perf.get("mature_n",0),
      "legacy_errors":_legacy_ai_trend_errors()
    }

def _stable_dashboard():
    items={}
    warnings=[]
    for name,prof in STABLE_SIGNAL_PROFILES.items():
        st=_generic_profile_stats(prof,120)
        items[name]=st
        if st["current_streak"]>=5:
            warnings.append({"name":name,"streak":st["current_streak"]})
    warnings=sorted(warnings,key=lambda x:(-x["streak"],x["name"]))
    return {"items":items,"warnings":warnings}


def _pool_04_weakness_decision(r,profile,strategy="27"):
    """Use T/Z/C/W/A + their real-performance weights to compare 0头 vs 4头.

    v34 does NOT force a head kill.
    A hard exclusion is activated only when the multi-model weakness gap is
    meaningful; otherwise both heads remain eligible.
    """
    models=_specialist_model_scores(r,profile,strategy)
    perf=_pool_performance()
    weights=perf.get("weights") or {k:.20 for k in models}

    def avg_for_head(score_map,h):
        nums=[n for n in range(1,50) if head_of(n)==h]
        return sum(float(score_map.get(n,.5)) for n in nums)/max(1,len(nums))

    per_model={}
    weighted={"0头":0.0,"4头":0.0}
    votes={"0头":0,"4头":0}
    for key,score_map in models.items():
        a0=avg_for_head(score_map,"0头")
        a4=avg_for_head(score_map,"4头")
        weak="0头" if a0<=a4 else "4头"
        votes[weak]+=1
        per_model[key]={
          "0头":round(a0,3),"4头":round(a4,3),"weak":weak,
          "weight_pct":round(100*float(weights.get(key,.20)),1)
        }
        weighted["0头"]+=float(weights.get(key,.20))*a0
        weighted["4头"]+=float(weights.get(key,.20))*a4

    weaker="0头" if weighted["0头"]<=weighted["4头"] else "4头"
    stronger="4头" if weaker=="0头" else "0头"
    gap=weighted[stronger]-weighted[weaker]
    vote_support=votes[weaker]/5.0

    # Mature real model-pool evidence can lower the activation threshold slightly.
    mature=int(perf.get("mature_n",0))
    threshold=.105 if mature<10 else (.090 if mature<30 else .080)
    active=(gap>=threshold and vote_support>=.60)

    return {
      "active":bool(active),
      "killed_head":weaker if active else "",
      "weak_head":weaker,
      "gap":round(gap,3),
      "threshold":round(threshold,3),
      "vote_support_pct":round(100*vote_support,1),
      "weighted":{"0头":round(weighted["0头"],3),"4头":round(weighted["4头"],3)},
      "votes":votes,
      "models":per_model,
      "reason":(
        f"多策略确认{weaker}偏弱，启用排除"
        if active else
        f"{weaker}略弱但证据不足，不强杀"
      )
    }

def _round10_info(issue):
    try:
        x=int(issue)
        start=(x//10)*10
        pos=x-start+1
        return {
          "start":str(start),"end":str(start+9),
          "position":max(1,min(10,pos))
        }
    except Exception:
        return {"start":"","end":"","position":0}

def _selection_number_scores(r, profile, strategy="20"):
    """Trend is the main model; AI has two smaller jobs.

    1) general AI supplies nonlinear context;
    2) correction AI is trained ONLY when trend misses.
    This reduces AI/trend homogenization.
    """
    trend_score,zmap,zheat,ctx,regime=_trend_only_number_scores(r,profile,strategy)
    trend_n=_norm_values(trend_score,range(1,50))

    with ai_lock:
        ai_ready=bool(ai_state.get("ready",False))
    if ai_ready:
        _X,_lg,base_probs=_ai_logits_and_probs(r)
        base_ai=_normalize_ai_probs(base_probs)
    else:
        base_ai={n:.5 for n in range(1,50)}

    corr_probs=_correction_probs(r,strategy)
    corr_n=_norm_values(corr_probs,range(1,50))

    with correction_lock:
        corr_trained=int(correction_state.get(str(strategy),{}).get("trained",0))

    # Strategy-specific separation:
    # 20码 = more responsive correction; 27码 = more stable trend.
    if str(strategy)=="27":
        base_ai_w=.09
        corr_w=min(.17,.06+.11*min(1.0,corr_trained/45.0))
    else:
        base_ai_w=.11
        corr_w=min(.23,.07+.16*min(1.0,corr_trained/45.0))

    if regime["name"]=="高随机":
        corr_w=min(corr_w+.03,.25 if str(strategy)=="20" else .19)
    trend_w=1.0-base_ai_w-corr_w

    score={}
    for n in range(1,50):
        t=float(trend_n.get(n,.5))
        a=float(base_ai.get(n,.5))
        c=float(corr_n.get(n,.5))
        # Small consensus bonus, but disagreement is no longer rewarded by itself.
        consensus=1.0-abs(t-a)
        score[n]=trend_w*t+base_ai_w*a+corr_w*c+.035*consensus

    # v33: add a genuine multi-model pool. It starts cautiously and becomes
    # more important only after enough locked real samples exist.
    pool_ens,_pool_models,pool_perf=_pool_ensemble_score(r,profile,strategy)
    base_norm=_norm_values(score,range(1,50))
    mature=int(pool_perf.get("mature_n",0))
    pool_mix=.20+.30*min(1.0,mature/30.0)
    score={
      n:(1.0-pool_mix)*base_norm.get(n,.5)+pool_mix*pool_ens.get(n,.5)
      for n in range(1,50)
    }

    # v35: only F=20 receives the blind-spot self-correction layer.
    error_mix=0.0
    error_meta={"ready":False,"total_misses":0}
    if str(strategy)=="20":
        rescue,error_meta=_pool_error_rescue_score(r,profile)
        mature_err=min(1.0,float(error_meta.get("total_misses",0))/45.0)
        error_mix=.05+.07*mature_err if error_meta.get("ready") else 0.0
        score={
          n:(1.0-error_mix)*score.get(n,.5)+error_mix*rescue.get(n,.5)
          for n in range(1,50)
        }

    ctx=dict(ctx)
    ctx["regime"]=regime
    ctx["correction_trained"]=corr_trained
    ctx["correction_weight_pct"]=round(corr_w*100,1)
    ctx["trend_weight_pct"]=round(trend_w*100,1)
    ctx["base_ai_weight_pct"]=round(base_ai_w*100,1)
    ctx["pool_mix_pct"]=round(pool_mix*100,1)
    ctx["pool_weights_pct"]={
      k:round(100*v,1) for k,v in (pool_perf.get("weights") or {}).items()
    }
    ctx["error_rescue_pct"]=round(error_mix*100,1)
    ctx["error_rescue_meta"]=error_meta
    return score,zmap,zheat,ctx

def _trend_diagnostics(r,profile,strategy="20"):
    """Pre-draw trend-only diagnostic used for training the correction AI."""
    score,zmap,zheat,ctx,regime=_trend_only_number_scores(r,profile,strategy)
    ranked=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
    ranked_z=sorted(ALL_ZODIACS,key=lambda z:(-zheat.get(z,-1e9),z))
    coldest3=ranked_z[-3:]
    if str(strategy)=="27":
        head=_vote_04_head_decision(r,force=True,profile="27码十期")
        cutoff=27
    else:
        head=_combined_04_head_decision(r,force=False)
        cutoff=20
    return {
      "ranked49":ranked,
      "top":ranked[:cutoff],
      "coldest3":coldest3,
      "killed_head":head.get("killed_head",""),
      "regime":regime.get("name","平衡"),
      "zmap":zmap
    }



def _recent_f_code_counts(limit=6):
    """Recent pre-draw F list sizes, newest first."""
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT special24 FROM prediction_log
                              WHERE profile=?
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT ?""",(POOL_FINAL_PROFILE,int(limit))).fetchall()
        finally:
            c.close()
    out=[]
    for row in rows:
        nums=_csv_nums(row["special24"])
        if nums:
            out.append(len(nums))
    return out

def _f_consensus_map(r,profile):
    """Per-number support from T/Z/C/W/A.

    support = number of specialist top-20 lists containing the number.
    weighted_support = current real-performance weight supporting the number.
    """
    models=_specialist_model_scores(r,profile,"20")
    perf=_pool_performance()
    weights=perf.get("weights") or {k:.20 for k in models}
    ranks={}
    for key,sm in models.items():
        order=sorted(range(1,50),key=lambda n:(-sm.get(n,-1e9),n))
        ranks[key]={n:i+1 for i,n in enumerate(order)}

    support={}
    weighted={}
    top15={}
    for n in range(1,50):
        support[n]=sum(1 for k in models if ranks[k].get(n,99)<=20)
        top15[n]=sum(1 for k in models if ranks[k].get(n,99)<=15)
        weighted[n]=sum(float(weights.get(k,.20)) for k in models if ranks[k].get(n,99)<=20)
    return models,perf,ranks,support,weighted,top15

def _choose_f_dynamic_count(ranked,final_score,pool_score,support,weighted,protected3):
    """Choose 19-23 codes. 20 is the default.

    23 is deliberately a rare insurance state:
    - all 21/22/23 edge candidates must be strong,
    - at least two of them need 3+ model support,
    - the score band must be tight,
    - and 23 cannot be used repeatedly.
    """
    target=20
    reasons=["默认20码"]

    if len(ranked)<23:
        return min(20,len(ranked)),reasons,[]

    s19=final_score.get(ranked[18],0.0)
    s20=final_score.get(ranked[19],0.0)
    edge=[]
    for idx in (20,21,22):
        n=ranked[idx]
        gap=max(0.0,s20-final_score.get(n,0.0))
        qualifies=(
          support.get(n,0)>=3
          or (
            support.get(n,0)>=2
            and weighted.get(n,0)>=.38
            and gap<=.075
          )
          or (
            support.get(n,0)>=2
            and pool_score.get(n,0)>=.80
            and gap<=.055
          )
        )
        edge.append({
          "code":n,"rank":idx+1,
          "support":support.get(n,0),
          "weighted_support":round(weighted.get(n,0)*100,1),
          "gap":round(gap,3),
          "qualifies":bool(qualifies)
        })

    if edge[0]["qualifies"]:
        target=21
        reasons.append("21名仍有强共识")
    if target>=21 and edge[1]["qualifies"]:
        target=22
        reasons.append("22名仍有强共识")

    # 23: much stricter than 21/22.
    band=max(0.0,s19-final_score.get(ranked[22],0.0))
    strong_edge=sum(1 for x in edge if x["support"]>=3)
    all_three=all(x["qualifies"] for x in edge)
    recent_counts=_recent_f_code_counts(6)
    recent23=sum(1 for x in recent_counts[:5] if x==23)
    last_was23=bool(recent_counts and recent_counts[0]==23)
    allow23=(not last_was23 and recent23<2)

    if target>=22 and all_three and strong_edge>=2 and band<=.15 and allow23:
        target=23
        reasons.append("高分歧保险档23码")
    elif target>=22 and all_three and not allow23:
        reasons.append("23码触发频率限制，保持22码")

    # Shrink to 19 only when there is a real score cliff and no protected
    # 3/5 consensus would be lost.
    gap19_20=max(0.0,s19-s20)
    n20=ranked[19]
    if (
        target==20
        and gap19_20>=.115
        and support.get(n20,0)<=1
        and len(protected3)<=19
        and not edge[0]["qualifies"]
    ):
        target=19
        reasons=["19/20出现明显断层，收缩19码"]

    target=max(19,min(23,target))
    return target,reasons,edge

def _predict20_hot(r, profile):
    """F动态19~23码：模型池主导，旧AI/趋势只做辅助。

    Rules:
    - Default 20 codes, dynamically 19~23.
    - 3/5 specialist consensus is protected.
    - Strong 2/5 consensus is prioritized.
    - Cold-zodiac / zodiac quotas become SOFT structure signals and cannot
      delete a protected consensus number.
    - 23-code mode is intentionally frequency-limited.
    """
    # Existing trend/AI/correction stack is retained as an AUXILIARY signal.
    aux_score,zmap,zheat,ctx=_selection_number_scores(r,profile,"20")
    aux_n=_norm_values(aux_score,range(1,50))

    # Specialist pool is now the primary framework.
    pool_score,models,perf=_pool_ensemble_score(r,profile,"20")
    models,perf,ranks,support,weighted,top15=_f_consensus_map(r,profile)

    # Blind-spot correction is only a small final rescue layer.
    blind,blind_meta=_pool_error_rescue_score(r,profile)
    blind_ready=bool(blind_meta.get("ready"))
    blind_w=min(.10,max(.05,float(ctx.get("error_rescue_pct",0))/100.0)) if blind_ready else 0.0
    aux_w=.18
    pool_w=1.0-aux_w-blind_w

    coldest3,cold_meta=_final_coldest3(r,profile,"20",zheat)
    coldset=set(coldest3)

    raw={}
    for n in range(1,50):
        # Consensus has direct value, independent of the average pool score.
        consensus_bonus=.055*(support.get(n,0)/5.0)+.065*weighted.get(n,0)
        v=(
          pool_w*pool_score.get(n,.5)
          +aux_w*aux_n.get(n,.5)
          +blind_w*blind.get(n,.5)
          +consensus_bonus
        )

        # Coldest3 is now a SOFT penalty only for low-consensus numbers.
        # 2/5 and 3/5 consensus numbers are never hard-deleted by zodiac.
        if zmap.get(n) in coldset and support.get(n,0)<=1:
            v-=.075
        elif zmap.get(n) in coldset and support.get(n,0)==2:
            v-=.015
        raw[n]=v

    final_score=_norm_values(raw,range(1,50))
    ranked49=sorted(range(1,50),key=lambda n:(-final_score.get(n,-1e9),n))

    protected3=[n for n in ranked49 if support.get(n,0)>=3]
    protected2_candidates=[
      n for n in ranked49
      if support.get(n,0)==2
      and (
        weighted.get(n,0)>=.42
        or top15.get(n,0)>=2
        or pool_score.get(n,0)>=.78
      )
    ]

    # v38: strong 2/5 consensus gets at most FOUR protected seats in F.
    # Rank them by current model weight support first, then pool strength,
    # then final F score. 3/5+ consensus remains unlimited/protected.
    protected2=sorted(
      protected2_candidates,
      key=lambda n:(
        -weighted.get(n,0),
        -pool_score.get(n,0),
        -final_score.get(n,0),
        ranked49.index(n)
      )
    )[:4]

    target_count,count_reasons,edge_candidates=_choose_f_dynamic_count(
      ranked49,final_score,pool_score,support,weighted,protected3
    )

    # Never choose fewer codes than the number of true 3/5 consensus codes.
    target_count=max(target_count,min(23,len(protected3)))

    # Priority order:
    #   1) protected 3/5 consensus
    #   2) strong 2/5 consensus near the final cut
    #   3) remaining final ranking
    priority=[]
    for n in protected3:
        if n not in priority:
            priority.append(n)
    for n in protected2:
        rank=ranked49.index(n)+1
        if rank<=target_count+4 and n not in priority:
            priority.append(n)
    for n in ranked49:
        if n not in priority:
            priority.append(n)

    selected=priority[:target_count]

    # Safety: if priority protection pushed an extremely low-ranked number in,
    # replace only non-protected tail numbers, never a 3/5 consensus code.
    selected_set=set(selected)
    for n in protected3:
        if n not in selected_set:
            replaceables=[x for x in selected if x not in protected3]
            if not replaceables:
                break
            out=min(replaceables,key=lambda x:final_score.get(x,0))
            selected[selected.index(out)]=n
            selected_set.discard(out); selected_set.add(n)

    # Keep display/copy deterministic; ranking metadata retains priority.
    selected=sorted(set(selected))
    if len(selected)>target_count:
        selected=selected[:target_count]

    # Metadata for explainability.
    consensus_summary={
      "protected3_count":len(protected3),
      "protected3":[f"{n:02d}" for n in protected3[:12]],
      "protected2_count":len(protected2),
      "protected2_cap":4,
      "protected2":[f"{n:02d}" for n in protected2],
      "pool_primary_pct":round(pool_w*100,1),
      "aux_ai_trend_pct":round(aux_w*100,1),
      "blind_rescue_pct":round(blind_w*100,1),
    }

    # Keep old diagnostic keys for UI/API compatibility.
    ztrans,ztrans_meta=_zodiac_transition_model(r)
    return selected,{
      "hot_zodiacs":[z for z in ALL_ZODIACS if z not in coldset],
      "top3_hot":sorted(ALL_ZODIACS,key=lambda z:(-zheat.get(z,-1e9),z))[:3],
      "coldest3":coldest3,
      "cold_meta":cold_meta,
      "rescued_cold_zodiacs":cold_meta.get("rescued_zodiacs",[]),
      "killed_head":"",
      "weak_head":"",
      "head_decision":_pool_04_weakness_decision(r,profile,"20"),
      "groups":[],
      "ranked49":ranked49,
      "edge_rescue_swaps":[],
      "dynamic_count":target_count,
      "count_reasons":count_reasons,
      "edge_candidates":edge_candidates,
      "consensus":consensus_summary,
      "regime":(ctx.get("regime") or {}).get("name","平衡"),
      "correction_trained":ctx.get("correction_trained",0),
      "correction_weight_pct":ctx.get("correction_weight_pct",0),
      "trend_weight_pct":ctx.get("trend_weight_pct",0),
      "base_ai_weight_pct":ctx.get("base_ai_weight_pct",0),
      "pool_mix_pct":round(pool_w*100,1),
      "pool_weights_pct":{k:round(100*v,1) for k,v in (perf.get("weights") or {}).items()},
      "error_rescue_pct":round(blind_w*100,1),
      "error_rescue_meta":blind_meta,
      "zodiac_transition_top":sorted(ALL_ZODIACS,key=lambda z:(-ztrans.get(z,0),z))[:4],
      "zodiac_transition_samples":ztrans_meta.get("samples",0)
    }


def _ten27_distribution(block,mapper,keys):
    c=Counter()
    for x in block:
        try:
            k=mapper(x)
        except Exception:
            k=None
        if k in keys:
            c[k]+=1
    n=max(1,sum(c.values()))
    return {k:c[k]/n for k in keys}

def _ten27_l1(a,b,keys):
    return sum(abs(float(a.get(k,0.0))-float(b.get(k,0.0))) for k in keys)

def _ten_horizon_analog_score(r):
    """Historical state -> following 10 specials.

    For each historical state, compare its preceding 10-draw structure with
    today's preceding 10 draws. Similar states vote for the numbers that
    appeared in their NEXT ten specials.
    """
    nums=range(1,50)
    if len(r)<90:
        return {n:.5 for n in nums},{"samples":0,"weight":0.0}

    cur=r[:10]
    zkeys=ALL_ZODIACS
    hkeys=list(HEAD_BASE)
    wpkeys=WAVE_PARITY_KEYS

    cur_z=_ten27_distribution(cur,lambda x: normalize_z(x["z7"] or ""),zkeys)
    cur_h=_ten27_distribution(cur,lambda x: head_of(x["special"]),hkeys)
    cur_wp=_ten27_distribution(cur,lambda x: wave_parity_of(x["special"]),wpkeys)

    cur_latest_z=normalize_z(r[0]["z7"] or "")
    cur_latest_h=head_of(r[0]["special"])
    cur_latest_wp=wave_parity_of(r[0]["special"])

    score=defaultdict(float)
    total=0.0
    samples=0

    # j is the historical state issue; j-1 ... j-10 are its "future 10".
    for j in range(18,min(len(r)-1,620)):
        if j<10 or j+9>=len(r):
            continue
        state_block=r[j:j+10]
        hz=_ten27_distribution(state_block,lambda x: normalize_z(x["z7"] or ""),zkeys)
        hh=_ten27_distribution(state_block,lambda x: head_of(x["special"]),hkeys)
        hw=_ten27_distribution(state_block,lambda x: wave_parity_of(x["special"]),wpkeys)

        dz=_ten27_l1(cur_z,hz,zkeys)
        dh=_ten27_l1(cur_h,hh,hkeys)
        dw=_ten27_l1(cur_wp,hw,wpkeys)

        sim=math.exp(-1.55*(.48*dz+.24*dh+.28*dw))
        if normalize_z(r[j]["z7"] or "")==cur_latest_z:
            sim*=1.18
        if head_of(r[j]["special"])==cur_latest_h:
            sim*=1.10
        if wave_parity_of(r[j]["special"])==cur_latest_wp:
            sim*=1.12
        sim*=exp_weight(j,420)

        if sim<.025:
            continue

        # All ten positions count because the user's target is "10期中几".
        for d in range(1,11):
            x=r[j-d]
            n=int(x["special"])
            # Very mild distance discount; period 10 still matters.
            dwgt=1.0-.012*(d-1)
            score[n]+=sim*dwgt
        total+=sim
        samples+=1

    if samples<8:
        return {n:.5 for n in nums},{"samples":samples,"weight":round(total,3)}

    norm=_norm_values(score,nums)
    return norm,{"samples":samples,"weight":round(total,3)}

def _ten27_model_performance(force=False):
    """How well each T/Z/C/W/A locked list covered the NEXT ten specials.

    Each historical model list is frozen at its target issue and tested against
    that issue plus the following 9 actual specials. This directly matches the
    27-code use case better than one-step hit rate.
    """
    now=time.time()
    with ten27_perf_lock:
        cached=ten27_perf_cache.get("data")
        if cached is not None and not force and now-float(ten27_perf_cache.get("ts",0))<20:
            return cached

    profiles=list(POOL_MODEL_PROFILES.values())
    placeholders=",".join("?" for _ in profiles)
    with db_lock:
        c=connect()
        try:
            rows=c.execute(f"""SELECT target_issue,profile,special24,actual_special
                               FROM prediction_log
                               WHERE profile IN ({placeholders}) AND settled=1
                               ORDER BY CAST(target_issue AS INTEGER) ASC""",
                           tuple(profiles)).fetchall()
        finally:
            c.close()

    actual={}
    by_profile={p:[] for p in profiles}
    for x in rows:
        issue=str(x["target_issue"])
        if x["actual_special"] is not None:
            actual[issue]=int(x["actual_special"])
        by_profile[str(x["profile"])].append(x)

    key_by_profile={v:k for k,v in POOL_MODEL_PROFILES.items()}
    stats={}
    raw={}
    baseline=10*(20/49)

    for prof,plist in by_profile.items():
        windows=[]
        for x in plist:
            try:
                start=int(x["target_issue"])
            except Exception:
                continue
            outcomes=[]
            ok=True
            for d in range(10):
                a=actual.get(str(start+d))
                if a is None:
                    ok=False
                    break
                outcomes.append(a)
            if not ok:
                continue
            codes=set(_csv_nums(x["special24"]))
            if not codes:
                continue
            hits=sum(1 for a in outcomes if a in codes)
            windows.append((start,hits))

        windows=sorted(windows,key=lambda y:y[0],reverse=True)[:30]
        n=len(windows)
        total_hits=sum(h for _,h in windows)
        avg=(total_hits/n) if n else baseline
        # shrink to random-coverage baseline until enough real 10-window samples.
        smooth=(total_hits+4*baseline)/(n+4)
        key=key_by_profile.get(prof,prof)
        stats[key]={
          "windows":n,
          "avg_hits10":round(avg,2) if n else 0.0,
          "smoothed_hits10":round(smooth,2),
          "best10":max((h for _,h in windows),default=0),
          "worst10":min((h for _,h in windows),default=0)
        }
        raw[key]=math.exp(.72*(smooth-baseline))

    # Missing models stay equal rather than being punished for missing samples.
    for k in POOL_MODEL_PROFILES:
        if k not in raw:
            raw[k]=1.0
            stats[k]={"windows":0,"avg_hits10":0.0,"smoothed_hits10":round(baseline,2),"best10":0,"worst10":0}

    weights=_normalize_capped_weights(raw,.10,.35)
    for k in stats:
        stats[k]["weight_pct"]=round(100*weights.get(k,.20),1)

    mature=max((v["windows"] for v in stats.values()),default=0)
    data={"stats":stats,"weights":weights,"mature_windows":mature}
    with ten27_perf_lock:
        ten27_perf_cache["ts"]=now
        ten27_perf_cache["data"]=data
    return data

def _ten27_recent_frequency(r):
    """Longer-horizon stability prior: recent 80 special-number frequency."""
    c=Counter()
    for i,x in enumerate(r[:80]):
        c[int(x["special"])]+=exp_weight(i,28)
    return _norm_values(c,range(1,50))

def _ten27_horizon_score(r,profile):
    analog,analog_meta=_ten_horizon_analog_score(r)
    specialists=_specialist_model_scores(r,profile,"27")
    perf=_ten27_model_performance()
    weights=perf.get("weights") or {k:.20 for k in specialists}

    spec={}
    for n in range(1,50):
        spec[n]=sum(float(weights.get(k,.20))*float(specialists[k].get(n,.5)) for k in specialists)
    spec=_norm_values(spec,range(1,50))
    freq=_ten27_recent_frequency(r)

    # The historical next-10 analog is the largest component.
    score={
      n:.52*analog.get(n,.5)+.40*spec.get(n,.5)+.08*freq.get(n,.5)
      for n in range(1,50)
    }
    score=_norm_values(score,range(1,50))
    return score,specialists,perf,{
      "analog_samples":analog_meta.get("samples",0),
      "analog_weight":analog_meta.get("weight",0.0)
    }

def _ten27_head_decision(score,specialists,perf):
    """0/4 judgment for the whole upcoming 10-period round, not one issue."""
    def avg(sm,h):
        ns=[n for n in range(1,50) if head_of(n)==h]
        return sum(float(sm.get(n,.5)) for n in ns)/max(1,len(ns))

    weights=perf.get("weights") or {k:.20 for k in specialists}
    horizon={"0头":avg(score,"0头"),"4头":avg(score,"4头")}
    votes={"0头":0,"4头":0}
    for k,sm in specialists.items():
        weak="0头" if avg(sm,"0头")<=avg(sm,"4头") else "4头"
        votes[weak]+=1

    weak="0头" if horizon["0头"]<=horizon["4头"] else "4头"
    strong="4头" if weak=="0头" else "0头"
    gap=horizon[strong]-horizon[weak]
    support=votes[weak]/5.0

    # Because a wrong hard kill hurts ten consecutive bets, threshold is strict.
    active=(gap>=.115 and support>=.60)
    return {
      "active":bool(active),
      "killed_head":weak if active else "",
      "weak_head":weak,
      "gap":round(gap,3),
      "vote_support_pct":round(100*support,1),
      "votes":votes,
      "horizon":{"0头":round(horizon["0头"],3),"4头":round(horizon["4头"],3)},
      "reason":f"未来10期{weak}明显偏弱" if active else f"未来10期{weak}略弱，不硬杀"
    }

def _build_27_ten_horizon_base(r,profile):
    score,specialists,perf,meta=_ten27_horizon_score(r,profile)
    head=_ten27_head_decision(score,specialists,perf)
    killed=head.get("killed_head","")
    zmap=_number_zodiac_map(r)

    ranked=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
    selected=[]
    zcount=Counter()

    # Ten-period coverage uses broad zodiac diversification. Max4 per zodiac,
    # but there is no "coldest3 hard kill" in this horizon model.
    for n in ranked:
        if killed and head_of(n)==killed:
            continue
        z=zmap.get(n)
        if z and zcount[z]>=4:
            continue
        selected.append(n)
        if z: zcount[z]+=1
        if len(selected)>=27:
            break

    if len(selected)<27:
        for n in ranked:
            if n in selected:
                continue
            if killed and head_of(n)==killed:
                continue
            selected.append(n)
            if len(selected)>=27:
                break

    meta=dict(meta)
    meta.update({
      "head_decision":head,
      "killed_head":killed,
      "ranked49":ranked,
      "model10":perf,
      "base_score_top":[{"code":n,"score":round(score.get(n,0),3)} for n in ranked[:10]]
    })
    return selected[:27],score,specialists,meta

def _ten27_round_context(target_issue):
    """Determine current 10-prediction round from the new honest profile.

    The first locked row of each group of 10 is the base 27 list. This means a
    deployment can start a fresh 10-round at any issue; it is not tied to issue
    numbers ending in 0.
    """
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT target_issue,special24,settled
                              FROM prediction_log
                              WHERE profile=?
                              ORDER BY CAST(target_issue AS INTEGER) ASC""",
                           (TEN27_PROFILE,)).fetchall()
        finally:
            c.close()

    issues=[str(x["target_issue"]) for x in rows]
    target=str(target_issue)
    if target in issues:
        idx=issues.index(target)
        gstart=(idx//10)*10
        pos=idx-gstart+1
        base=_csv_nums(rows[gstart]["special24"])[:27]
        start_issue=issues[gstart]
        return {
          "existing":True,"new_round":False,"position":pos,
          "start_issue":start_issue,
          "end_issue":_issue_add(start_issue,9) if start_issue else "",
          "base_codes":base
        }

    n=len(rows)
    rem=n%10
    if rem:
        gstart=n-rem
        base=_csv_nums(rows[gstart]["special24"])[:27]
        start_issue=issues[gstart]
        return {
          "existing":False,"new_round":False,"position":rem+1,
          "start_issue":start_issue,
          "end_issue":_issue_add(start_issue,9) if start_issue else "",
          "base_codes":base
        }

    return {
      "existing":False,"new_round":True,"position":1,
      "start_issue":target,
      "end_issue":_issue_add(target,9) if target else "",
      "base_codes":[]
    }


def _ten27_v2_round_context(target_issue):
    """23 core + 4 mobile round context.

    A fresh V2 profile is used so old fixed-27 results remain intact and the
    new rescue counters are honest forward records.
    """
    with db_lock:
        c=connect()
        try:
            full=c.execute("""SELECT target_issue,special24,settled
                              FROM prediction_log
                              WHERE profile=?
                              ORDER BY CAST(target_issue AS INTEGER) ASC""",
                           (TEN27_V2_PROFILE,)).fetchall()
            core_rows=c.execute("""SELECT target_issue,special24
                                   FROM prediction_log
                                   WHERE profile=?
                                   ORDER BY CAST(target_issue AS INTEGER) ASC""",
                                (TEN27_CORE_PROFILE,)).fetchall()
            mobile_rows=c.execute("""SELECT target_issue,special24
                                     FROM prediction_log
                                     WHERE profile=?
                                     ORDER BY CAST(target_issue AS INTEGER) ASC""",
                                  (TEN27_MOBILE_PROFILE,)).fetchall()
        finally:
            c.close()

    target=str(target_issue)
    issues=[str(x["target_issue"]) for x in full]

    # Determine current sequential group of ten.
    if target in issues:
        idx=issues.index(target)
        gstart=(idx//10)*10
        pos=idx-gstart+1
    else:
        n=len(full)
        rem=n%10
        if rem:
            gstart=n-rem
            pos=rem+1
        else:
            return {
              "new_round":True,"position":1,
              "start_issue":target,
              "end_issue":_issue_add(target,9) if target else "",
              "core23":[],"mobile4":[],
              "mobile_changes":0
            }

    round_issues=issues[gstart:gstart+10]
    if not round_issues:
        return {
          "new_round":True,"position":1,
          "start_issue":target,
          "end_issue":_issue_add(target,9) if target else "",
          "core23":[],"mobile4":[],"mobile_changes":0
        }

    start_issue=round_issues[0]
    round_set=set(round_issues)
    core_map={str(x["target_issue"]):_csv_nums(x["special24"]) for x in core_rows if str(x["target_issue"]) in round_set}
    mobile_map={str(x["target_issue"]):_csv_nums(x["special24"]) for x in mobile_rows if str(x["target_issue"]) in round_set}

    core23=list(core_map.get(start_issue) or [])
    if not core23:
        # Safe fallback from the first full V2 row.
        first=full[gstart]
        core23=_csv_nums(first["special24"])[:23]

    # Latest mobile set is the current mobile warehouse.
    mobile4=[]
    ordered_mobile=[]
    for issue in round_issues:
        m=list(mobile_map.get(issue) or [])
        if m:
            mobile4=m[:4]
            ordered_mobile.append(tuple(sorted(m[:4])))

    if not mobile4:
        first_full=_csv_nums(full[gstart]["special24"])
        mobile4=first_full[23:27]

    changes=0
    prev=None
    for m in ordered_mobile:
        if prev is not None and m!=prev:
            changes+=1
        prev=m

    return {
      "new_round":False,
      "position":pos,
      "start_issue":start_issue,
      "end_issue":_issue_add(start_issue,9) if start_issue else "",
      "core23":core23[:23],
      "mobile4":mobile4[:4],
      "mobile_changes":changes
    }

def _ten27_specialist_support(specialists):
    ranks={}
    for k,sm in specialists.items():
        order=sorted(range(1,50),key=lambda n:(-sm.get(n,-1e9),n))
        ranks[k]={n:i+1 for i,n in enumerate(order)}
    return ranks

def _ten27_adjust_mobile4(r,profile,core23,current4,changes_used):
    """At most two mobile-warehouse changes in one 10-period round.

    Only one slot can change on a single issue, and only if the new candidate
    is materially stronger for the future-10 objective.
    """
    score,specialists,perf,meta=_ten27_horizon_score(r,profile)
    ranks=_ten27_specialist_support(specialists)

    cur=list(current4)[:4]
    if len(cur)<4:
        ranked=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
        for n in ranked:
            if n in core23 or n in cur:
                continue
            cur.append(n)
            if len(cur)>=4:
                break

    if changes_used>=2 or not cur:
        return cur,{"changed":False,"changes_used":changes_used,"reason":"本轮机动调整次数已用完" if changes_used>=2 else "保持"}

    weakest=min(cur,key=lambda n:score.get(n,0))
    weakest_score=float(score.get(weakest,0))

    ranked=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
    candidates=[]
    for n in ranked:
        if n in core23 or n in cur:
            continue
        support=sum(1 for k in specialists if ranks[k].get(n,99)<=20)
        top12=sum(1 for k in specialists if ranks[k].get(n,99)<=12)
        gain=float(score.get(n,0))-weakest_score
        hrank=ranked.index(n)+1
        active=(
          gain>=.115
          and hrank<=18
          and (
            support>=4
            or (support>=3 and top12>=2 and float(score.get(n,0))>=.76)
          )
        )
        if active:
            quality=.55*float(score.get(n,0))+.30*(support/5.0)+.15*(top12/5.0)
            candidates.append((quality,n,gain,hrank,support,top12))

    if not candidates:
        return cur,{"changed":False,"changes_used":changes_used,"reason":"没有足够强的新机动码"}

    _q,newn,gain,hrank,support,top12=max(candidates,key=lambda x:(x[0],-x[3],-x[1]))
    new4=[x for x in cur if x!=weakest]+[newn]
    return new4[:4],{
      "changed":True,
      "out":weakest,
      "in":newn,
      "gain":round(gain,3),
      "rank":hrank,
      "support_models":support,
      "top12_models":top12,
      "changes_used":changes_used+1,
      "reason":f"机动位 {weakest:02d}→{newn:02d}"
    }

def _ten27_rescue28_v2(r,profile,core23,mobile4):
    """28th code is emergency insurance, never a replacement."""
    score,specialists,perf,meta=_ten27_horizon_score(r,profile)
    ranks=_ten27_specialist_support(specialists)
    occupied=set(core23)|set(mobile4)
    ranked=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))

    candidates=[]
    for n in ranked:
        if n in occupied:
            continue
        support=sum(1 for k in specialists if ranks[k].get(n,99)<=18)
        top10=sum(1 for k in specialists if ranks[k].get(n,99)<=10)
        hrank=ranked.index(n)+1
        sc=float(score.get(n,0))
        # Strict: 28th seat should be rare.
        active=(
          (hrank<=13 and support>=4 and sc>=.78)
          or (hrank<=10 and support>=3 and top10>=2 and sc>=.82)
        )
        if active:
            quality=.58*sc+.27*(support/5.0)+.15*(top10/5.0)
            candidates.append((quality,n,hrank,support,top10,sc))

    if not candidates:
        return {"active":False,"code":None,"message":""},meta

    _q,n,hrank,support,top10,sc=max(candidates,key=lambda x:(x[0],-x[2],-x[1]))
    return {
      "active":True,
      "code":n,
      "rank":hrank,
      "support_models":support,
      "top10_models":top10,
      "ensemble_score":round(sc,3),
      "message":f"未来10期模型发现核心23+机动4之外强覆盖码 {n:02d}，本期临时补为第28码"
    },meta

def _stats27_core_mobile():
    """Current/last round: core23 hits, mobile rescue, 28 rescue, final hits."""
    profiles=[TEN27_V2_PROFILE,TEN27_CORE_PROFILE,TEN27_MOBILE_PROFILE,TEN27_28_PROFILE]
    placeholders=",".join("?" for _ in profiles)
    with db_lock:
        c=connect()
        try:
            rows=c.execute(f"""SELECT target_issue,profile,hit24,settled,special24
                               FROM prediction_log
                               WHERE profile IN ({placeholders})
                               ORDER BY CAST(target_issue AS INTEGER) ASC""",
                           tuple(profiles)).fetchall()
        finally:
            c.close()

    by_issue={}
    for x in rows:
        issue=str(x["target_issue"])
        by_issue.setdefault(issue,{})[str(x["profile"])]=dict(x)

    issues=sorted([i for i,d in by_issue.items() if TEN27_V2_PROFILE in d],key=lambda x:int(x))
    groups=[]
    for i in range(0,len(issues),10):
        chunk=issues[i:i+10]
        if not chunk:
            continue
        settled_issues=[issue for issue in chunk if int(by_issue[issue][TEN27_V2_PROFILE]["settled"] or 0)==1]
        core_hits=mobile_rescues=rescue28=final_hits=0
        mobile_sets=[]
        for issue in chunk:
            d=by_issue[issue]
            mrow=d.get(TEN27_MOBILE_PROFILE)
            if mrow:
                ms=tuple(sorted(_csv_nums(mrow["special24"])))
                if ms:
                    mobile_sets.append(ms)

        mobile_changes=0
        prev=None
        for ms in mobile_sets:
            if prev is not None and ms!=prev:
                mobile_changes+=1
            prev=ms

        for issue in settled_issues:
            d=by_issue[issue]
            ch=int((d.get(TEN27_CORE_PROFILE) or {}).get("hit24",0) or 0)
            mh=int((d.get(TEN27_MOBILE_PROFILE) or {}).get("hit24",0) or 0)
            rh=int((d.get(TEN27_28_PROFILE) or {}).get("hit24",0) or 0)
            fh=int((d.get(TEN27_V2_PROFILE) or {}).get("hit24",0) or 0)
            core_hits+=ch
            if mh and not ch:
                mobile_rescues+=1
            if rh and not ch and not mh:
                rescue28+=1
            final_hits+=fh

        start=chunk[0]
        groups.append({
          "start":start,
          "end":_issue_add(start,9) if start else "",
          "n":len(settled_issues),
          "core23_hits":core_hits,
          "mobile4_rescues":mobile_rescues,
          "rescue28":rescue28,
          "final_hits":final_hits,
          "mobile_changes":mobile_changes
        })

    empty={"start":0,"end":0,"n":0,"core23_hits":0,"mobile4_rescues":0,"rescue28":0,"final_hits":0,"mobile_changes":0}
    current=groups[-1] if groups else dict(empty)
    complete=next((g for g in reversed(groups) if g["n"]>=10),None)
    return {
      "current":current,
      "last_complete":complete,
      "rounds_completed":sum(1 for g in groups if g["n"]>=10),
      "overall":_profile_hit_stats(TEN27_V2_PROFILE,60)
    }

def _f_failure_attribution(window=60):
    """Why did F miss: fusion loss vs whole model-pool miss?"""
    profiles=list(POOL_MODEL_PROFILES.values())+[POOL_FINAL_PROFILE]
    placeholders=",".join("?" for _ in profiles)
    with db_lock:
        c=connect()
        try:
            rows=c.execute(f"""SELECT target_issue,profile,hit24,actual_special
                               FROM prediction_log
                               WHERE profile IN ({placeholders}) AND settled=1
                               ORDER BY CAST(target_issue AS INTEGER) DESC""",
                           tuple(profiles)).fetchall()
        finally:
            c.close()

    p2k={v:k for k,v in POOL_MODEL_PROFILES.items()}
    by={}
    for x in rows:
        issue=str(x["target_issue"])
        d=by.setdefault(issue,{"hits":{},"F":None})
        if str(x["profile"])==POOL_FINAL_PROFILE:
            d["F"]=int(x["hit24"] or 0)
        else:
            k=p2k.get(str(x["profile"]))
            if k:
                d["hits"][k]=int(x["hit24"] or 0)

    fusion_miss=pool_all_miss=consensus3_lost=two_model_lost=0
    total_f_miss=0
    used=0
    for issue in sorted(by,key=lambda x:int(x),reverse=True):
        d=by[issue]
        if d["F"] is None or len(d["hits"])<3:
            continue
        used+=1
        if d["F"]==0:
            total_f_miss+=1
            hit_count=sum(d["hits"].values())
            if hit_count==0:
                pool_all_miss+=1
            else:
                fusion_miss+=1
                if hit_count>=3:
                    consensus3_lost+=1
                elif hit_count==2:
                    two_model_lost+=1
        if used>=window:
            break

    return {
      "n":used,
      "f_misses":total_f_miss,
      "fusion_miss":fusion_miss,
      "pool_all_miss":pool_all_miss,
      "consensus3_lost":consensus3_lost,
      "two_model_lost":two_model_lost
    }


def _long27_window20_filter(r):
    """User-defined long-code filter from the previous 20 specials.

    Wave-parity:
    - if the two least frequent categories total <=4/20, kill both;
    - if not, but the single least category appears <=1/20, kill that one.

    0/4 head:
    - compare 0头 and 4头 counts in the same 20 specials;
    - kill the less frequent one; tie = no head kill.
    """
    block=r[:20]
    wp=Counter()
    heads=Counter()
    for x in block:
        try:
            n=int(x["special"])
        except Exception:
            continue
        wp[wave_parity_of(n)]+=1
        h=head_of(n)
        if h in ("0头","4头"):
            heads[h]+=1

    all_wp=list(WAVE_PARITY_KEYS)
    ordered=sorted(all_wp,key=lambda k:(wp.get(k,0),k))
    killed_wp=[]
    if len(ordered)>=2 and wp.get(ordered[0],0)+wp.get(ordered[1],0) <= 4:
        killed_wp=ordered[:2]
    elif ordered and wp.get(ordered[0],0) <= 1:
        killed_wp=ordered[:1]

    c0=heads.get("0头",0)
    c4=heads.get("4头",0)
    killed_head=""
    if c0<c4:
        killed_head="0头"
    elif c4<c0:
        killed_head="4头"

    return {
      "window":len(block),
      "wave_counts":{k:int(wp.get(k,0)) for k in all_wp},
      "killed_wave_parity":killed_wp,
      "head_counts":{"0头":int(c0),"4头":int(c4)},
      "killed_head":killed_head
    }

def _long27_filter_token(filt,segment=1,switches=0):
    waves=",".join(filt.get("killed_wave_parity") or [])
    head=str(filt.get("killed_head") or "")
    return f"LONG27|waves={waves}|head={head}|segment={int(segment)}|switches={int(switches)}"

def _long27_parse_token(token):
    out={"waves":[],"head":"","segment":1,"switches":0}
    t=str(token or "")
    if not t.startswith("LONG27|"):
        return out
    for part in t.split("|")[1:]:
        if "=" not in part:
            continue
        k,v=part.split("=",1)
        if k=="waves":
            out["waves"]=[x for x in v.split(",") if x]
        elif k=="head":
            out["head"]=v
        elif k=="segment":
            try: out["segment"]=int(v)
            except Exception: pass
        elif k=="switches":
            try: out["switches"]=int(v)
            except Exception: pass
    return out

def _long27_killed_streak(r,waves,head,max_check=3):
    """How many newest consecutive specials fall into the active killed signal."""
    wp_streak=0
    head_streak=0
    waves=set(waves or [])
    for x in r[:max_check]:
        try:
            n=int(x["special"])
        except Exception:
            break
        if wave_parity_of(n) in waves:
            wp_streak+=1
        else:
            break
    for x in r[:max_check]:
        try:
            n=int(x["special"])
        except Exception:
            break
        if head and head_of(n)==head:
            head_streak+=1
        else:
            break
    return {"wave":wp_streak,"head":head_streak,"trigger":(wp_streak>=2 or head_streak>=2)}

def _long27_v3_round_context(target_issue):
    """Sequential groups of 10 for the V3 long-code strategy."""
    with db_lock:
        c=connect()
        try:
            full=c.execute("""SELECT target_issue,special24,settled
                              FROM prediction_log
                              WHERE profile=?
                              ORDER BY CAST(target_issue AS INTEGER) ASC""",
                           (TEN27_V3_PROFILE,)).fetchall()
            cores=c.execute("""SELECT target_issue,special24
                               FROM prediction_log
                               WHERE profile=?
                               ORDER BY CAST(target_issue AS INTEGER) ASC""",
                            (TEN27_V3_CORE_PROFILE,)).fetchall()
            mobiles=c.execute("""SELECT target_issue,special24
                                 FROM prediction_log
                                 WHERE profile=?
                                 ORDER BY CAST(target_issue AS INTEGER) ASC""",
                              (TEN27_V3_MOBILE_PROFILE,)).fetchall()
            audits=c.execute("""SELECT target_issue,regime
                                FROM strategy_audit
                                WHERE profile=?
                                ORDER BY CAST(target_issue AS INTEGER) ASC""",
                             (TEN27_V3_PROFILE,)).fetchall()
        finally:
            c.close()

    target=str(target_issue)
    issues=[str(x["target_issue"]) for x in full]
    if target in issues:
        idx=issues.index(target)
        gstart=(idx//10)*10
        pos=idx-gstart+1
    else:
        n=len(full)
        rem=n%10
        if rem:
            gstart=n-rem
            pos=rem+1
        else:
            return {
              "new_round":True,"position":1,"start_issue":target,
              "end_issue":_issue_add(target,9),"core23":[],"mobile4":[],
              "prev_filter":{"waves":[],"head":"","segment":1,"switches":0}
            }

    round_issues=issues[gstart:gstart+10]
    if not round_issues:
        return {
          "new_round":True,"position":1,"start_issue":target,
          "end_issue":_issue_add(target,9),"core23":[],"mobile4":[],
          "prev_filter":{"waves":[],"head":"","segment":1,"switches":0}
        }

    rset=set(round_issues)
    core_map={str(x["target_issue"]):_csv_nums(x["special24"]) for x in cores if str(x["target_issue"]) in rset}
    mobile_map={str(x["target_issue"]):_csv_nums(x["special24"]) for x in mobiles if str(x["target_issue"]) in rset}
    audit_map={str(x["target_issue"]):str(x["regime"] or "") for x in audits if str(x["target_issue"]) in rset}

    start_issue=round_issues[0]
    latest_issue=round_issues[-1]
    core23=list(core_map.get(latest_issue) or core_map.get(start_issue) or [])
    mobile4=list(mobile_map.get(latest_issue) or mobile_map.get(start_issue) or [])
    prev_token=audit_map.get(latest_issue) or audit_map.get(start_issue) or ""

    return {
      "new_round":False,"position":pos,
      "start_issue":start_issue,"end_issue":_issue_add(start_issue,9),
      "core23":core23[:23],"mobile4":mobile4[:4],
      "prev_filter":_long27_parse_token(prev_token)
    }

def _build_long27_codes(r,profile,filt):
    """Build 27 using future-10 score, then apply user's 20-draw kills."""
    score,specialists,perf,meta=_ten27_horizon_score(r,profile)
    ranked=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
    kw=set(filt.get("killed_wave_parity") or [])
    kh=str(filt.get("killed_head") or "")

    def excluded(n):
        return (kh and head_of(n)==kh) or (wave_parity_of(n) in kw)

    selected=[n for n in ranked if not excluded(n)][:27]
    forced_fill=[]
    if len(selected)<27:
        # Structural filters can overlap and leave fewer than 27 numbers.
        # Keep the kill logic dominant, then fill the minimum number of
        # highest-ranked excluded numbers so output remains exactly 27.
        for n in ranked:
            if n in selected:
                continue
            selected.append(n)
            forced_fill.append(n)
            if len(selected)>=27:
                break

    meta=dict(meta)
    meta.update({
      "ranked49":ranked,
      "filter20":filt,
      "forced_fill":forced_fill
    })
    return selected[:27],score,specialists,perf,meta

def _long27_rescue28(r,profile,base27,filt):
    """Rare 28th rescue, but do not rescue an actively killed structure."""
    score,specialists,perf,meta=_ten27_horizon_score(r,profile)
    ranks=_ten27_specialist_support(specialists)
    occupied=set(base27)
    kw=set(filt.get("killed_wave_parity") or [])
    kh=str(filt.get("killed_head") or "")
    ranked=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))

    candidates=[]
    for n in ranked:
        if n in occupied:
            continue
        if kh and head_of(n)==kh:
            continue
        if wave_parity_of(n) in kw:
            continue
        support=sum(1 for k in specialists if ranks[k].get(n,99)<=18)
        top10=sum(1 for k in specialists if ranks[k].get(n,99)<=10)
        hrank=ranked.index(n)+1
        sc=float(score.get(n,0))
        if (hrank<=13 and support>=4 and sc>=.78) or (hrank<=10 and support>=3 and top10>=2 and sc>=.82):
            quality=.58*sc+.27*(support/5.0)+.15*(top10/5.0)
            candidates.append((quality,n,hrank,support,top10,sc))
    if not candidates:
        return {"active":False,"code":None,"message":""}
    _q,n,hrank,support,top10,sc=max(candidates,key=lambda x:(x[0],-x[2],-x[1]))
    return {
      "active":True,"code":n,"rank":hrank,
      "support_models":support,"top10_models":top10,
      "ensemble_score":round(sc,3),
      "message":f"长码10期发现27码外强覆盖码 {n:02d}，本期临时补为第28码"
    }

def _stats27_v3():
    profiles=[TEN27_V3_PROFILE,TEN27_V3_CORE_PROFILE,TEN27_V3_MOBILE_PROFILE,TEN27_V3_28_PROFILE]
    placeholders=",".join("?" for _ in profiles)
    with db_lock:
        c=connect()
        try:
            rows=c.execute(f"""SELECT target_issue,profile,hit24,settled,special24
                               FROM prediction_log
                               WHERE profile IN ({placeholders})
                               ORDER BY CAST(target_issue AS INTEGER) ASC""",
                           tuple(profiles)).fetchall()
            audits=c.execute("""SELECT target_issue,regime
                                FROM strategy_audit
                                WHERE profile=?
                                ORDER BY CAST(target_issue AS INTEGER) ASC""",
                             (TEN27_V3_PROFILE,)).fetchall()
        finally:
            c.close()

    by_issue={}
    for x in rows:
        issue=str(x["target_issue"])
        by_issue.setdefault(issue,{})[str(x["profile"])]=dict(x)
    audit_map={str(x["target_issue"]):str(x["regime"] or "") for x in audits}

    issues=sorted([i for i,d in by_issue.items() if TEN27_V3_PROFILE in d],key=lambda x:int(x))
    groups=[]
    for i in range(0,len(issues),10):
        chunk=issues[i:i+10]
        if not chunk: continue
        settled=[q for q in chunk if int(by_issue[q][TEN27_V3_PROFILE]["settled"] or 0)==1]
        core_hits=mobile_rescue=rescue28=final_hits=0
        segments=[]
        for q in chunk:
            tok=_long27_parse_token(audit_map.get(q,""))
            segments.append(int(tok.get("segment",1)))
        for q in settled:
            d=by_issue[q]
            ch=int((d.get(TEN27_V3_CORE_PROFILE) or {}).get("hit24",0) or 0)
            mh=int((d.get(TEN27_V3_MOBILE_PROFILE) or {}).get("hit24",0) or 0)
            rh=int((d.get(TEN27_V3_28_PROFILE) or {}).get("hit24",0) or 0)
            fh=int((d.get(TEN27_V3_PROFILE) or {}).get("hit24",0) or 0)
            core_hits+=ch
            if mh and not ch: mobile_rescue+=1
            if rh and not ch and not mh: rescue28+=1
            final_hits+=fh
        start=chunk[0]
        groups.append({
          "start":start,"end":_issue_add(start,9),"n":len(settled),
          "core23_hits":core_hits,"mobile4_rescues":mobile_rescue,
          "rescue28":rescue28,"final_hits":final_hits,
          "trend_switches":max(segments,default=1)-1
        })
    empty={"start":0,"end":0,"n":0,"core23_hits":0,"mobile4_rescues":0,"rescue28":0,"final_hits":0,"trend_switches":0}
    current=groups[-1] if groups else dict(empty)
    complete=next((g for g in reversed(groups) if g["n"]>=10),None)
    return {
      "current":current,"last_complete":complete,
      "rounds_completed":sum(1 for g in groups if g["n"]>=10),
      "overall":_profile_hit_stats(TEN27_V3_PROFILE,60)
    }

def _ten27_outside_rescue(r,profile,base27):
    """During the round, only ADD one 28th code; never replace the base 27."""
    score,specialists,perf,meta=_ten27_horizon_score(r,profile)
    ranked=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
    base=set(base27)

    model_ranks={}
    for k,sm in specialists.items():
        order=sorted(range(1,50),key=lambda n:(-sm.get(n,-1e9),n))
        model_ranks[k]={n:i+1 for i,n in enumerate(order)}

    candidates=[]
    for n in ranked:
        if n in base:
            continue
        support=sum(1 for k in specialists if model_ranks[k].get(n,99)<=20)
        top12=sum(1 for k in specialists if model_ranks[k].get(n,99)<=12)
        hrank=ranked.index(n)+1
        sc=float(score.get(n,0.0))

        # Stronger than the old one-step 28 rule because this add-on will be
        # used in a 10-period plan.
        active=(
          (hrank<=18 and support>=4 and sc>=.72)
          or (hrank<=14 and support>=3 and top12>=2 and sc>=.77)
        )
        if active:
            quality=.55*sc+.30*(support/5.0)+.15*(top12/5.0)
            candidates.append((quality,n,hrank,support,top12,sc))

    if not candidates:
        return {"active":False,"code":None,"message":""},meta

    quality,n,hrank,support,top12,sc=max(candidates,key=lambda x:(x[0],-x[2],-x[1]))
    return {
      "active":True,
      "code":n,
      "rank":hrank,
      "support_models":support,
      "top12_models":top12,
      "ensemble_score":round(sc,3),
      "message":f"未来10期模型发现基础27码外强覆盖码 {n:02d}，本期补为第28码"
    },meta

def _issue_block10(issue):
    try:
        x=int(issue)
        start=(x//10)*10
        return start,start+9
    except Exception:
        return 0,0

def _existing_27_block_codes(start,end):
    if not start:
        return []
    with db_lock:
        c=connect()
        try:
            row=c.execute("""SELECT special24 FROM prediction_log
                             WHERE profile='27码十期'
                               AND CAST(target_issue AS INTEGER) BETWEEN ? AND ?
                             ORDER BY CAST(target_issue AS INTEGER) ASC
                             LIMIT 1""",(int(start),int(end))).fetchone()
        finally:
            c.close()
    if not row:
        return []
    return [int(x) for x in str(row["special24"] or "").split(",") if str(x).strip().isdigit()]

def _tenblock_state(r,target_issue):
    start,end=_issue_block10(target_issue)
    state_issue=start-1
    if start:
        for i,x in enumerate(r):
            try:
                if int(x["issue"])==state_issue:
                    return r[i:],start,end
            except Exception:
                pass
    return r,start,end

def _predict27_tenblock(r, profile, target_issue):
    """10-period long-code mode driven by previous-20 structure.

    Normal state: keep the same 27 through the 10-period round.
    Trend-change state: if an actively killed head OR killed wave-parity
    appears for 2+ consecutive draws, re-check the latest 20 draws; when the
    weak structure changes, rebuild the 27 for the remaining round.
    """
    rc=_long27_v3_round_context(target_issue)
    current_filter=_long27_window20_filter(r)

    if rc.get("new_round") or not rc.get("core23"):
        chosen27,_score,_spec,perf,meta=_build_long27_codes(r,profile,current_filter)
        segment=1
        switches=0
        active_filter=current_filter
        switched=False
        trigger={"wave":0,"head":0,"trigger":False}
    else:
        prev=rc.get("prev_filter") or {}
        prev_waves=prev.get("waves") or []
        prev_head=prev.get("head") or ""
        segment=int(prev.get("segment") or 1)
        switches=int(prev.get("switches") or 0)
        trigger=_long27_killed_streak(r,prev_waves,prev_head,3)

        # Default: long code does not move.
        chosen27=list(rc.get("core23") or [])[:23]+list(rc.get("mobile4") or [])[:4]
        active_filter={
          "window":20,
          "wave_counts":current_filter.get("wave_counts",{}),
          "head_counts":current_filter.get("head_counts",{}),
          "killed_wave_parity":list(prev_waves),
          "killed_head":prev_head
        }
        switched=False

        # User rule: killed signal showing 2/3 periods in a row = trend change.
        if trigger.get("trigger"):
            new_waves=list(current_filter.get("killed_wave_parity") or [])
            new_head=str(current_filter.get("killed_head") or "")
            if set(new_waves)!=set(prev_waves) or new_head!=prev_head:
                chosen27,_score,_spec,perf,meta=_build_long27_codes(r,profile,current_filter)
                active_filter=current_filter
                segment+=1
                switches+=1
                switched=True

    # If no rebuild occurred above, fetch diagnostics/perf for display.
    if 'perf' not in locals():
        _score,_spec,perf,_hm=_ten27_horizon_score(r,profile)
        meta={"analog_samples":_hm.get("analog_samples",0),"forced_fill":[]}

    chosen27=list(dict.fromkeys(chosen27))[:27]
    core23=chosen27[:23]
    mobile4=chosen27[23:27]
    rescue28=_long27_rescue28(r,profile,chosen27,active_filter)
    selected=list(chosen27)
    if rescue28.get("active") and rescue28.get("code") not in selected:
        selected.append(int(rescue28["code"]))

    model_stats=(perf.get("stats") or {}) if isinstance(perf,dict) else {}
    audit_regime=_long27_filter_token(active_filter,segment,switches)

    return selected,{
      "block_start":rc.get("start_issue") or str(target_issue),
      "block_end":rc.get("end_issue") or _issue_add(target_issue,9),
      "round_position":int(rc.get("position") or 1),
      "round_mode":"10期长码·20期结构·逆势2连变盘",
      "core23":core23,"mobile4":mobile4,
      "code_count":len(selected),
      "filter20":active_filter,
      "trend_trigger":trigger,
      "trend_switched":bool(switched),
      "trend_segment":segment,
      "trend_switches":switches,
      "forced_fill":meta.get("forced_fill",[]),
      "killed_head":active_filter.get("killed_head",""),
      "killed_wave_parity":active_filter.get("killed_wave_parity",[]),
      "head_decision":{
        "killed_head":active_filter.get("killed_head",""),
        "reason":"前20期0/4头较少者"
      },
      "ranked49":meta.get("ranked49") or [],
      "rescue28":rescue28,
      "ten_horizon":{
        "analog_samples":meta.get("analog_samples",0),
        "model_windows":perf.get("mature_windows",0) if isinstance(perf,dict) else 0,
        "model_weights":{k:(model_stats.get(k,{}).get("weight_pct",20.0)) for k in POOL_MODEL_PROFILES}
      },
      "regime":f"长码段{segment}·变盘{switches}次",
      "audit_regime":audit_regime,
      "correction_trained":0,"correction_weight_pct":0,"trend_weight_pct":0,
      "pool_weights_pct":{k:(model_stats.get(k,{}).get("weight_pct",20.0)) for k in POOL_MODEL_PROFILES}
    }

def _record_strategy_audit(target_issue,profile,selected,meta):
    ranked=meta.get("ranked49") or []
    excluded=meta.get("coldest3") or []
    killed=str(meta.get("killed_head") or "")
    regime=str(meta.get("audit_regime") or meta.get("regime") or "")
    with db_lock:
        c=connect()
        try:
            c.execute("""INSERT OR IGNORE INTO strategy_audit
              (target_issue,profile,ranked49,selected_codes,excluded_zodiacs,killed_head,regime)
              VALUES (?,?,?,?,?,?,?)""",
              (str(target_issue),str(profile),
               ",".join(str(n) for n in ranked),
               ",".join(str(n) for n in selected),
               ",".join(excluded),killed,regime))
            c.commit()
        finally:
            c.close()

def _settle_strategy_audits(issue,actual_special,actual_zodiac):
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT * FROM strategy_audit
                              WHERE target_issue=? AND settled=0""",(str(issue),)).fetchall()
            for row in rows:
                ranked=_csv_nums(row["ranked49"])
                selected=set(_csv_nums(row["selected_codes"]))
                excluded=set(_csv_text(row["excluded_zodiacs"]))
                killed=str(row["killed_head"] or "")
                rank=(ranked.index(int(actual_special))+1) if int(actual_special) in ranked else 0
                hit=int(int(actual_special) in selected)
                cold_err=int(bool(actual_zodiac) and actual_zodiac in excluded)
                head_success=None
                if killed:
                    head_success=int(head_of(int(actual_special))!=killed)

                if hit:
                    reason="命中"
                elif cold_err:
                    reason="冷三肖误杀"
                elif killed and head_success==0:
                    reason="杀头误杀"
                elif rank and rank<=20 and row["profile"]=="20码精选":
                    reason="生肖配额挤出"
                elif rank and rank<=27:
                    reason="截断边缘"
                else:
                    reason="底层排序"

                c.execute("""UPDATE strategy_audit SET
                  settled=1,actual_special=?,actual_zodiac=?,actual_rank=?,
                  selected_hit=?,cold_zodiac_error=?,head_kill_success=?,failure_reason=?
                  WHERE target_issue=? AND profile=?""",
                  (int(actual_special),str(actual_zodiac or ""),int(rank),hit,cold_err,
                   head_success,reason,str(issue),str(row["profile"])))
            c.commit()
        finally:
            c.close()

def _strategy_diagnostics(profile,window=60):
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT * FROM strategy_audit
                              WHERE profile=? AND settled=1
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT ?""",(profile,int(window))).fetchall()
        finally:
            c.close()
    buckets={"1-10":0,"11-20":0,"21-27":0,"28+":0}
    reasons=Counter()
    cold_errors=0
    kill_n=kill_success=0
    kill_by_head={"0头":{"n":0,"hits":0},"4头":{"n":0,"hits":0}}
    for x in rows:
        rank=int(x["actual_rank"] or 0)
        if 1<=rank<=10: buckets["1-10"]+=1
        elif 11<=rank<=20: buckets["11-20"]+=1
        elif 21<=rank<=27: buckets["21-27"]+=1
        else: buckets["28+"]+=1
        reasons[str(x["failure_reason"] or "")]+=1
        cold_errors+=int(x["cold_zodiac_error"] or 0)
        if x["head_kill_success"] is not None:
            kill_n+=1
            hs=int(x["head_kill_success"] or 0)
            kill_success+=hs
            kh=str(x["killed_head"] or "")
            if kh in kill_by_head:
                kill_by_head[kh]["n"]+=1
                kill_by_head[kh]["hits"]+=hs
    for kh,v in kill_by_head.items():
        v["rate"]=round(100*v["hits"]/v["n"],1) if v["n"] else 0.0
    n=len(rows)
    return {
      "n":n,
      "rank_buckets":buckets,
      "cold_zodiac_errors":cold_errors,
      "cold_zodiac_error_rate":round(100*cold_errors/n,1) if n else 0.0,
      "head_kill_n":kill_n,
      "head_kill_success":kill_success,
      "head_kill_rate":round(100*kill_success/kill_n,1) if kill_n else 0.0,
      "head_kill_by_head":kill_by_head,
      "failure_reasons":dict(reasons)
    }

def _profile_hit_stats(profile,window=60):
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT hit24 FROM prediction_log
                              WHERE profile=? AND settled=1
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT ?""",(profile,int(window))).fetchall()
        finally:
            c.close()
    n=len(rows)
    hits=sum(int(x["hit24"] or 0) for x in rows)
    return {"n":n,"hits":hits,"rate":round(100*hits/n,1) if n else 0.0}

def _stats27_blocks():
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT target_issue,hit24,settled,special24
                              FROM prediction_log
                              WHERE profile=?
                              ORDER BY CAST(target_issue AS INTEGER) ASC""",
                           (TEN27_PROFILE,)).fetchall()
        finally:
            c.close()

    groups=[]
    for i in range(0,len(rows),10):
        chunk=rows[i:i+10]
        if not chunk:
            continue
        settled=[x for x in chunk if int(x["settled"] or 0)==1]
        hits=sum(int(x["hit24"] or 0) for x in settled)
        start=str(chunk[0]["target_issue"])
        groups.append({
          "start":start,
          "end":_issue_add(start,9) if start else "",
          "n":len(settled),
          "hits":hits,
          "base_count":len(_csv_nums(chunk[0]["special24"])) if chunk else 0
        })

    current=groups[-1] if groups else {"start":0,"end":0,"n":0,"hits":0,"base_count":0}
    complete=next((g for g in reversed(groups) if g["n"]>=10),None)
    return {
      "current":current,
      "last_complete":complete,
      "overall":_profile_hit_stats(TEN27_PROFILE,60),
      "rounds_completed":sum(1 for g in groups if g["n"]>=10)
    }

def _candidate24_complement_by_zodiac(r, profile):
    """12肖×2码 complement mode.

    Slot A in every zodiac = consensus/stability slot.
    Slot B = a deliberately independent AI-side or trend-side seat.
    The number of AI-side vs trend-side second seats is learned from honest
    AI-only vs trend-only forward hits instead of a simple global percentage.
    """
    zmap=_number_zodiac_map(r)
    ctx=_strategy_context(r)
    ns,transition_meta,ztrans=_predictive_number_scores(r,profile,ctx)
    pair_bonus=_within_zodiac_pair_bonus(r,zmap)

    with ai_lock:
        ai_ready=bool(ai_state.get("ready",False))
        ai_trained=int(ai_state.get("trained",0))
    if ai_ready:
        _X,_lg,ai_probs=_ai_logits_and_probs(r)
        ai_norm=_normalize_ai_probs(ai_probs)
    else:
        ai_norm={n:.5 for n in range(1,50)}

    comp=complement_matrix(60, profile)
    target_ai_slots=int(comp.get("ai_second_slots",6))

    pools={z:[] for z in ALL_ZODIACS}
    for n in range(1,50):
        z=zmap.get(n)
        if z in pools:
            pools[z].append(n)

    prepared=[]
    for z in ALL_ZODIACS:
        pool=pools[z]
        trend_norm=_norm_pool(ns,pool)
        pair_norm=_norm_pool(pair_bonus,pool)

        trend_side={}
        ai_side={}
        consensus={}
        for n in pool:
            cold=.06*ctx["num_cold"].get(n,0) if ctx["cold_rebound_now"] else 0.0
            t=.70*trend_norm.get(n,.5)+.30*pair_norm.get(n,.5)+cold
            a=.82*ai_norm.get(n,.5)+.18*pair_norm.get(n,.5)+cold
            agree=max(0.0,1.0-abs(t-a))
            c=.44*t+.44*a+.12*pair_norm.get(n,.5)+.08*agree
            trend_side[n]=t
            ai_side[n]=a
            consensus[n]=c

        stable=max(pool,key=lambda n:(consensus.get(n,-1e9),trend_side.get(n,-1e9),-n))
        rem=[n for n in pool if n!=stable]
        if rem:
            ai_pick=max(rem,key=lambda n:(ai_side.get(n,-1e9), ai_side.get(n,0)-trend_side.get(n,0), -n))
            trend_pick=max(rem,key=lambda n:(trend_side.get(n,-1e9), trend_side.get(n,0)-ai_side.get(n,0), -n))
        else:
            ai_pick=trend_pick=stable

        ai_value=ai_side.get(ai_pick,0.0)+.32*max(0.0,ai_side.get(ai_pick,0.0)-trend_side.get(ai_pick,0.0))
        trend_value=trend_side.get(trend_pick,0.0)+.32*max(0.0,trend_side.get(trend_pick,0.0)-ai_side.get(trend_pick,0.0))
        prepared.append({
          "zodiac":z,"stable":stable,
          "ai_pick":ai_pick,"trend_pick":trend_pick,
          "margin":ai_value-trend_value,
          "consensus":consensus,
          "trend_side":trend_side,
          "ai_side":ai_side
        })

    # Allocate independent seats globally, so neither model silently swallows the other.
    differing=[x for x in prepared if x["ai_pick"]!=x["trend_pick"]]
    differing_sorted=sorted(differing,key=lambda x:(-x["margin"],x["zodiac"]))
    ai_zodiacs={x["zodiac"] for x in differing_sorted[:min(target_ai_slots,len(differing_sorted))]}

    groups=[]; selected=[]; used=set()
    actual_ai_slots=0; actual_trend_slots=0; shared_slots=0
    for x in prepared:
        z=x["zodiac"]; stable=x["stable"]
        if x["ai_pick"]==x["trend_pick"]:
            second=x["ai_pick"]; slot_type="共识补位"; shared_slots+=1
        elif z in ai_zodiacs:
            second=x["ai_pick"]; slot_type="AI补位"; actual_ai_slots+=1
        else:
            second=x["trend_pick"]; slot_type="趋势补位"; actual_trend_slots+=1

        codes=[stable]
        if second!=stable:
            codes.append(second)
        # Extremely defensive fallback for a malformed zodiac pool.
        if len(codes)<2:
            for n in pools[z]:
                if n not in codes:
                    codes.append(n)
                if len(codes)>=2: break

        groups.append({"zodiac":z,"codes":codes[:2],"stable":stable,"second_type":slot_type})
        for n in codes[:2]:
            if n not in used:
                selected.append(n); used.add(n)

    if len(selected)<24:
        global_rank=sorted(range(1,50),key=lambda n:(-ns.get(n,-1e9),n))
        for n in global_rank:
            if n not in used:
                selected.append(n); used.add(n)
            if len(selected)>=24: break

    meta=dict(transition_meta)
    meta.update({
      "ai_ready":ai_ready,
      "ai_trained":ai_trained,
      "complement":comp,
      "actual_ai_second_slots":actual_ai_slots,
      "actual_trend_second_slots":actual_trend_slots,
      "shared_second_slots":shared_slots
    })
    return selected[:24],groups,ns,_zodiac_scores_profile(r,profile),ctx,meta,ztrans

def _predict_complement_with_profile(r, profile):
    cand24,groups,ns,zs,ctx,tmeta,ztrans=_candidate24_complement_by_zodiac(r,profile)
    # Keep 4肖 selection logic, but its code is now selected from the two
    # complement-aware codes of that zodiac.
    z4,zpairs=_dynamic_zodiac4_one_code(r,profile,groups,ns,zs,ctx,ztrans)
    main4=[p["code"] for p in zpairs]
    return cand24,main4,z4,groups,zpairs

def _candidate24_by_zodiac(r, profile):
    """Exactly 24 = 12 zodiacs × 2 codes.
    Hybrid ranking: statistical forecast + stabilized zodiac pair history + online AI."""
    zmap=_number_zodiac_map(r)
    ctx=_strategy_context(r)
    ns,transition_meta,ztrans=_predictive_number_scores(r,profile,ctx)
    pair_bonus=_within_zodiac_pair_bonus(r,zmap)

    with ai_lock:
        ai_trained=int(ai_state["trained"])
        ai_ready=bool(ai_state["ready"])
    if ai_ready:
        _X,_lg,ai_probs=_ai_logits_and_probs(r)
        ai_norm=_normalize_ai_probs(ai_probs)
        fusion=get_dynamic_ai_mix()
        ai_mix=float(fusion.get("mix_pct",35.0))/100.0
    else:
        ai_norm={n:.5 for n in range(1,50)}
        ai_mix=0.0

    pools={z:[] for z in ALL_ZODIACS}
    for n in range(1,50):
        z=zmap.get(n)
        if z in pools:
            pools[z].append(n)

    groups=[]; selected=[]; used=set()
    for z in ALL_ZODIACS:
        pool=pools[z]
        ns_norm=_norm_pool(ns,pool)
        pair_norm=_norm_pool(pair_bonus,pool)
        combined={}
        for n in pool:
            base=.66*ns_norm.get(n,.5)+.34*pair_norm.get(n,.5)
            combined[n]=(1-ai_mix)*base + ai_mix*ai_norm.get(n,.5)
            if ctx["cold_rebound_now"]:
                combined[n]+=.06*ctx["num_cold"].get(n,0)

        ranked=sorted(pool,key=lambda n:(-combined.get(n,-1e9),-ns.get(n,-1e9),n))
        codes=ranked[:2]
        groups.append({"zodiac":z,"codes":codes})
        for n in codes:
            if n not in used:
                selected.append(n); used.add(n)

    if len(selected)<24:
        for n in sorted(range(1,50),key=lambda n:(-ns[n],n)):
            if n not in used:
                selected.append(n); used.add(n)
            if len(selected)>=24:
                break

    transition_meta=dict(transition_meta)
    transition_meta["ai_ready"]=ai_ready
    transition_meta["ai_trained"]=ai_trained
    transition_meta["ai_mix_pct"]=round(ai_mix*100,1)

    return selected[:24],groups,ns,_zodiac_scores_profile(r,profile),ctx,transition_meta,ztrans

def _dynamic_zodiac4_one_code(r, profile, groups, ns, zs, ctx, ztrans):
    """Predict 4 zodiacs for the NEXT issue using transition + trend, one code each."""
    if not r:
        return [],[]

    prev_zs=_zodiac_scores_profile(r[1:],profile) if len(r)>1 else {}
    latest_z=normalize_z(r[0]["z7"] or "")

    _ln,long_z,long_ready=_long_prior_bonus(r)
    ai_zmass=_ai_zodiac_mass(r)
    fusion=get_dynamic_ai_mix()
    ai_z_weight=float(fusion.get("mix_pct",35.0))/100.0
    adjusted={}
    for z in ALL_ZODIACS:
        momentum=zs.get(z,0)-prev_zs.get(z,0)
        adjusted[z]=zs.get(z,0)+1.05*momentum
        adjusted[z]+=1.50*ztrans[z]
        adjusted[z]+=2.20*ai_z_weight*ai_zmass.get(z,0.0)
        if long_ready:
            adjusted[z]+=.55*long_z[z]
        adjusted[z]+=(.72 if ctx["cold_rebound_now"] else .12)*ctx["z_cold"].get(z,0)

        # Small anti-chase cooldown. Historical transition can overcome it.
        if z==latest_z:
            adjusted[z]-=.12*max(abs(zs.get(z,0)),1.0)

    z4=sorted(ALL_ZODIACS,key=lambda z:(-adjusted.get(z,-1e9),z))[:4]
    groupmap={g["zodiac"]:g["codes"] for g in groups}
    pairs=[]
    for z in z4:
        codes=groupmap.get(z,[])
        if codes:
            code=max(codes,key=lambda n:(ns.get(n,-1e9),-n))
            pairs.append({"zodiac":z,"code":code})
    return z4,pairs

def _predict_with_profile(r, profile):
    cand24,groups,ns,zs,ctx,tmeta,ztrans=_candidate24_by_zodiac(r,profile)
    z4,zpairs=_dynamic_zodiac4_one_code(r,profile,groups,ns,zs,ctx,ztrans)
    main4=[p["code"] for p in zpairs]
    return cand24,main4,z4,groups,zpairs

def _draw_zodiac_set(x):
    return {normalize_z(x[f"z{k}"] or "") for k in range(1,8) if x[f"z{k}"]}

def _pingte_yixiao_scores(r):
    """Predict one zodiac that will appear anywhere among the next draw's 7 numbers."""
    score={z:0.0 for z in ALL_ZODIACS}
    if not r:
        return score,{"samples":0}

    # Base presence trend. Binary per draw: a zodiac counts once even if repeated.
    hist=r[1:] if len(r)>1 else r
    for horizon,half,coef in [(8,3,1.80),(16,5,1.45),(36,11,1.00),(80,25,.62),(160,50,.35)]:
        for i,x in enumerate(hist[:min(horizon,len(hist))]):
            w=coef*exp_weight(i,half)
            for z in _draw_zodiac_set(x):
                if z in score:
                    score[z]+=w

    # Presence acceleration.
    a=Counter()
    b=Counter()
    for x in hist[:8]:
        for z in _draw_zodiac_set(x): a[z]+=1
    for x in hist[8:32]:
        for z in _draw_zodiac_set(x): b[z]+=1
    for z in ALL_ZODIACS:
        score[z]+=1.25*(a[z]/8.0-b[z]/24.0)

    # Current-state -> next-draw zodiac-presence transition.
    cur=r[0]
    cur_special_z=normalize_z(cur["z7"] or "")
    cur_set=_draw_zodiac_set(cur)
    cur_head=head_of(cur["special"])
    cur_wave=wave_of(cur["special"])
    cur_size=size_of(cur["special"])
    cur_parity=parity_of(cur["special"])

    trans=defaultdict(float)
    samples=0
    for j in range(1,min(len(r)-1,700)):
        state=r[j]
        outcome=r[j-1]
        match=0.0
        if normalize_z(state["z7"] or "")==cur_special_z: match+=1.70
        if head_of(state["special"])==cur_head: match+=.62
        if wave_of(state["special"])==cur_wave: match+=.48
        if size_of(state["special"])==cur_size: match+=.38
        if parity_of(state["special"])==cur_parity: match+=.34
        stset=_draw_zodiac_set(state)
        if cur_set and stset:
            match+=.90*(len(cur_set & stset)/max(1,len(cur_set | stset)))
        if match<=0: continue
        samples+=1
        ww=exp_weight(j,280)*match
        for z in _draw_zodiac_set(outcome):
            if z in score:
                trans[z]+=ww

    if trans:
        vals=list(trans.values()); lo=min(vals); hi=max(vals)
        if hi-lo>1e-9:
            for z in ALL_ZODIACS:
                score[z]+=1.85*((trans[z]-lo)/(hi-lo))
        else:
            for z in trans:
                score[z]+=.90

    return score,{"samples":samples}

def _predict_pingte_yixiao(r):
    scores,meta=_pingte_yixiao_scores(r)
    _ln,long_z,long_ready=_long_prior_bonus(r)
    ai_zmass=_ai_zodiac_mass(r)
    fusion=get_dynamic_ai_mix()
    ai_z_weight=float(fusion.get("mix_pct",35.0))/100.0
    for z in ALL_ZODIACS:
        if long_ready:
            scores[z]+=.45*long_z[z]
        # AI has been explicitly trained on 7-position zodiac presence,
        # so its learned zodiac mass also participates in 平特一肖.
        scores[z]+=2.00*ai_z_weight*ai_zmass.get(z,0.0)
    ranked=sorted(ALL_ZODIACS,key=lambda z:(-scores.get(z,-1e9),z))
    one=ranked[0] if ranked else ""
    return one,meta


def _csv_nums(text):
    out=[]
    for x in str(text or "").split(","):
        x=x.strip()
        if x.isdigit():
            out.append(int(x))
    return out

def _csv_text(text):
    return [x for x in str(text or "").split(",") if x]

def refresh_learner_cache(window=60):
    """Use only forecasts that were saved before their outcomes were known."""
    result={}
    with db_lock:
        c=connect()
        try:
            for profile in PROFILE_LIBRARY:
                rows=c.execute("""SELECT hit24,hitmain,hitz,hitping
                                  FROM prediction_log
                                  WHERE profile=? AND settled=1
                                  ORDER BY CAST(target_issue AS INTEGER) DESC
                                  LIMIT ?""",(profile,window)).fetchall()
                n=len(rows)
                h24=sum(int(x["hit24"] or 0) for x in rows)
                hm=sum(int(x["hitmain"] or 0) for x in rows)
                hz=sum(int(x["hitz"] or 0) for x in rows)
                hp=sum(int(x["hitping"] or 0) for x in rows)

                # Bayesian smoothing: avoid 0/100% overreaction with small n.
                r24=(h24 + 4*(24/49)) / (n+4)
                rm=(hm + 4*(4/49)) / (n+4)
                rz=(hz + 4*(4/12)) / (n+4)
                rp=(hp + 4*.50) / (n+4)

                # Special 24-code hit is the dominant learning target.
                score=.88*r24 + .03*rm + .05*rz + .04*rp
                weight=math.exp(5.0*(score-.49))
                result[profile]={
                    "n":n,
                    "hit24":h24,"hitmain":hm,"hitz":hz,"hitping":hp,
                    "rate24":round(h24/n*100,1) if n else 0.0,
                    "rateMain":round(hm/n*100,1) if n else 0.0,
                    "rateZ":round(hz/n*100,1) if n else 0.0,
                    "ratePing":round(hp/n*100,1) if n else 0.0,
                    "score":score,
                    "weight":weight
                }
                c.execute("""INSERT INTO learner_scores
                  (profile,weight,n,hit24,hitmain,hitz,hitping,score,updated_at)
                  VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                  ON CONFLICT(profile) DO UPDATE SET
                    weight=excluded.weight,n=excluded.n,hit24=excluded.hit24,
                    hitmain=excluded.hitmain,hitz=excluded.hitz,hitping=excluded.hitping,
                    score=excluded.score,updated_at=CURRENT_TIMESTAMP""",
                    (profile,weight,n,h24,hm,hz,hp,score))
            c.commit()
        finally:
            c.close()

    best=max(PROFILE_LIBRARY.keys(),
             key=lambda p:(result[p]["score"],result[p]["rate24"],p)) if result else "平衡"
    settled=max((v["n"] for v in result.values()),default=0)
    with learner_lock:
        learner_cache.update({
            "ready": settled > 0,
            "best_profile": best,
            "profiles": result,
            "settled": settled,
            "window": window,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
        })
    return best,result

def settle_predictions(issue, nums, zs):
    """When issue X arrives, grade forecasts that had already been saved for X."""
    actual_special=int(nums[6])
    actual_z=normalize_z(zs[6] if len(zs)>=7 else "")
    draw_z={normalize_z(z) for z in zs if z}
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT * FROM prediction_log
                              WHERE target_issue=? AND settled=0""",(str(issue),)).fetchall()
            for row in rows:
                s24=set(_csv_nums(row["special24"]))
                m4=set(_csv_nums(row["main4"]))
                z4=set(_csv_text(row["zodiac4"]))
                py=normalize_z(row["pingte"] or "")
                c.execute("""UPDATE prediction_log SET
                    settled=1, hit24=?, hitmain=?, hitz=?, hitping=?,
                    actual_special=?, actual_zodiac=?
                    WHERE target_issue=? AND profile=?""",
                    (int(actual_special in s24),
                     int(actual_special in m4),
                     int(bool(actual_z) and actual_z in z4),
                     int(bool(py) and py in draw_z),
                     actual_special, actual_z, str(issue), row["profile"]))
            c.commit()
        finally:
            c.close()

    try:
        _settle_strategy_audits(issue,actual_special,actual_z)
    except Exception as e:
        print(f"[AUDIT] settle failed: {e}",flush=True)

    if rows:
        with pool_perf_lock:
            pool_perf_cache["ts"]=0.0
            pool_perf_cache["data"]=None
        with ten27_perf_lock:
            ten27_perf_cache["ts"]=0.0
            ten27_perf_cache["data"]=None
        best,_=refresh_learner_cache(60)
        try:
            refresh_dynamic_ai_mix()
        except Exception as e:
            print(f"[FUSION] refresh failed: {e}",flush=True)
        print(f"[LEARN] settled issue={issue} models={len(rows)} best={best}",flush=True)

def record_shadow_predictions(r):
    """Save all model forecasts for the NEXT issue. Never rewrites a saved forecast."""
    if not r:
        return
    try:
        target=_next_issue_id(r[0]["issue"])
    except Exception:
        return

    t0=time.time()
    py,_=_predict_pingte_yixiao(r)
    records=[]
    for profile in PROFILE_LIBRARY:
        try:
            c24,m4,z4,_,_=_predict_with_profile(r,profile)
            records.append((
                target, profile,
                ",".join(str(n) for n in c24),
                ",".join(str(n) for n in m4),
                ",".join(z4),
                py
            ))
        except Exception as e:
            print(f"[LEARN] shadow {profile} failed: {type(e).__name__}: {e}",flush=True)

    # Keep the legacy AI-hybrid forecast exactly as before so the existing
    # 41+ real validation samples remain comparable.
    try:
        best_profile,_=_select_profile(r)
        c24,m4,z4,_,_=_predict_with_profile(r,best_profile)
        records.append((
            target,"AI在线",
            ",".join(str(n) for n in c24),
            ",".join(str(n) for n in m4),
            ",".join(z4),
            py
        ))
    except Exception as e:
        print(f"[AI] locked prediction failed: {type(e).__name__}: {e}",flush=True)

    # New final complement forecast gets its own profile and starts honest
    # forward validation from this version onward. Old samples are never faked.
    try:
        best_profile,_=_select_profile(r)
        c24,m4,z4,_,_=_predict_complement_with_profile(r,best_profile)
        records.append((
            target,"互补在线",
            ",".join(str(n) for n in c24),
            ",".join(str(n) for n in m4),
            ",".join(z4),
            py
        ))
    except Exception as e:
        print(f"[COMP] locked prediction failed: {type(e).__name__}: {e}",flush=True)

    try:
        best_profile,_=_select_profile(r)
        c20,_m20=_predict20_hot(r,best_profile)
        records.append((
            target,"20码精选",
            ",".join(str(n) for n in c20),
            "","",py
        ))
        _record_strategy_audit(target,"20码精选",c20,_m20)
    except Exception as e:
        print(f"[20CODE] locked prediction failed: {type(e).__name__}: {e}",flush=True)

    try:
        best_profile,_=_select_profile(r)
        c27,_m27=_predict27_tenblock(r,best_profile,target)
        core23=list(_m27.get("core23") or [])[:23]
        mobile4=list(_m27.get("mobile4") or [])[:4]
        rescue28=_m27.get("rescue28") or {}
        rescue_codes=[int(rescue28["code"])] if rescue28.get("active") and rescue28.get("code") else []

        records.append((target,TEN27_V3_PROFILE,",".join(str(n) for n in c27),"","",py))
        records.append((target,TEN27_V3_CORE_PROFILE,",".join(str(n) for n in core23),"","",py))
        records.append((target,TEN27_V3_MOBILE_PROFILE,",".join(str(n) for n in mobile4),"","",py))
        records.append((target,TEN27_V3_28_PROFILE,",".join(str(n) for n in rescue_codes),"","",py))
        _record_strategy_audit(target,TEN27_V3_PROFILE,c27,_m27)
    except Exception as e:
        print(f"[27CODE] locked prediction failed: {type(e).__name__}: {e}",flush=True)

    # Parallel specialist pool: same 20-code target, different logic.
    try:
        best_profile,_=_select_profile(r)
        specialists=_specialist_model_scores(r,best_profile,"20")
        for key,prof in POOL_MODEL_PROFILES.items():
            ranked=sorted(range(1,50),key=lambda n:(-specialists[key].get(n,-1e9),n))
            codes=ranked[:20]
            records.append((target,prof,",".join(str(n) for n in codes),"","",py))
        # F = the actual final 20-code list, kept separately from 20码精选 for
        # easy T/Z/C/W/A/F table comparisons.
        c20f,_mf=_predict20_hot(r,best_profile)
        records.append((target,POOL_FINAL_PROFILE,",".join(str(n) for n in c20f),"","",py))
    except Exception as e:
        print(f"[POOL] locked specialist models failed: {type(e).__name__}: {e}",flush=True)

    # High-coverage stable signals: these are also locked pre-draw and settled
    # through the same prediction_log, so streaks cannot be backfilled.
    try:
        best_profile,_=_select_profile(r)
        stable=_stable_signal_predictions(r,best_profile)
        for prof,codes in stable.items():
            records.append((target,prof,",".join(str(n) for n in codes),"","",py))
    except Exception as e:
        print(f"[STABLE] locked signals failed: {type(e).__name__}: {e}",flush=True)

    if records:
        with db_lock:
            c=connect()
            try:
                c.executemany("""INSERT OR IGNORE INTO prediction_log
                    (target_issue,profile,special24,main4,zodiac4,pingte)
                    VALUES (?,?,?,?,?,?)""",records)
                c.commit()
            finally:
                c.close()
        print(f"[LEARN] recorded target={target} models={len(records)} in {time.time()-t0:.2f}s",flush=True)
        # This is the safest point to back up: the next issue's predictions
        # have already been locked before the outcome exists.
        threading.Thread(target=write_checkpoint_atomic,daemon=True,name="learning-checkpoint").start()

def learner_validation_stats():
    with learner_lock:
        best=learner_cache.get("best_profile","平衡")
        info=dict((learner_cache.get("profiles",{}).get(best,{}) or {}))
        n=int(info.get("n",0))
    if not info:
        return {
          "n":0,"target":60,"profile":best,"building":False,"learning":True,
          "hit24":0.0,"err24":0.0,
          "hitMain":0.0,"errMain":0.0,
          "hitZ":0.0,"errZ":0.0,
          "hitPingte":0.0,"errPingte":0.0
        }
    return {
      "n":n,"target":60,"profile":best,"building":False,"learning":True,
      "hit24":info.get("rate24",0.0),"err24":round(100-info.get("rate24",0.0),1),
      "hitMain":info.get("rateMain",0.0),"errMain":round(100-info.get("rateMain",0.0),1),
      "hitZ":info.get("rateZ",0.0),"errZ":round(100-info.get("rateZ",0.0),1),
      "hitPingte":info.get("ratePing",0.0),"errPingte":round(100-info.get("ratePing",0.0),1)
    }

def export_learning_payload(limit=1000):
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT * FROM prediction_log
                              ORDER BY CAST(target_issue AS INTEGER) DESC, profile
                              LIMIT ?""",(int(limit),)).fetchall()
            airow=c.execute("SELECT * FROM ai_model WHERE id=1").fetchone()
            audits=c.execute("""SELECT * FROM strategy_audit
                                ORDER BY CAST(target_issue AS INTEGER) DESC,profile
                                LIMIT 1000""").fetchall()
            corrections=c.execute("SELECT * FROM correction_model ORDER BY strategy").fetchall()
            return {
              "logs":[{k:x[k] for k in x.keys()} for x in rows],
              "ai":({k:airow[k] for k in airow.keys()} if airow else None),
              "audits":[{k:x[k] for k in x.keys()} for x in audits],
              "correction":[{k:x[k] for k in x.keys()} for x in corrections]
            }
        finally:
            c.close()



def _supabase_headers(content_type=None):
    key=SUPABASE_SERVICE_ROLE_KEY
    h={
      "apikey": key,
      "Cache-Control": "no-cache"
    }
    if key and not key.startswith("sb_secret_"):
        h["Authorization"]=f"Bearer {key}"
    if content_type:
        h["Content-Type"]=content_type
    return h

def upload_checkpoint_to_supabase():
    if not REMOTE_BACKUP_ENABLED or not os.path.exists(CHECKPOINT_PATH):
        return False
    try:
        with open(CHECKPOINT_PATH,"rb") as f:
            data=f.read()
        url=f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_OBJECT}"
        h=_supabase_headers("application/gzip")
        h["x-upsert"]="true"
        r=requests.post(url,headers=h,data=data,timeout=25)
        if r.status_code not in (200,201):
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:220]}")
        auto_state["remote_backup_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
        auto_state["remote_backup_error"]=""
        print(f"[REMOTE] Supabase backup OK bytes={len(data)}",flush=True)
        return True
    except Exception as e:
        auto_state["remote_backup_error"]=f"{type(e).__name__}: {e}"
        print(f"[REMOTE] backup failed: {auto_state['remote_backup_error']}",flush=True)
        return False

def download_checkpoint_from_supabase():
    if not REMOTE_BACKUP_ENABLED:
        return False
    try:
        url=f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_OBJECT}"
        r=requests.get(url,headers=_supabase_headers(),timeout=25)
        if r.status_code==404:
            print("[REMOTE] no checkpoint yet",flush=True)
            return False
        if r.status_code!=200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:220]}")
        parent=os.path.dirname(os.path.abspath(CHECKPOINT_PATH))
        os.makedirs(parent,exist_ok=True)
        tmp=CHECKPOINT_PATH+".remote.tmp"
        with open(tmp,"wb") as f:
            f.write(r.content)
        with gzip.open(tmp,"rt",encoding="utf-8") as f:
            payload=json.load(f)
        if not isinstance(payload,dict) or "learning" not in payload:
            raise RuntimeError("invalid remote checkpoint")
        os.replace(tmp,CHECKPOINT_PATH)
        auto_state["remote_restore_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
        auto_state["remote_backup_error"]=""
        print(f"[REMOTE] Supabase restore OK bytes={len(r.content)}",flush=True)
        return True
    except Exception as e:
        auto_state["remote_backup_error"]=f"restore {type(e).__name__}: {e}"
        print(f"[REMOTE] restore failed: {auto_state['remote_backup_error']}",flush=True)
        return False

def _checkpoint_payload():
    learning=export_learning_payload(1500)
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT * FROM draws
                              ORDER BY CAST(issue AS INTEGER) DESC
                              LIMIT 1400""").fetchall()
            scores=c.execute("SELECT * FROM learner_scores ORDER BY profile").fetchall()
        finally:
            c.close()
    return {
      "version":"v41",
      "created_at":time.strftime("%Y-%m-%d %H:%M:%S"),
      "persistent_mode":PERSISTENT_MODE,
      "learning":learning,
      "draws":[{k:r[k] for k in r.keys()} for r in rows],
      "learner_scores":[{k:r[k] for k in r.keys()} for r in scores]
    }

def write_checkpoint_atomic():
    """Atomic learning checkpoint. On a persistent disk this survives deploy/restart."""
    try:
        parent=os.path.dirname(os.path.abspath(CHECKPOINT_PATH))
        os.makedirs(parent,exist_ok=True)
        payload=_checkpoint_payload()
        tmp=CHECKPOINT_PATH+".tmp"
        with gzip.open(tmp,"wt",encoding="utf-8") as f:
            json.dump(payload,f,ensure_ascii=False,separators=(",",":"))
        os.replace(tmp,CHECKPOINT_PATH)
        auto_state["checkpoint_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
        auto_state["checkpoint_error"]=""
        if REMOTE_BACKUP_ENABLED:
            upload_checkpoint_to_supabase()
        return True
    except Exception as e:
        auto_state["checkpoint_error"]=f"{type(e).__name__}: {e}"
        print(f"[BACKUP] checkpoint failed: {auto_state['checkpoint_error']}",flush=True)
        return False

def _restore_draw_dicts(items):
    if not items:
        return 0
    cols=[
      "issue","n1","n2","n3","n4","n5","n6","special",
      "z1","z2","z3","z4","z5","z6","z7",
      "c1","c2","c3","c4","c5","c6","c7","raw","created_at"
    ]
    sql=f"""INSERT OR IGNORE INTO draws({','.join(cols)})
             VALUES ({','.join(['?']*len(cols))})"""
    inserted=0
    with db_lock:
        c=connect()
        try:
            for x in items:
                before=c.total_changes
                vals=[x.get(col) for col in cols]
                c.execute(sql,vals)
                inserted += int(c.total_changes>before)
            c.commit()
        finally:
            c.close()
    return inserted

def restore_checkpoint_if_better():
    """Restore automatically when checkpoint contains more learning history than DB."""
    if not os.path.exists(CHECKPOINT_PATH):
        return 0
    try:
        with gzip.open(CHECKPOINT_PATH,"rt",encoding="utf-8") as f:
            payload=json.load(f)
        learning=payload.get("learning",{}) or {}
        ck_logs=learning.get("logs",[]) or []

        with db_lock:
            c=connect()
            try:
                current_logs=int(c.execute("SELECT COUNT(*) AS n FROM prediction_log").fetchone()["n"])
            finally:
                c.close()

        restored=0
        restored += import_learning_payload(learning)
        restored += _restore_draw_dicts(payload.get("draws",[]) or [])
        load_ai_state()
        load_correction_state()
        refresh_learner_cache(60)
        print(f"[BACKUP] restored checkpoint logs={len(ck_logs)} inserted={restored}",flush=True)
        return restored
    except Exception as e:
        print(f"[BACKUP] restore failed: {type(e).__name__}: {e}",flush=True)
        return 0

def import_learning_payload(payload):
    logs=payload.get("logs",[]) if isinstance(payload,dict) else []
    ai_payload=payload.get("ai") if isinstance(payload,dict) else None
    audits=payload.get("audits",[]) if isinstance(payload,dict) else []
    correction_payload=payload.get("correction",[]) if isinstance(payload,dict) else []
    inserted=0
    with db_lock:
        c=connect()
        try:
            for x in logs:
                before=c.total_changes
                c.execute("""INSERT OR IGNORE INTO prediction_log
                  (target_issue,profile,special24,main4,zodiac4,pingte,created_at,
                   settled,hit24,hitmain,hitz,hitping,actual_special,actual_zodiac)
                  VALUES (?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP),
                          ?,?,?,?,?,?,?)""",
                  (str(x.get("target_issue","")),str(x.get("profile","")),
                   str(x.get("special24","")),str(x.get("main4","")),
                   str(x.get("zodiac4","")),str(x.get("pingte","")),
                   str(x.get("created_at","")),int(x.get("settled") or 0),
                   x.get("hit24"),x.get("hitmain"),x.get("hitz"),x.get("hitping"),
                   x.get("actual_special"),str(x.get("actual_zodiac") or "")))
                inserted += int(c.total_changes>before)
            c.commit()
        finally:
            c.close()
    if ai_payload:
        try:
            weights=str(ai_payload.get("weights") or "[]")
            parsed=json.loads(weights)
            if len(parsed)<len(AI_FEATURES):
                parsed=list(parsed)+[0.0]*(len(AI_FEATURES)-len(parsed))
                weights=json.dumps(parsed,separators=(",",":"))
            elif len(parsed)>len(AI_FEATURES):
                parsed=list(parsed)[:len(AI_FEATURES)]
                weights=json.dumps(parsed,separators=(",",":"))
            if len(parsed)==len(AI_FEATURES):
                with db_lock:
                    c=connect()
                    try:
                        c.execute("""UPDATE ai_model SET weights=?,steps=?,trained=?,
                                     last_issue=?,lr=?,historical_validation_n=?,
                                     historical_hit24=?,rolling_window=?,rolling_trained=?,
                                     refit_count=?,last_refit_seconds=?,
                                     updated_at=CURRENT_TIMESTAMP WHERE id=1""",
                                  (weights,int(ai_payload.get("steps") or 0),
                                   int(ai_payload.get("trained") or 0),
                                   str(ai_payload.get("last_issue") or ""),
                                   float(ai_payload.get("lr") or .08),
                                   int(ai_payload.get("historical_validation_n") or 0),
                                   float(ai_payload.get("historical_hit24") or 0.0),
                                   int(ai_payload.get("rolling_window") or 100),
                                   int(ai_payload.get("rolling_trained") or 0),
                                   int(ai_payload.get("refit_count") or 0),
                                   float(ai_payload.get("last_refit_seconds") or 0.0)))
                        c.commit()
                    finally:
                        c.close()
                load_ai_state()
        except Exception as e:
            print(f"[AI] inherited state failed: {e}",flush=True)
    if audits:
        with db_lock:
            c=connect()
            try:
                for x in audits:
                    c.execute("""INSERT INTO strategy_audit
                      (target_issue,profile,ranked49,selected_codes,excluded_zodiacs,
                       killed_head,regime,created_at,settled,actual_special,actual_zodiac,
                       actual_rank,selected_hit,cold_zodiac_error,head_kill_success,failure_reason)
                      VALUES (?,?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP),
                              ?,?,?,?,?,?,?,?)
                      ON CONFLICT(target_issue,profile) DO UPDATE SET
                        ranked49=excluded.ranked49,
                        selected_codes=excluded.selected_codes,
                        excluded_zodiacs=excluded.excluded_zodiacs,
                        killed_head=excluded.killed_head,
                        regime=excluded.regime,
                        settled=MAX(strategy_audit.settled,excluded.settled),
                        actual_special=COALESCE(excluded.actual_special,strategy_audit.actual_special),
                        actual_zodiac=COALESCE(NULLIF(excluded.actual_zodiac,''),strategy_audit.actual_zodiac),
                        actual_rank=COALESCE(excluded.actual_rank,strategy_audit.actual_rank),
                        selected_hit=COALESCE(excluded.selected_hit,strategy_audit.selected_hit),
                        cold_zodiac_error=COALESCE(excluded.cold_zodiac_error,strategy_audit.cold_zodiac_error),
                        head_kill_success=COALESCE(excluded.head_kill_success,strategy_audit.head_kill_success),
                        failure_reason=COALESCE(NULLIF(excluded.failure_reason,''),strategy_audit.failure_reason)""",
                      (str(x.get("target_issue","")),str(x.get("profile","")),
                       str(x.get("ranked49","")),str(x.get("selected_codes","")),
                       str(x.get("excluded_zodiacs","")),str(x.get("killed_head","")),
                       str(x.get("regime","")),str(x.get("created_at","")),
                       int(x.get("settled") or 0),x.get("actual_special"),
                       str(x.get("actual_zodiac") or ""),x.get("actual_rank"),
                       x.get("selected_hit"),x.get("cold_zodiac_error"),
                       x.get("head_kill_success"),str(x.get("failure_reason") or "")))
                c.commit()
            finally:
                c.close()

    if correction_payload:
        with db_lock:
            c=connect()
            try:
                for x in correction_payload:
                    strategy=str(x.get("strategy") or "")
                    if strategy not in ("20","27"):
                        continue
                    incoming=int(x.get("trained") or 0)
                    cur=c.execute("SELECT trained FROM correction_model WHERE strategy=?",(strategy,)).fetchone()
                    current=int(cur["trained"] or 0) if cur else 0
                    if incoming>=current:
                        weights=str(x.get("weights") or "[]")
                        try:
                            parsed=json.loads(weights)
                        except Exception:
                            parsed=[]
                        if len(parsed)<len(AI_FEATURES):
                            parsed=list(parsed)+[0.0]*(len(AI_FEATURES)-len(parsed))
                        elif len(parsed)>len(AI_FEATURES):
                            parsed=list(parsed)[:len(AI_FEATURES)]
                        c.execute("""INSERT INTO correction_model(strategy,weights,steps,trained,updated_at)
                                     VALUES (?,?,?,?,CURRENT_TIMESTAMP)
                                     ON CONFLICT(strategy) DO UPDATE SET
                                       weights=excluded.weights,steps=excluded.steps,
                                       trained=excluded.trained,updated_at=CURRENT_TIMESTAMP""",
                                  (strategy,json.dumps(parsed,separators=(",",":")),
                                   int(x.get("steps") or 0),incoming))
                c.commit()
            finally:
                c.close()
        load_correction_state()

    refresh_learner_cache(60)
    return inserted

def sync_previous_learning():
    urls=[]
    if WEBHOOK_BASE_URL:
        urls.append(WEBHOOK_BASE_URL+"/api/learning-export?limit=1000")
    if HISTORY_SOURCE_URL and "/api/" in HISTORY_SOURCE_URL:
        base=HISTORY_SOURCE_URL.split("/api/",1)[0]
        urls.append(base+"/api/learning-export?limit=1000")
    seen=set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        try:
            rr=requests.get(url,timeout=12,headers={"User-Agent":"SanfenLearner/26"})
            if rr.ok:
                n=import_learning_payload(rr.json())
                print(f"[LEARN] inherited rows={n} from {url}",flush=True)
                return n
        except Exception:
            pass
    return 0

def _profile_score_on_recent(r, profile, samples=28):
    if len(r)<260:
        return 0.0
    tests=min(samples,len(r)-220)
    h24=hmain=hz=0
    n=0
    for k in range(tests-1,-1,-1):
        train=r[k+1:]
        if len(train)<220: continue
        c24,m4,z4,_,_=_predict_with_profile(train,profile)
        actual=r[k]
        sp=actual["special"]
        az=normalize_z(actual["z7"] or "")
        h24+=int(sp in c24)
        hmain+=int(sp in m4)
        hz+=int(bool(az) and az in z4)
        n+=1
    if not n: return 0.0
    # User's primary objective is the 24-code special-number hit rate.
    # 4肖 remains a secondary tie-breaker; 4肖1码 no longer dominates calibration.
    return (.82*h24 + .05*hmain + .13*hz)/n

def _light_profile_calibration(r):
    """Very small calibration pass. Never blocks the web/model rebuild path."""
    if not r or background_state.get("profile_calibrating"):
        return
    background_state["profile_calibrating"]=True
    try:
        # Still background-only, but now use enough validation points to avoid
        # nonsense such as a 0%/100% score from a single issue.
        rr=r[:950]
        cal_n=10
        scores={name:_profile_score_on_recent(rr,name,cal_n) for name in PROFILE_LIBRARY}
        best=max(scores,key=lambda k:(scores[k],k))
        model_state["profile"]=best
        model_state["profile_scores"]={k:round(v*100,1) for k,v in scores.items()}
        model_state["calibration_n"]=cal_n
        model_state["last_calibrated_issue"]=r[0]["issue"] if r else None
        print(f"[CAL] lightweight profile={best} scores={model_state['profile_scores']}",flush=True)
    except Exception as e:
        print(f"[CAL] failed: {type(e).__name__}: {e}",flush=True)
    finally:
        background_state["profile_calibrating"]=False

def _select_profile(r, force=False):
    """Use continuously learned profile when enough real forward results exist."""
    with learner_lock:
        learned_n=int(learner_cache.get("settled",0))
        learned_best=learner_cache.get("best_profile","平衡")
        learned_profiles=dict(learner_cache.get("profiles",{}) or {})

    if learned_n >= 8 and learned_best in PROFILE_LIBRARY:
        scores={p:round(v.get("score",0.0)*100,1) for p,v in learned_profiles.items()}
        model_state["profile"]=learned_best
        model_state["profile_scores"]=scores
        model_state["calibration_n"]=min(learned_n,60)
        return learned_best,scores

    latest_issue=r[0]["issue"] if r else None
    cached=model_state.get("profile") or "平衡"
    last=model_state.get("last_calibrated_issue")

    due=force or not last
    if not due and latest_issue and last:
        try:
            due=abs(int(latest_issue)-int(last)) >= 18
        except Exception:
            due=False

    if due and not background_state.get("profile_calibrating"):
        threading.Thread(
            target=_light_profile_calibration,
            args=(list(r),),
            daemon=True,
            name="profile-calibration"
        ).start()

    if not model_state.get("profile"):
        model_state["profile"]="平衡"
        model_state["profile_scores"]={"平衡":0.0}
    return model_state["profile"], dict(model_state.get("profile_scores") or {})

def predict_core(r, profile=None):
    if not r:return [],[],[]
    profile=profile or _select_profile(r)[0]
    c24,m4,z4,_,_=_predict_with_profile(r,profile)
    return c24,m4,z4

def backtest_stats(r, sample=60):
    if not r or len(r)<420:
        return {"n":0,"hit24":0.0,"err24":100.0,"hitMain":0.0,"errMain":100.0,
                "hitZ":0.0,"errZ":100.0,"hitPingte":0.0,"errPingte":100.0,"profile":"--"}
    latest_issue=r[0]["issue"]
    if stats_cache["issue"]==latest_issue and stats_cache["value"] is not None:
        return stats_cache["value"]

    tests=min(sample,len(r)-320)

    # Choose profile only from older data than the validation window.
    calibration_source=r[tests:]
    profile,_scores=_select_profile(calibration_source)

    h24=hmain=hz=hping=0
    actual_tests=0
    for k in range(tests-1,-1,-1):
        train=r[k+1:]
        if len(train)<260: continue
        c24,m4,z4,_,_=_predict_with_profile(train,profile)
        py,_=_predict_pingte_yixiao(train)
        actual=r[k]
        sp=actual["special"]
        az=normalize_z(actual["z7"] or "")
        draw_z=_draw_zodiac_set(actual)

        h24+=int(sp in c24)
        hmain+=int(sp in m4)
        hz+=int(bool(az) and az in z4)
        hping+=int(bool(py) and py in draw_z)
        actual_tests+=1

    n=max(actual_tests,1)
    val={
      "n":actual_tests,
      "profile":profile,
      "hit24":round(h24/n*100,1),"err24":round((actual_tests-h24)/n*100,1),
      "hitMain":round(hmain/n*100,1),"errMain":round((actual_tests-hmain)/n*100,1),
      "hitZ":round(hz/n*100,1),"errZ":round((actual_tests-hz)/n*100,1),
      "hitPingte":round(hping/n*100,1),"errPingte":round((actual_tests-hping)/n*100,1)
    }
    stats_cache["issue"]=latest_issue
    stats_cache["value"]=val
    return val

def initialize_quick_live_cache():
    """Populate latest draw immediately without loading the full database."""
    try:
        r=recent_rows(1)
        if not r:
            return
        latest=r[0]
        latest_numbers=[latest[f"n{i}"] for i in range(1,7)]+[latest["special"]]
        try:
            next_issue=_next_issue_id(latest["issue"])
        except Exception:
            next_issue=""
        data={
          "issue":latest["issue"],"next_issue":next_issue,"count":history_cache.get("total",0),
          "latest_numbers":latest_numbers,
          "latest_special_zodiac":normalize_z(latest["z7"] or ""),
          "latest_created_at":latest["created_at"] or "",
          "special24":[],"main4":[],"zodiac4":[],"zodiac_pairs":[],
          "pingte_yixiao":"",
          "pingte_samples":0,
          "profile":model_state.get("profile") or "平衡",
          "profile_scores":{},
          "forecast":{"target_issue":next_issue,"transition_samples":0,
                      "exact_previous_number_samples":0,
                      "mode":"模型后台初始化中"},
          "strategy":{"cold_rebound_now":False,"cold_zodiacs":[],
                      "latest_zodiac":normalize_z(latest["z7"] or ""),
                      "nmy_samples":0,"nmy_conditional_pct":0.0,"nmy_baseline_pct":0.0,
                      "nmy_lift_pct":0.0,"head_advice":"模型后台初始化中","head_strength":{}},
          "trend":{"wave":{},"size":{},"parity":{}},
          "telegram":bool(BOT_TOKEN),
          "recalculating":True
        }
        with live_cache_lock:
            live_cache["issue"]=latest["issue"]
            live_cache["data"]=data
            live_cache["building"]=False
        print(f"[BOOT] quick cache ready issue={latest['issue']}",flush=True)
    except Exception as e:
        print(f"[BOOT] quick cache failed: {type(e).__name__}: {e}",flush=True)

def build_model():
    model_state["recalc_started_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
    r=recent_rows(1200)
    if not r:
        return {"issue":None,"count":0,"special24":[],"main4":[],"zodiac4":[],"zodiac_pairs":[],"telegram":bool(BOT_TOKEN),"recalculating":False}
    profile,profile_scores=_select_profile(r)
    _legacy24,m4,z4,groups,zpairs=_predict_complement_with_profile(r,profile)
    comp=complement_matrix(60,profile)
    pingte_one,pingte_meta=_predict_pingte_yixiao(r)
    trend=_trend_profiles(r)
    strategy=_strategy_context(r)
    _nt,_zt,_ht,_wt,_st,_pt,transition_meta=_forward_transition_scores(r)
    latest=r[0]
    latest_numbers=[latest[f"n{i}"] for i in range(1,7)]+[latest["special"]]
    try: next_issue=_next_issue_id(latest["issue"])
    except Exception: next_issue=""
    c20,meta20=_predict20_hot(r,profile)
    c27,meta27=_predict27_tenblock(r,profile,next_issue)
    stats20=_profile_hit_stats("20码精选",60)
    stats27=_stats27_v3()
    diag20=_strategy_diagnostics("20码精选",60)
    diag27=_strategy_diagnostics(TEN27_V3_PROFILE,60)
    correction=_correction_status()
    model_pool=_pool_dashboard()
    stable_signals=_stable_dashboard()
    f_error_diag=_f_failure_attribution(60)
    return {
      "issue":latest["issue"],"next_issue":next_issue,"count":history_cache.get("total",0),
      "latest_numbers":latest_numbers,
      "latest_special_zodiac":normalize_z(latest["z7"] or ""),
      "latest_created_at":latest["created_at"] or "",
      "special20":[f"{n:02d}" for n in sorted(c20)],
      "special27":[f"{n:02d}" for n in sorted(c27)],
      "special24":[f"{n:02d}" for n in sorted(c20)],  # compatibility alias
      "strategy20":meta20,
      "strategy27":meta27,
      "stats20":stats20,
      "stats27":stats27,
      "diagnostics20":diag20,
      "diagnostics27":diag27,
      "correction":correction,
      "model_pool":model_pool,
      "stable_signals":stable_signals,
      "f_error_diag":f_error_diag,
      "main4":[f"{n:02d}" for n in m4],
      "zodiac4":z4,
      "zodiac_pairs":[{"zodiac":p["zodiac"],"code":f"{p['code']:02d}"} for p in zpairs],
      "pingte_yixiao":pingte_one,
      "pingte_samples":pingte_meta.get("samples",0),
      "profile":profile,
      "profile_scores":profile_scores,
      "complement":comp,
      "calibration_n":model_state.get("calibration_n",0),
      "learning":{
        "enabled":True,
        "best_profile":learner_cache.get("best_profile","平衡"),
        "settled":learner_cache.get("settled",0),
        "target":60,
        "rate24":(learner_cache.get("profiles",{}).get(learner_cache.get("best_profile","平衡"),{}) or {}).get("rate24",0.0),
        "ai_trained":ai_state.get("trained",0),
        "ai_ready":ai_state.get("ready",False),
        "ai_rolling_window":ai_state.get("rolling_window",100),
        "ai_rolling_trained":ai_state.get("rolling_trained",0),
        "ai_refit_count":ai_state.get("refit_count",0),
        "ai_refit_seconds":ai_state.get("last_refit_seconds",0.0),
        "ai_mix_pct":(get_dynamic_ai_mix().get("mix_pct",35.0) if ai_state.get("ready",False) else 0.0),
        "fusion":get_dynamic_ai_mix(),
        "ai_history_n":ai_state.get("historical_validation_n",0),
        "ai_history_hit24":ai_state.get("historical_hit24",0.0),
        "ai_live":ai_live_validation_stats(60),
        "updated_at":learner_cache.get("updated_at",""),
        "auto":{
            "updating":auto_state.get("updating",False),
            "last_draw_issue":auto_state.get("last_draw_issue",""),
            "last_learning_issue":auto_state.get("last_learning_issue",""),
            "last_prediction_issue":auto_state.get("last_prediction_issue",""),
            "last_refresh_at":auto_state.get("last_refresh_at",""),
            "last_error":auto_state.get("last_error","")
        }
      },
      "forecast":{
        "target_issue":next_issue,
        "transition_samples":transition_meta.get("samples",0),
        "exact_previous_number_samples":transition_meta.get("exact_samples",0),
        "long_prior_ready":bool(long_prior.get("ready")),
        "long_prior_rows":int(long_prior.get("total",0)),
        "mode":"F动态19-23单期 + 27码20期结构长码10期"
      },
      "strategy":{
        "cold_rebound_now":strategy["cold_rebound_now"],
        "cold_zodiacs":strategy["cold_zodiacs"],
        "latest_zodiac":strategy["latest_zodiac"],
        "nmy_samples":strategy["nmy_rebound"]["samples"],
        "nmy_conditional_pct":round(strategy["nmy_rebound"]["conditional"]*100,1),
        "nmy_baseline_pct":round(strategy["nmy_rebound"]["baseline"]*100,1),
        "nmy_lift_pct":round(strategy["nmy_rebound"]["lift"]*100,1),
        "head_advice":strategy["head"]["advice"],
        "head_strength":strategy["head"]["strength"]
      },
      "trend":{
        "wave":{k:round(v*100,1) for k,v in trend["wave"].items()},
        "size":{k:round(v*100,1) for k,v in trend["size"].items()},
        "parity":{k:round(v*100,1) for k,v in trend["parity"].items()},
        "wave_parity":{k:round(v*100,1) for k,v in trend["wave_parity"].items()}
      },
      "telegram":bool(BOT_TOKEN),
      "recalculating":False,
      "model_recalc_started_at":model_state.get("recalc_started_at",""),
      "model_recalc_finished_at":time.strftime("%Y-%m-%d %H:%M:%S")
    }

def rebuild_live_cache():
    with live_cache_lock:
        if live_cache.get("building"):
            return live_cache.get("data")
        live_cache["building"]=True
        previous=live_cache.get("data")
    t0=time.time()
    try:
        data=build_model()
        model_state["recalc_finished_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
        with live_cache_lock:
            live_cache["data"]=data
            live_cache["issue"]=data.get("issue") if isinstance(data,dict) else None
        print(f"[CACHE] model rebuilt issue={data.get('issue')} in {time.time()-t0:.2f}s",flush=True)
        try:
            rr=recent_rows(1200)
            threading.Thread(
                target=record_shadow_predictions,
                args=(rr,),
                daemon=True,
                name="continuous-learning-shadow"
            ).start()
        except Exception as e:
            print(f"[LEARN] shadow start failed: {e}",flush=True)
        return data
    except Exception as e:
        print(f"[CACHE] model rebuild failed: {type(e).__name__}: {e}",flush=True)
        return previous
    finally:
        with live_cache_lock:
            live_cache["building"]=False

def model():
    """Return live snapshot and self-heal if DB has moved ahead of cache."""
    with live_cache_lock:
        data=live_cache.get("data")
        building=live_cache.get("building")
    try:
        latest=latest_row()
    except Exception:
        latest=None

    if data is not None and latest is not None:
        cached_issue=str(data.get("issue") or "")
        latest_issue=str(latest["issue"] or "")
        if cached_issue != latest_issue:
            if not building:
                threading.Thread(target=rebuild_live_cache,daemon=True,name="stale-cache-self-heal").start()
            stale=dict(data)
            stale["issue"]=latest_issue
            stale["next_issue"]=_next_issue_id(latest_issue)
            stale["recalculating"]=True
            stale["stale_prediction"]=True
            stale["special20"]=[]
            stale["special27"]=[]
            stale["special24"]=[]
            fc=dict(stale.get("forecast") or {})
            fc["target_issue"]=_next_issue_id(latest_issue)
            fc["mode"]="新期开奖已入库 · 正在重算下一期"
            stale["forecast"]=fc
            return stale

    if data is not None:
        ans=dict(data)
        ans["stale_prediction"]=False
        return ans

    if not building:
        return rebuild_live_cache() or {
          "issue":str(latest["issue"]) if latest else None,
          "next_issue":_next_issue_id(latest["issue"]) if latest else "",
          "count":0,"recalculating":True,"stale_prediction":True,
          "special20":[],"special27":[],"special24":[]
        }

    return {
      "issue":str(latest["issue"]) if latest else None,
      "next_issue":_next_issue_id(latest["issue"]) if latest else "",
      "count":0,"recalculating":True,"stale_prediction":True,
      "special20":[],"special27":[],"special24":[]
    }


@app.get("/")
def home():
    return render_template_string(INDEX_HTML)

@app.get("/api/health")
def health():
    with history_cache_lock:
        count=history_cache.get("total",0)
    return jsonify({"ok":True,"ready":background_state.get("boot_ready",False),
                    "model_building":bool(live_cache.get("building")),
                    "stats_building":background_state.get("stats_building",False),
                    "long_prior_ready":bool(long_prior.get("ready")),
                    "long_prior_rows":int(long_prior.get("total",0)),
                    "learning_settled":int(learner_cache.get("settled",0)),
                    "learning_best":learner_cache.get("best_profile","平衡"),
                    "db":DB,"sqlite_mode":"WAL","telegram":bool(BOT_TOKEN),
                    "telegram_mode":"webhook","webhook_base":WEBHOOK_BASE_URL,"count":count,
                    "last_error":auto_state.get("last_error",""),
                    "last_draw_issue":auto_state.get("last_draw_issue",""),
                    "last_prediction_issue":auto_state.get("last_prediction_issue","")})

@app.get("/api/prediction")
def prediction():
    # Cached live endpoint: normally no full-history calculation happens here.
    resp=jsonify(model())
    resp.headers["Cache-Control"]="no-store, no-cache, must-revalidate, max-age=0"
    return resp

def _build_stats_background():
    if background_state.get("stats_building"):
        return
    background_state["stats_building"]=True
    try:
        r=recent_rows(1100)
        # Keep this modest on the free instance. It is background diagnostics,
        # never allowed to block the web page.
        val=backtest_stats(r,24)
        print(f"[STATS] refreshed n={val.get('n')} issue={r[0]['issue'] if r else None}",flush=True)
    except Exception as e:
        print(f"[STATS] failed: {type(e).__name__}: {e}",flush=True)
    finally:
        background_state["stats_building"]=False

@app.get("/api/stats")
def stats_api():
    base=learner_validation_stats()
    base["code20"]=_profile_hit_stats("20码精选",60)
    base["code27"]=_stats27_v3()
    base["diag20"]=_strategy_diagnostics("20码精选",60)
    base["diag27"]=_strategy_diagnostics(TEN27_V3_PROFILE,60)
    base["correction"]=_correction_status()
    return jsonify(base)

@app.get("/api/learning")
def learning_status():
    with learner_lock:
        return jsonify({
          "best_profile":learner_cache.get("best_profile","平衡"),
          "settled":learner_cache.get("settled",0),
          "window":learner_cache.get("window",60),
          "profiles":learner_cache.get("profiles",{}),
          "updated_at":learner_cache.get("updated_at","")
        })

@app.get("/api/learning-export")
def learning_export():
    try:
        limit=max(20,min(int(request.args.get("limit","1000")),5000))
    except Exception:
        limit=1000
    return jsonify(export_learning_payload(limit))


@app.get("/api/auto-status")
def auto_status():
    with learner_lock:
        best=learner_cache.get("best_profile","平衡")
        settled=int(learner_cache.get("settled",0))
        profiles=dict(learner_cache.get("profiles",{}) or {})
    live=ai_live_validation_stats(60)
    trained=int(ai_state.get("trained",0))
    rolling_trained=int(ai_state.get("rolling_trained",0))
    fusion=get_dynamic_ai_mix()
    mix=float(fusion.get("mix_pct",35.0)) if ai_state.get("ready",False) else 0.0
    return jsonify({
      "ok":True,
      "updating":bool(auto_state.get("updating",False)),
      "last_draw_issue":auto_state.get("last_draw_issue",""),
      "last_learning_issue":auto_state.get("last_learning_issue",""),
      "last_prediction_issue":auto_state.get("last_prediction_issue",""),
      "last_refresh_at":auto_state.get("last_refresh_at",""),
      "last_error":auto_state.get("last_error",""),
      "ai_trained":trained,
      "ai_rolling_window":int(ai_state.get("rolling_window",100)),
      "ai_rolling_trained":rolling_trained,
      "ai_refit_count":int(ai_state.get("refit_count",0)),
      "ai_refit_seconds":float(ai_state.get("last_refit_seconds",0.0)),
      "ai_mix_pct":mix,
      "fusion":fusion,
      "ai_live":live,
      "model_settled":settled,
      "model_best":best,
      "model_rate24":(profiles.get(best,{}) or {}).get("rate24",0.0),
      "complement":complement_matrix(60, fusion.get("benchmark_profile") or best),
      "persistent":PERSISTENT_MODE,
      "remote_backup_enabled":REMOTE_BACKUP_ENABLED,
      "remote_backup_at":auto_state.get("remote_backup_at",""),
      "remote_restore_at":auto_state.get("remote_restore_at",""),
      "remote_backup_error":auto_state.get("remote_backup_error",""),
      "checkpoint_at":auto_state.get("checkpoint_at",""),
      "checkpoint_error":auto_state.get("checkpoint_error","")
    })

@app.get("/api/complement-status")
def complement_status():
    fusion=get_dynamic_ai_mix()
    return jsonify(complement_matrix(60, fusion.get("benchmark_profile") or learner_cache.get("best_profile","趋势快")))

@app.get("/api/strategy-diagnostics")
def strategy_diagnostics_api():
    return jsonify({
      "code20":_strategy_diagnostics("20码精选",60),
      "code27":_strategy_diagnostics(TEN27_V3_PROFILE,60),
      "correction":_correction_status()
    })

@app.get("/api/model-pool")
def model_pool_api():
    return jsonify({
      "model_pool":_pool_dashboard(),
      "stable_signals":_stable_dashboard()
    })

@app.get("/api/backup-status")
def backup_status():
    exists=os.path.exists(CHECKPOINT_PATH)
    try:
        size=os.path.getsize(CHECKPOINT_PATH) if exists else 0
    except Exception:
        size=0
    return jsonify({
      "persistent":PERSISTENT_MODE,
      "persist_dir":PERSIST_DIR,
      "db_path":DB,
      "checkpoint_path":CHECKPOINT_PATH,
      "checkpoint_exists":exists,
      "checkpoint_bytes":size,
      "checkpoint_at":auto_state.get("checkpoint_at",""),
      "checkpoint_error":auto_state.get("checkpoint_error",""),
      "remote_backup_enabled":REMOTE_BACKUP_ENABLED,
      "remote_provider":"Supabase Storage" if REMOTE_BACKUP_ENABLED else "",
      "remote_bucket":SUPABASE_BUCKET if REMOTE_BACKUP_ENABLED else "",
      "remote_object":SUPABASE_OBJECT if REMOTE_BACKUP_ENABLED else "",
      "remote_backup_at":auto_state.get("remote_backup_at",""),
      "remote_restore_at":auto_state.get("remote_restore_at",""),
      "remote_backup_error":auto_state.get("remote_backup_error","")
    })

@app.get("/api/full-backup")
def full_backup():
    write_checkpoint_atomic()
    if not os.path.exists(CHECKPOINT_PATH):
        return jsonify({"ok":False,"error":"checkpoint unavailable"}),500
    return send_file(
        CHECKPOINT_PATH,
        as_attachment=True,
        download_name="sanfen_ai_checkpoint.json.gz",
        mimetype="application/gzip"
    )

@app.get("/api/history")
def history():
    try:
        limit=max(20,min(int(request.args.get("limit","200")),500))
    except Exception:
        limit=200
    with history_cache_lock:
        total=history_cache.get("total",0)
        items=list(history_cache.get("items",[]))[:limit]
        loaded_at=history_cache.get("loaded_at","")
    # If cache is unexpectedly empty, rebuild once.
    if not items:
        try:
            refresh_history_cache()
            with history_cache_lock:
                total=history_cache.get("total",0)
                items=list(history_cache.get("items",[]))[:limit]
                loaded_at=history_cache.get("loaded_at","")
        except Exception as e:
            return jsonify({"total":0,"items":[],"error":str(e)}),503
    resp=jsonify({"total":total,"items":items,"cache_time":loaded_at})
    resp.headers["Cache-Control"]="no-store, no-cache, must-revalidate, max-age=0"
    return resp

@app.get("/api/export")
def export_history():
    try:
        limit=max(20,min(int(request.args.get("limit","20000")),30000))
    except Exception:
        limit=20000
    def _read():
        c=connect()
        try:
            rr=c.execute("""SELECT * FROM draws
                            ORDER BY CAST(issue AS INTEGER) DESC LIMIT ?""",(limit,)).fetchall()
            total=c.execute("SELECT COUNT(*) FROM draws").fetchone()[0]
            return total,rr
        finally:
            c.close()
    total,rr=_db_retry(_read)
    items=[{k:x[k] for k in x.keys()} for x in rr]
    return jsonify({"total":total,"items":items})

@app.get("/api/sync-status")
def sync_status():
    with history_cache_lock:
        count=history_cache.get("total",0)
    return jsonify({**sync_state,"history_count":count})

@app.post("/api/sync-history")
def sync_history_now():
    body=request.get_json(silent=True) or {}
    url=str(body.get("url") or HISTORY_SOURCE_URL or "").strip()
    n=sync_remote_history(url)
    refresh_all_caches()
    return jsonify({"ok":not bool(sync_state["last_error"]),"imported":n,**sync_state})


def tg_send(chat_id, text, reply_to_message_id=None):
    if not BOT_TOKEN:
        return False
    payload={"chat_id":chat_id,"text":text}
    if reply_to_message_id:
        payload["reply_to_message_id"]=reply_to_message_id
    try:
        r=requests.post(f"{TG_API}/sendMessage",json=payload,timeout=12)
        if not r.ok:
            print(f"[TG] sendMessage failed {r.status_code}: {r.text[:300]}",flush=True)
        return r.ok
    except Exception as e:
        print(f"[TG] sendMessage exception: {type(e).__name__}: {e}",flush=True)
        return False

def set_telegram_webhook():
    if not BOT_TOKEN:
        print("[TG] BOT_TOKEN not configured; webhook disabled",flush=True)
        return False
    if not WEBHOOK_BASE_URL:
        print("[TG] no public webhook URL detected; set WEBHOOK_BASE_URL manually",flush=True)
        return False
    url=f"{WEBHOOK_BASE_URL}{WEBHOOK_PATH}"
    payload={
        "url":url,
        "secret_token":WEBHOOK_SECRET,
        "allowed_updates":["message"],
        "drop_pending_updates":False,
        "max_connections":20
    }
    try:
        r=requests.post(f"{TG_API}/setWebhook",json=payload,timeout=15)
        print(f"[TG] setWebhook url={url} -> {r.status_code} {r.text[:400]}",flush=True)
        return r.ok
    except Exception as e:
        print(f"[TG] setWebhook exception: {type(e).__name__}: {e}",flush=True)
        return False

def webhook_keeper():
    # Re-assert webhook during deploy overlap, then every 10 minutes.
    for delay in (0,8,20,40):
        if delay:
            time.sleep(delay)
        set_telegram_webhook()
    while True:
        time.sleep(600)
        set_telegram_webhook()

def process_telegram_message(msg):
    text=msg.get("text") or ""
    chat=msg.get("chat") or {}
    sender=msg.get("from") or {}
    chat_id=chat.get("id")
    message_id=msg.get("message_id")
    sender_name=sender.get("username") or " ".join(
        x for x in [sender.get("first_name"),sender.get("last_name")] if x
    ) or "unknown"
    sender_is_bot=bool(sender.get("is_bot"))
    print(f"[TG-WEBHOOK] chat={chat_id} sender={sender_name} is_bot={sender_is_bot} text={text[:160]!r}",flush=True)

    # Commands remain available for setup/testing.
    cmd=(text.strip().split()[0].split("@")[0].lower() if text.strip().startswith("/") else "")
    if cmd=="/id":
        tg_send(chat_id,f"Chat ID: {chat_id}",message_id)
        return
    if cmd=="/status":
        m=model()
        tg_send(chat_id,f"运行正常\\n历史期数: {m['count']}\\n最新期号: {m['issue']}",message_id)
        return

    if ALLOWED_CHAT_ID and str(chat_id)!=ALLOWED_CHAT_ID:
        print(f"[TG-WEBHOOK] ignored: chat id does not match ALLOWED_CHAT_ID={ALLOWED_CHAT_ID}",flush=True)
        return

    p=parse_draw(text)
    if not p:
        print("[TG-WEBHOOK] message received but parser did not recognize a complete draw",flush=True)
        return

    issue,nums,zs,colors=p
    print(f"[TG-WEBHOOK] parsed issue={issue} nums={nums} zodiac={zs} colors={colors}",flush=True)
    if add_draw(issue,nums,zs,colors,text):
        print(f"[TG-WEBHOOK] inserted issue={issue}",flush=True)
        # Official bot messages are never auto-replied to, avoiding bot loops/flood control.
        if not sender_is_bot:
            tg_send(chat_id,f"已入库 {issue}，统计结果已更新。",message_id)
    else:
        print(f"[TG-WEBHOOK] duplicate/invalid issue={issue}, not inserted",flush=True)

@app.post(WEBHOOK_PATH)
def telegram_webhook():
    if WEBHOOK_SECRET:
        got=request.headers.get("X-Telegram-Bot-Api-Secret-Token","")
        if got!=WEBHOOK_SECRET:
            return jsonify({"ok":False,"error":"bad secret"}),403
    data=request.get_json(silent=True) or {}
    msg=data.get("message")
    if msg:
        process_telegram_message(msg)
    return jsonify({"ok":True})

@app.get("/api/webhook")
def webhook_status():
    if not BOT_TOKEN:
        return jsonify({"ok":False,"configured":False})
    try:
        r=requests.get(f"{TG_API}/getWebhookInfo",timeout=10)
        payload=r.json()
        payload["_detected_base_url"]=WEBHOOK_BASE_URL
        return jsonify(payload)
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}),500

def boot():
    init_db()
    if REMOTE_BACKUP_ENABLED:
        download_checkpoint_from_supabase()
    restore_checkpoint_if_better()
    import_history_once()

    # Preserve latest bot-collected history before takeover.
    sync_best_available_history()

    # From v26 onward, also inherit learned prediction/score history.
    sync_previous_learning()
    load_ai_state()
    load_correction_state()
    refresh_learner_cache(60)
    try:
        refresh_dynamic_ai_mix()
    except Exception as e:
        print(f"[FUSION] boot refresh failed: {e}",flush=True)
    try:
        _r0=recent_rows(1)
        if _r0:
            auto_state["last_draw_issue"]=str(_r0[0]["issue"])
            auto_state["last_prediction_issue"]=_next_issue_id(_r0[0]["issue"])
    except Exception:
        pass
    auto_state["last_refresh_at"]=time.strftime("%Y-%m-%d %H:%M:%S")

    # Make the site usable immediately. Do NOT wait for model/backtest here.
    try:
        refresh_history_cache()
    except Exception as e:
        print(f"[BOOT] history cache failed: {type(e).__name__}: {e}",flush=True)
    initialize_quick_live_cache()
    background_state["boot_ready"]=True

    # Webhook can now receive new draws.
    if BOT_TOKEN:
        threading.Thread(target=webhook_keeper,daemon=True,name="telegram-webhook-keeper").start()

    # Live model first, then full-history prior in parallel. Neither blocks the site.
    threading.Thread(target=rebuild_live_cache,daemon=True,name="initial-model-build").start()
    threading.Thread(target=build_long_prior,daemon=True,name="full-history-prior").start()
    threading.Thread(target=bootstrap_ai_history,daemon=True,name="ai-history-bootstrap").start()
    threading.Thread(target=bootstrap_correction_models,daemon=True,name="correction-ai-bootstrap").start()

boot()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=PORT)
