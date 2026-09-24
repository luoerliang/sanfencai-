import os, re, csv, sqlite3, threading, math, time, hashlib, json, gzip
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

  <section class="card">
    <div class="sectionHead">
      <div class="sectionTitle">下一期预测状态</div>
      <div class="sectionHint">不是把刚开奖号追进去</div>
    </div>
    <div class="strategyGrid">
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

  <section class="card">
    <div class="sectionHead">
      <div>
        <div class="sectionTitle">20码 · 每期精选</div>
        <div class="sectionHint">最冷3肖不取 · 前3热肖最多3码 · 每期重算</div>
      </div>
      <button class="copyBtn" onclick="copySpecial()">一键复制</button>
    </div>
    <div id="sp" class="balls"></div>
    <div class="pillrow" style="margin-top:10px">
      <span class="pill">AI+趋势共识优先</span>
      <span class="pill">最冷3肖彻底不取</span>
      <span class="pill">0/4弱头可直接杀</span>
      <span class="pill">红蓝绿×单双共同评分</span>
    </div>
  </section>

  <section class="card">
    <div class="sectionHead">
      <div>
        <div class="sectionTitle">27码 · 十期一换</div>
        <div id="code27Hint" class="sectionHint">AI+趋势必杀0/4其中一头</div>
      </div>
      <button class="copyBtn" onclick="copy27()">一键复制</button>
    </div>
    <div id="sp27" class="balls"></div>
    <div class="pillrow" style="margin-top:10px">
      <span id="code27Block" class="pill"></span>
      <span id="code27Kill" class="pill"></span>
      <span id="code27Stats" class="pill"></span>
    </div>
  </section>

  <section class="card">
    <div class="sectionHead">
      <div class="sectionTitle">4肖 · 一肖一码</div>
      <div class="sectionHint">每期重算 · 4肖与4码严格一一对应</div>
    </div>
    <div id="zpair" class="zpairGrid"></div>
  </section>

  <section class="card">
    <div class="sectionHead">
      <div class="sectionTitle">平特一肖</div>
      <div class="sectionHint">预测下一期7个号码中至少出现1次的生肖</div>
    </div>
    <div class="comboBox" style="text-align:center">
      <div id="pingteOne" style="font-size:36px;font-weight:900;letter-spacing:2px">--</div>
      <div id="pingteSamples" class="sectionHint" style="margin-top:8px">--</div>
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
      <div class="stat"><div class="statName">20码精选</div><div id="hit22" class="rate">--</div><div id="err22" class="err"></div></div>
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

  <div id="toast" class="toast">已复制</div>
  <div class="foot">号码颜色按红 / 蓝 / 绿波显示。v31改成趋势主攻、AI专门纠错：生肖转移/7码结构先决定热中冷层，基础AI负责非线性结构，纠错AI只训练趋势漏掉、冷三肖误杀、杀头误杀和排名截断案例。20码与27码使用独立纠错权重；所有诊断只统计开奖前已锁定记录，不代表未来概率。</div>
</div>

<script>
const RED=new Set([1,2,7,8,12,13,18,19,23,24,29,30,34,35,40,45,46]);
const BLUE=new Set([3,4,9,10,14,15,20,25,26,31,36,37,41,42,47,48]);
function cls(n){n=Number(n);return RED.has(n)?'red':(BLUE.has(n)?'blue':'green')}
let SPECIAL20=[];
let SPECIAL27=[];
function fmt(n){return String(n).padStart(2,'0')}
async function copySpecial(){
  const text=SPECIAL20.join(' ');
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
  const text=SPECIAL27.join(' ');
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
    const m20=d.strategy20||{}, m27=d.strategy27||{}, s20=d.stats20||{}, s27=d.stats27||{};
    code27Block.textContent=`${m27.block_start||'--'}-${m27.block_end||'--'} 十期固定`;
    code27Kill.textContent=`本轮必杀：${m27.killed_head||'--'}`;
    const cur27=s27.current||{}, last27=s27.last_complete||null;
    code27Stats.textContent=last27?`上一完整轮：10中${last27.hits??0}`:`本轮：${cur27.hits??0}中${cur27.n??0}/10`;
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
    forecastTarget.textContent=fc.target_issue?`预测 ${fc.target_issue} 期`:'--';
    forecastMode.innerHTML=`${fc.mode||'前瞻预测'}<br>转移样本 ${fc.transition_samples??0} · 全历史 ${fc.long_prior_ready?'已缓存':'后台加载'}`;
    const lr=d.learning||{};
    const ail=lr.ai_live||{};
    const au=lr.auto||{};
    const fu=lr.fusion||{};
    learningState.innerHTML=`趋势主模型 + AI纠错模型<br>基础AI继续学结构，纠错AI只学趋势漏掉的期`;
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
    corr20State.innerHTML=`已学错题 ${(cr['20']||{}).trained??0} 次<br>当前纠错权重 ${m20.correction_weight_pct??0}%`;
    rank20State.innerHTML=`1-10 ${rb['1-10']??0} · 11-20 ${rb['11-20']??0}<br>21-27 ${rb['21-27']??0} · 28+ ${rb['28+']??0}`;
    head27State.innerHTML=`成功 ${dg27.head_kill_success??0}/${dg27.head_kill_n??0}<br>${dg27.head_kill_rate??0}%`;
    zodiacTransitionState.textContent=`生肖转移优先：${(m20.zodiac_transition_top||[]).join('、')||'--'}`;
    failureState.textContent=`冷三肖误杀 ${dg20.cold_zodiac_errors??0} · 底层排序 ${fr['底层排序']??0}`;
    const sg=d.strategy||{};
    coldSignal.innerHTML=sg.cold_rebound_now?'冷反弹信号：启用':'冷反弹信号：普通';
    coldZodiac.innerHTML=(sg.cold_zodiacs||[]).length?`偏冷：${sg.cold_zodiacs.join('、')}`:'暂无';
    headSignal.innerHTML=sg.head_advice||'暂无';
    nmySignal.innerHTML=`样本 ${sg.nmy_samples??0}<br>条件 ${sg.nmy_conditional_pct??0}% / 基准 ${sg.nmy_baseline_pct??0}%`;
    latestZodiac.textContent=d.latest_special_zodiac?`特码生肖 ${d.latest_special_zodiac}`:'特码生肖 --';
    lastIngest.textContent=d.latest_created_at?`最后录入 ${d.latest_created_at}`:'实时录入';
    tg.textContent=d.telegram?'Telegram 已连接':'Telegram 未配置';
    adaptiveInfo.textContent=`前瞻预测：${d.next_issue||'--'}期 · 20码每期变 · 27码十期一换`;
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
    learningState.innerHTML=`趋势主模型 + AI纠错模型<br>AI不再和趋势做同一件事`;
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
                auto_state["last_prediction_issue"]=str(int(rr[0]["issue"])+1)
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
                if _update_correction_model(state,actual,strategy,persist=False):
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
                failed=(rank>cutoff or actual_z in set(diag["coldest3"])
                        or (diag["killed_head"] and head_of(actual)==diag["killed_head"]))
                if failed:
                    _update_correction_model(state,actual,strategy,persist=False)
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

def _update_correction_model(state_rows, actual_special, strategy, persist=True):
    """Train ONLY on cases where the trend-side decision failed.

    This deliberately gives AI a different job from the trend model:
    learn what the trend model tends to miss, instead of copying it.
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

    ctx=dict(ctx)
    ctx["regime"]=regime
    ctx["correction_trained"]=corr_trained
    ctx["correction_weight_pct"]=round(corr_w*100,1)
    ctx["trend_weight_pct"]=round(trend_w*100,1)
    ctx["base_ai_weight_pct"]=round(base_ai_w*100,1)
    return score,zmap,zheat,ctx

def _trend_diagnostics(r,profile,strategy="20"):
    """Pre-draw trend-only diagnostic used for training the correction AI."""
    score,zmap,zheat,ctx,regime=_trend_only_number_scores(r,profile,strategy)
    ranked=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
    ranked_z=sorted(ALL_ZODIACS,key=lambda z:(-zheat.get(z,-1e9),z))
    coldest3=ranked_z[-3:]
    if str(strategy)=="27":
        head=_combined_04_head_decision(r,force=True)
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


def _predict20_hot(r, profile):
    """20码：每期一换。

    - 最冷3肖彻底不取
    - 剩余9肖默认各2码 = 18
    - 前3热肖最多3码，其中两个热肖各加1个 = 20
    - 0/4头若AI+趋势判断一头明显更弱，20码彻底杀掉该头
    """
    score,zmap,zheat,ctx=_selection_number_scores(r,profile,"20")
    ranked_z=sorted(ALL_ZODIACS,key=lambda z:(-zheat.get(z,-1e9),z))
    allowed=ranked_z[:9]
    top3=ranked_z[:3]
    coldest3=ranked_z[-3:]

    head=_combined_04_head_decision(r,force=False)
    killed=head["killed_head"]

    pools={}
    for z in allowed:
        pools[z]=sorted(
            [n for n in range(1,50)
             if zmap.get(n)==z and (not killed or head_of(n)!=killed)],
            key=lambda n:(-score.get(n,-1e9),n)
        )

    quotas={z:2 for z in allowed}
    thirds=[]
    for z in top3:
        if len(pools.get(z,[]))>=3:
            n=pools[z][2]
            thirds.append((score.get(n,-1e9),z))
    for _v,z in sorted(thirds,reverse=True)[:2]:
        quotas[z]=3

    selected=[]; groups=[]
    for z in allowed:
        take=pools.get(z,[])[:quotas[z]]
        selected.extend(take)
        groups.append({"zodiac":z,"codes":take,"quota":quotas[z]})

    # Defensive fill: never use coldest3 and never violate the killed head.
    used=set(selected)
    if len(selected)<20:
        for n in sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n)):
            if n in used or zmap.get(n) not in allowed:
                continue
            if killed and head_of(n)==killed:
                continue
            z=zmap.get(n)
            cap=3 if z in top3 else 2
            if sum(1 for x in selected if zmap.get(x)==z)>=cap:
                continue
            selected.append(n); used.add(n)
            if len(selected)>=20:
                break

    ranked49=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
    ztrans,ztrans_meta=_zodiac_transition_model(r)
    return selected[:20],{
      "hot_zodiacs":allowed,
      "top3_hot":top3,
      "coldest3":coldest3,
      "killed_head":killed,
      "head_decision":head,
      "groups":groups,
      "ranked49":ranked49,
      "regime":(ctx.get("regime") or {}).get("name","平衡"),
      "correction_trained":ctx.get("correction_trained",0),
      "correction_weight_pct":ctx.get("correction_weight_pct",0),
      "trend_weight_pct":ctx.get("trend_weight_pct",0),
      "base_ai_weight_pct":ctx.get("base_ai_weight_pct",0),
      "zodiac_transition_top":sorted(ALL_ZODIACS,key=lambda z:(-ztrans.get(z,0),z))[:4],
      "zodiac_transition_samples":ztrans_meta.get("samples",0)
    }

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
    """27码：10期固定一组，AI+趋势必杀0头/4头其中一头。"""
    br,start,end=_tenblock_state(r,target_issue)

    # Reuse the first locked list in the block, guaranteeing "十期一换".
    existing=_existing_27_block_codes(start,end)
    score,zmap,zheat,ctx=_selection_number_scores(br,profile,"27")
    ranked_z=sorted(ALL_ZODIACS,key=lambda z:(-zheat.get(z,-1e9),z))
    allowed=ranked_z[:9]      # 热肖 + 中位肖
    coldest3=ranked_z[-3:]
    head=_combined_04_head_decision(br,force=True)
    killed=head["killed_head"]

    if existing:
        ranked49=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
        return existing[:27],{
          "block_start":str(start),"block_end":str(end),
          "hot_mid_zodiacs":allowed,"coldest3":coldest3,
          "killed_head":killed,"head_decision":head,
          "reused":True,
          "ranked49":ranked49,
          "regime":(ctx.get("regime") or {}).get("name","平衡"),
          "correction_trained":ctx.get("correction_trained",0),
          "correction_weight_pct":ctx.get("correction_weight_pct",0),
          "trend_weight_pct":ctx.get("trend_weight_pct",0)
        }

    selected=[]; groups=[]
    for z in allowed:
        pool=sorted(
            [n for n in range(1,50)
             if zmap.get(n)==z and head_of(n)!=killed],
            key=lambda n:(-score.get(n,-1e9),n)
        )
        take=pool[:3]
        selected.extend(take)
        groups.append({"zodiac":z,"codes":take})

    # Normally 9肖×3码 = 27 exactly after one head is killed.
    # If a rare mapping shortage occurs, fill only from the same non-coldest 9肖.
    used=set(selected)
    if len(selected)<27:
        for n in sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n)):
            if n in used or zmap.get(n) not in allowed or head_of(n)==killed:
                continue
            selected.append(n); used.add(n)
            if len(selected)>=27:
                break

    ranked49=sorted(range(1,50),key=lambda n:(-score.get(n,-1e9),n))
    return selected[:27],{
      "block_start":str(start),"block_end":str(end),
      "hot_mid_zodiacs":allowed,"coldest3":coldest3,
      "killed_head":killed,"head_decision":head,
      "groups":groups,"reused":False,
      "ranked49":ranked49,
      "regime":(ctx.get("regime") or {}).get("name","平衡"),
      "correction_trained":ctx.get("correction_trained",0),
      "correction_weight_pct":ctx.get("correction_weight_pct",0),
      "trend_weight_pct":ctx.get("trend_weight_pct",0)
    }


def _record_strategy_audit(target_issue,profile,selected,meta):
    ranked=meta.get("ranked49") or []
    excluded=meta.get("coldest3") or []
    killed=str(meta.get("killed_head") or "")
    regime=str(meta.get("regime") or "")
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
            kill_success+=int(x["head_kill_success"] or 0)
    n=len(rows)
    return {
      "n":n,
      "rank_buckets":buckets,
      "cold_zodiac_errors":cold_errors,
      "cold_zodiac_error_rate":round(100*cold_errors/n,1) if n else 0.0,
      "head_kill_n":kill_n,
      "head_kill_success":kill_success,
      "head_kill_rate":round(100*kill_success/kill_n,1) if kill_n else 0.0,
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
            rows=c.execute("""SELECT target_issue,hit24 FROM prediction_log
                              WHERE profile='27码十期' AND settled=1
                              ORDER BY CAST(target_issue AS INTEGER) DESC
                              LIMIT 40""").fetchall()
        finally:
            c.close()

    grouped={}
    for x in rows:
        start,end=_issue_block10(x["target_issue"])
        g=grouped.setdefault(start,{"start":start,"end":end,"n":0,"hits":0})
        g["n"]+=1
        g["hits"]+=int(x["hit24"] or 0)

    blocks=sorted(grouped.values(),key=lambda g:g["start"],reverse=True)
    current=blocks[0] if blocks else {"start":0,"end":0,"n":0,"hits":0}
    complete=next((g for g in blocks if g["n"]>=10),None)
    return {
      "current":current,
      "last_complete":complete,
      "overall":_profile_hit_stats("27码十期",60)
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
        target=str(int(r[0]["issue"])+1)
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
        records.append((
            target,"27码十期",
            ",".join(str(n) for n in c27),
            "","",py
        ))
        _record_strategy_audit(target,"27码十期",c27,_m27)
    except Exception as e:
        print(f"[27CODE] locked prediction failed: {type(e).__name__}: {e}",flush=True)

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
      "version":"v31",
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
            next_issue=str(int(latest["issue"])+1)
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
    try: next_issue=str(int(latest["issue"])+1)
    except Exception: next_issue=""
    c20,meta20=_predict20_hot(r,profile)
    c27,meta27=_predict27_tenblock(r,profile,next_issue)
    stats20=_profile_hit_stats("20码精选",60)
    stats27=_stats27_blocks()
    diag20=_strategy_diagnostics("20码精选",60)
    diag27=_strategy_diagnostics("27码十期",60)
    correction=_correction_status()
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
        "mode":"趋势主攻 + AI专门纠错 · 20码/27码分开学习"
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
    """Always return the current memory snapshot immediately."""
    with live_cache_lock:
        data=live_cache.get("data")
        building=live_cache.get("building")
    if data is not None:
        return data
    if not building:
        # First boot only. Build synchronously once.
        return rebuild_live_cache() or {"issue":None,"count":0,"recalculating":True}
    return {"issue":None,"count":0,"recalculating":True}


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
                    "telegram_mode":"webhook","webhook_base":WEBHOOK_BASE_URL,"count":count})

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
    base["code27"]=_stats27_blocks()
    base["diag20"]=_strategy_diagnostics("20码精选",60)
    base["diag27"]=_strategy_diagnostics("27码十期",60)
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
      "code27":_strategy_diagnostics("27码十期",60),
      "correction":_correction_status()
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
            auto_state["last_prediction_issue"]=str(int(_r0[0]["issue"])+1)
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
