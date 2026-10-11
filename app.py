import os, re, csv, sqlite3, threading, math, time, hashlib, json, gzip, itertools
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from flask import Flask, jsonify, render_template_string, request, send_file
import requests
import v72_evidence

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
    "W":"池W波色v52",
    "P":"池P单双v52",
    "A":"池A纠错",
}
POOL_MODEL_DISPLAY = {
    "T":"趋势",
    "Z":"生肖",
    "C":"冷热",
    "W":"波色",
    "P":"单双",
    "A":"纠错",
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
TEN27_V4_PROFILE="27十期精确长码"
TEN27_V4_CORE_PROFILE="27V4核心23"
TEN27_V4_MOBILE_PROFILE="27V4机动4"
TEN27_V4_28_PROFILE="27V4第28码"
TEN27_V5_PROFILE="27十期严格冷肖杀码"
TEN27_V5_CORE_PROFILE="27V5核心23"
TEN27_V5_MOBILE_PROFILE="27V5后4"
TEN27_V5_28_PROFILE="27V5第28码"
F_DYNAMIC_V45_PROFILE="F动态v45"
F_DYNAMIC_V48_PROFILE="F动态v48真2of5"
TEN27_V6_PROFILE="27十期规则诊断版"
TEN27_V6_CORE_PROFILE="27V6核心23"
TEN27_V6_MOBILE_PROFILE="27V6后4"
TEN27_V6_28_PROFILE="27V6第28码"
TEN27_V6_RAW_PROFILE="27V6原始Top27"
F_DYNAMIC_V49_PROFILE="F动态v49独家救援"
TEN27_V7_PROFILE="27十期冷肖可行性"
TEN27_V7_CORE_PROFILE="27V7核心23"
TEN27_V7_MOBILE_PROFILE="27V7后4"
TEN27_V7_28_PROFILE="27V7第28码"
TEN27_V7_RAW_PROFILE="27V7原始Top27"
B27_V50_PROFILE="27B每期动态v50"
B27_V51_PROFILE="27B动态杀头v51"
B27_V52_PROFILE="27B纯20期v52"
C22_V54_PROFILE="22C纯20期v54"
PINGTE_B20_V54_PROFILE="平特一肖B20v54"
PINGTE_DYNAMIC_V56_PROFILE="平特一肖多节奏v56"
F_DYNAMIC_V57_PROFILE="F动态v57减法融合"
F_RAW23_V57_PROFILE="F六模型原始Top23v57"
A27_V57_PROFILE="A组27简化长码v57"
B27_V58_PROFILE="27B近20期尾数软评分v58"
C22_V58_PROFILE="22C独立优化v58"
F_DYNAMIC_V59_PROFILE="F动态v59误伤控制"
F_RAW_SAME_V59_PROFILE="F六模型同码数基准v59"
F_RAW23_V59_PROFILE="F六模型Top23参考v59"
F_CORE20_V60_PROFILE="F核心20贡献v60"
F_ADD21_V60_PROFILE="F第21码贡献v60"
F_ADD22_V60_PROFILE="F第22码贡献v60"
F_ADD23_V60_PROFILE="F第23码贡献v60"
F_DYNAMIC_V61_PROFILE="F动态v61四票绝对保留"
F_DYNAMIC_V62_PROFILE="F动态v62四票绝保强三票优先"
F_RAW_SAME_V62_PROFILE="F六模型同码数基准v62"
F_RAW23_V62_PROFILE="F六模型Top23参考v62"
F_CORE20_V62_PROFILE="F核心20贡献v62"
F_ADD21_V62_PROFILE="F第21码贡献v62"
F_ADD22_V62_PROFILE="F第22码贡献v62"
F_ADD23_V62_PROFILE="F第23码贡献v62"
A27_V62_PROFILE="A组27多策略池杀一头v62"
B27_V62_PROFILE="27B纯近10期尾数软评分v62"
C22_V62_PROFILE="22C纯近10期独立优化v62"
B27_V63_PROFILE="27B近10期缺尾补码v63"
C22_V63_PROFILE="22C近10期缺尾补码v63"
D16_V64_PROFILE="16D多策略波色+近10期v64"
D16_V65_PROFILE="16D波色大小+近10期v65"
F_V66_PROFILE="F减法v66四票绝保"
F_RAW_V66_PROFILE="F原始同码数Control_v66"
F_OLD_SHADOW_V66_PROFILE="F旧规则Shadow_v66"
A27_V66_PROFILE="A27原始Top27Control_v66"
A27_OLD_SHADOW_V66_PROFILE="A27杀号Shadow_v66"
B27_V66_PROFILE="B27原始近10Top27Control_v66"
B27_OLD_SHADOW_V66_PROFILE="B27规则Shadow_v66"
C22_V66_PROFILE="C22原始近10Top22Control_v66"
C22_OLD_SHADOW_V66_PROFILE="C22规则Shadow_v66"
D16_V66_PROFILE="D16波色大小软评分_v66"
D16_OLD_SHADOW_V66_PROFILE="D16配额Shadow_v66"

# ===================== v67 frozen live + atomic shadow experiments =====================
F_BASE_V67_PROFILE="F_BASE_TOP22_v67"
F_4VOTE_V67_PROFILE="F_4VOTE_Shadow_v67"
F_STRONG3_V67_PROFILE="F_STRONG3_Shadow_v67"
F_LIMIT2_V67_PROFILE="F_LIMIT2_Shadow_v67"
F_LEGACY_V66_V67_PROFILE="F_LEGACY_V66_Shadow_v67"

A_BASE_V67_PROFILE="A_BASE_TOP27_v67"
A_KILL_HEAD_V67_PROFILE="A_KILL_HEAD_Shadow_v67"
A_FIXED_ROUND_V67_PROFILE="A_FIXED_ROUND_Shadow_v67"
A_LEGACY_V66_V67_PROFILE="A_LEGACY_V66_Shadow_v67"

B_BASE_V67_PROFILE="B_BASE_TOP27_v67"
B_TAIL_ONLY_V67_PROFILE="B_TAIL_ONLY_Shadow_v67"
B_HEAD_ONLY_V67_PROFILE="B_HEAD_ONLY_Shadow_v67"
B_TAIL_HEAD_V67_PROFILE="B_TAIL_HEAD_Shadow_v67"
B_LEGACY_V66_V67_PROFILE="B_LEGACY_V66_Shadow_v67"

C_BASE_V67_PROFILE="C_BASE_TOP22_v67"
C_TAIL_ONLY_V67_PROFILE="C_TAIL_ONLY_Shadow_v67"
C_COLDHEAD_ONLY_V67_PROFILE="C_COLDHEAD_ONLY_Shadow_v67"
C_TAIL_COLDHEAD_V67_PROFILE="C_TAIL_COLDHEAD_Shadow_v67"
C_LEGACY_V66_V67_PROFILE="C_LEGACY_V66_Shadow_v67"

D_BASE_V67_PROFILE="D_BASE_TOP16_v67"
D_COLOR_ONLY_V67_PROFILE="D_COLOR_ONLY_Shadow_v67"
D_SIZE_ONLY_V67_PROFILE="D_SIZE_ONLY_Shadow_v67"
D_COLOR_SIZE_V67_PROFILE="D_COLOR_SIZE_Shadow_v67"
D_LEGACY_V66_V67_PROFILE="D_LEGACY_V66_Shadow_v67"

V67_RULE_VERSION="v67.4-r1"
V67_MODEL_VERSION="v67.4-model-001"

# ===================== v68 probabilistic rankers =====================
F_BASE_V68_PROFILE="F_R1_TOP22_v68"
A_BASE_V68_PROFILE="A_R1_TOP27_v68"
B_BASE_V68_PROFILE="B_R2_TOP27_v68"
C_BASE_V68_PROFILE="C_R2_TOP22_v68"
D_BASE_V68_PROFILE="D_R3_TOP16_v68"
V68_MODEL_VERSION="v68-prob-001"
V68_CONTROL_RULE_VERSION="v68-v674-control-r1"
V68_PROMOTION_MIN_N=300
V68_PRIMARY_ALPHA=0.0167  # 3 primary rankers, conservative Bonferroni-style gate
V68_UNIFORM_LOGLOSS=math.log(49.0)
V68_UNIFORM_BRIER=48.0/49.0
v68_wf_lock=threading.RLock()
v68_wf_cache={"ts":0.0,"data":None}

# ===================== v70 EV-Gate evidence gate =====================
F_BASE_V69_PROFILE="F_R1_CHALLENGER_TOP22_v69"
F_CURRENT_PROFILE="F_SINGLE_Q1_TOP22_v73"
CURRENT16_PROFILE="CURRENT_Q1_TOP16_v74"
POOL22_PROFILE="POOL_WEIGHTED_TOP22_v74"
A_BASE_V69_PROFILE="A_R1_CHALLENGER_TOP27_v69"
B_BASE_V69_PROFILE="B_R2_CHALLENGER_TOP27_v69"
C_BASE_V69_PROFILE="C_R2_CHALLENGER_TOP22_v69"
D_BASE_V69_PROFILE="D_R1_CHALLENGER_TOP16_v69"
V69_MODEL_VERSION="v69-null-first-001"
V69_PRIMARY_ALPHA=0.025   # two frozen challengers: R1 / R2
V69_PROMOTION_MIN_N=300
V69_UNIFORM_LOGLOSS=math.log(49.0)
V69_UNIFORM_BRIER=48.0/49.0
V69_GATE_LAMBDA=0.0      # no evidence => official probability stays Uniform

# ===================== v70 payout-aware EV gate =====================
V70_PAYOUT_TOTAL=float(os.getenv("SPECIAL_PAYOUT_TOTAL","47.5"))
V70_MIN_FINANCE_N=int(os.getenv("V70_MIN_FINANCE_N","500"))
V70_MODEL_VERSION="v70-profit-gate-001"

# ===================== v71 prospective challenger lab =====================
# New rankers are SHADOW-ONLY until they beat the frozen same-code incumbent
# on genuine pre-draw samples. No result-driven retroactive rewrite is allowed.
V71_MODEL_VERSION="v71-forward-competition-001"
V71_PROMOTION_MIN_N=int(os.getenv("V71_PROMOTION_MIN_N","300"))
V71_ALPHA=float(os.getenv("V71_ALPHA","0.025"))

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

INDEX_HTML = '<!doctype html>\n<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>三分彩 · 精简看板</title>\n<style>\n:root{color-scheme:dark;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#080e18;color:#e8eef8;font-size:12px}*{box-sizing:border-box}body{margin:0}.wrap{max-width:720px;margin:auto;padding:8px}header{display:flex;justify-content:space-between;align-items:center;margin:2px 0 8px}h1{font-size:15px;margin:0}h2{font-size:12px;margin:0}p{margin:4px 0}.muted{color:#94a4bb;font-size:10px;line-height:1.45}.card{padding:9px;margin-bottom:7px;border:1px solid #233149;border-radius:9px;background:#101a29}.head{display:flex;justify-content:space-between;align-items:center;gap:8px}.periods{display:flex;gap:12px;font-size:11px;margin:5px 0 7px;flex-wrap:wrap}.balls{display:flex;gap:4px;flex-wrap:wrap;margin:6px 0}.ball{width:23px;height:23px;display:inline-flex;align-items:center;justify-content:center;border-radius:50%;font-size:11px;font-weight:700;color:white}.red{background:#b63549}.blue{background:#275eaf}.green{background:#22835d}.special{outline:2px solid #ffd168;outline-offset:1px}.group{padding:8px 0;border-bottom:1px solid #253148}.group:last-child{border:0;padding-bottom:0}.group:first-child{padding-top:0}.record{font-size:10px;color:#abbad0;line-height:1.5}.pool{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:4px;margin-top:7px}.model{padding:6px 2px;text-align:center;border-radius:5px;background:#192539;font-size:10px;line-height:1.6}.model strong{font-size:11px}.model span{display:block;color:#9aaec9;font-size:9px}.scroll{overflow:auto;-webkit-overflow-scrolling:touch;margin-top:6px;max-height:225px}table{border-collapse:collapse;width:100%;font-size:10px;text-align:center;white-space:nowrap}th,td{padding:5px 3px;border-bottom:1px solid #233149}th{position:sticky;top:0;background:#18253a;color:#afbed5;z-index:1}td:first-child{font-variant-numeric:tabular-nums;font-size:9px}.hit{color:#58d79f}.miss{color:#f18a95}button{font:inherit;color:#d9e8ff;border:1px solid #3b5274;background:#20314a;border-radius:5px;padding:4px 7px;white-space:nowrap}summary{cursor:pointer;font-weight:600;font-size:11px}.details{margin-top:7px}#historyList{max-height:180px;overflow:auto}.historyRow{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid #24324a}.historyRow .balls{margin:0}.historyRow .ball{width:20px;height:20px;font-size:9px}#toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%);padding:8px 14px;background:#264363;border-radius:7px;display:none;z-index:10}.note{font-size:10px;color:#93a8c4;line-height:1.5;margin-top:7px}@media(max-width:380px){.wrap{padding:6px}.card{padding:7px}.ball{width:21px;height:21px}.pool{gap:2px}th,td{padding:5px 2px}}\n</style></head><body><main class="wrap">\n<header><h1>三分彩 · 精简看板</h1><span class="muted" id="connection">连接中</span></header>\n<section class="card"><div class="head"><h2>最新开奖</h2><span class="muted" id="total">历史 -- 期</span></div><div class="periods"><span>最新 <b id="issue">--</b></span><span>下期 <b id="nextIssue">--</b></span></div><div class="balls" id="latestNums"></div><div class="muted" id="ingest">等待开奖记录</div></section>\n<section class="card" id="groups"><div class="muted">等待三组号码</div></section>\n<section class="card"><div class="head"><h2>多策略池</h2><span class="muted" id="poolMaturity">等待前瞻样本</span></div><div class="pool" id="poolRows"></div></section>\n<section class="card"><div class="head"><h2>最近20期正式锁单结算</h2><span class="muted">✓ 中 · × 错 · — 未锁单</span></div><div class="muted">三组独立结算 · 缺少正式锁单显示 —</div><div class="scroll"><table><thead><tr><th>期号</th><th>特</th><th>趋势</th><th>生肖</th><th>冷热</th><th>波色</th><th>单双</th><th>纠错</th><th>算法22</th><th>算法16</th><th>策略22</th></tr></thead><tbody id="lockRows"><tr><td colspan="11">等待正式结算</td></tr></tbody></table></div></section>\n<section class="card"><div class="head"><h2>平特一肖</h2><strong id="pingte">--</strong></div><div class="record" id="pingteRecord">等待前瞻样本</div></section>\n<details class="card" id="algorithmDetails"><summary>当前算法与验证思路</summary><div class="details"><p class="muted">算法22与算法16共用一套多尺度排序：近60期40% + 近360期40% + 历史转移20%，分别取前22、前16码。每期重算，正式锁单后不改。</p><p class="muted">多策略22独立融合趋势、生肖、冷热、波色、单双、纠错六模型评分，按近10/30/60期正式成绩调整权重，不从算法22截取。三组分别统计；新增两组从v74正式锁单开始。</p><div id="research" class="muted">等待同期验证</div></div></details>\n<details class="card" id="historyDetails"><summary>历史开奖 <span class="muted" id="historyCount"></span></summary><div class="details" id="historyList">展开后加载</div></details>\n<div class="note">v74 · 开奖前锁单，开奖后结算。历史命中率不代表下一期结果。</div>\n</main><div id="toast"></div><script>\nconst $=id=>document.getElementById(id), RED=new Set([1,2,7,8,12,13,18,19,23,24,29,30,34,35,40,45,46]), BLUE=new Set([3,4,9,10,14,15,20,25,26,31,36,37,41,42,47,48]);\nconst names=[\'趋势\',\'生肖\',\'冷热\',\'波色\',\'单双\',\'纠错\'];let current={},loading=false,poolLoading=false,historyLoading=false,groupsKey=\'\';\nconst esc=x=>String(x??\'\').replace(/[&<>"\']/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\',"\'":\'&#39;\'}[c]));\nconst fmt=n=>String(n).padStart(2,\'0\');\nfunction balls(nums,special=false){return(nums||[]).map((n,i)=>`<span class="ball ${RED.has(Number(n))?\'red\':BLUE.has(Number(n))?\'blue\':\'green\'} ${special&&i===6?\'special\':\'\'}">${esc(fmt(n))}</span>`).join(\'\')}\nfunction hm(v){return v===1?\'<span class="hit">✓</span>\':v===0?\'<span class="miss">×</span>\':\'—\'}\nfunction record(s){const n=s.n??0,h=s.hits??0;return `本轮 已开${n}/20 · 中${h} · 错${s.misses??Math.max(0,n-h)}`}\nasync function copyGroup(key){const text=(current[key]||[]).map(fmt).join(\',\');if(!text)return;try{await navigator.clipboard.writeText(text)}catch(e){const t=document.createElement(\'textarea\');t.value=text;document.body.append(t);t.select();document.execCommand(\'copy\');t.remove()}$(\'toast\').textContent=\'已复制 \'+text;$(\'toast\').style.display=\'block\';setTimeout(()=>$(\'toast\').style.display=\'none\',1800)}\nfunction renderPool(mp){if(!mp||!Object.keys(mp.stats||{}).length)return;$(\'poolMaturity\').textContent=`样本 ${mp.mature_n??0}/30`;$(\'poolRows\').innerHTML=names.map(k=>{const x=(mp.stats||{})[k]||{};return `<div class="model"><strong>${k}</strong><span>权重${esc(x.weight_pct??0)}%</span><span>错10:${esc(x.e10??0)}</span><span>错30:${esc(x.e30??0)}</span><span>连错:${esc(x.miss_streak??0)}</span></div>`}).join(\'\');const rows=(mp.recent20||[]).slice(0,20);$(\'lockRows\').innerHTML=rows.length?rows.map(x=>`<tr><td>${esc(x.issue)}</td><td>${esc(x.actual??\'—\')}</td>${names.map(k=>`<td>${hm(x[k])}</td>`).join(\'\')}<td>${hm(x.F)}</td><td>${hm(x.M16)}</td><td>${hm(x.P22)}</td></tr>`).join(\'\'):\'<tr><td colspan="11">等待正式结算</td></tr>\'}\nasync function loadPool(){if(poolLoading)return;poolLoading=true;try{const r=await fetch(\'/api/model-pool\',{cache:\'no-store\'});if(!r.ok)throw Error(r.status);renderPool((await r.json()).model_pool)}catch(e){}finally{poolLoading=false}}\nasync function loadMain(){if(loading)return;loading=true;try{const r=await fetch(\'/api/prediction\',{cache:\'no-store\'});if(!r.ok)throw Error(r.status);const d=await r.json();current=d;$(\'issue\').textContent=d.issue||\'暂无\';$(\'nextIssue\').textContent=d.next_issue||\'--\';$(\'total\').textContent=`历史 ${Number(d.count||0).toLocaleString()} 期`;$(\'latestNums\').innerHTML=balls(d.latest_numbers,true);$(\'ingest\').textContent=`${d.latest_special_zodiac||\'\'} · ${d.stale_prediction?\'下一期重算中\':d.recalculating?\'后台更新中\':d.prediction_locked?\'下一期已正式锁单\':\'等待正式锁单\'} · ${d.latest_created_at||\'\'}`;\nconst config=[[\'算法22\',\'special20\',\'22\',d.statsF||{},\'F\'],[\'算法16\',\'special16\',\'16\',d.stats16||{},\'M16\'],[\'多策略22\',\'pool22\',\'22\',d.statsPool22||{},\'P22\']];\nconst key=JSON.stringify({groups:config,codes:[d.special20,d.special16,d.pool22],locked:d.group_locked});if(key!==groupsKey){$(\'groups\').innerHTML=config.map(([label,key,k,s,group])=>{const life=s.lifetime||{};return `<div class="group"><div class="head"><h2>${label}码</h2><span class="muted">${(d.group_locked||{})[group]?\'已锁单\':\'待锁单\'}</span><button onclick="copyGroup(\'${key}\')">复制</button></div><div class="balls">${balls(d[key]||[])}</div><div class="record">${record(s)} · 累计中${esc(life.hits??0)}/错${esc(life.misses??0)}</div></div>`}).join(\'\');groupsKey=key}\n$(\'pingte\').textContent=d.pingte_yixiao||\'--\';const ps=d.pingte_b20_stats||{};$(\'pingteRecord\').textContent=`动态更新 · 实盘中${ps.hits??0} · 错${ps.misses??0}`;renderPool(d.model_pool);\n$(\'research\').textContent=\'只统计真实开奖前锁单，不回填旧期预测；目前未证明预测优势。\';\n$(\'connection\').textContent=\'已连接\';if($(\'historyDetails\').open)loadHistory();}catch(e){$(\'connection\').textContent=\'连接中断，正在重试\'}finally{loading=false}}\nasync function loadHistory(){if(historyLoading)return;historyLoading=true;try{const r=await fetch(\'/api/history?limit=100\',{cache:\'no-store\'});if(!r.ok)throw Error(r.status);const d=await r.json();$(\'historyCount\').textContent=`共${Number(d.total||0).toLocaleString()}期`;$(\'historyList\').innerHTML=(d.items||[]).map(x=>`<div class="historyRow"><span class="muted">${esc(x.issue)}</span><div class="balls">${balls(x.numbers,true)}</div></div>`).join(\'\')}catch(e){$(\'historyCount\').textContent=\'加载失败，可重新展开\'}finally{historyLoading=false}}\n$(\'historyDetails\').addEventListener(\'toggle\',()=>{if($(\'historyDetails\').open)loadHistory()});loadMain();loadPool();setInterval(loadMain,5000);setInterval(loadPool,15000);\n</script></body></html>\n'


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
        c.execute("""CREATE TABLE IF NOT EXISTS rule_counterfactual(
          draw_id TEXT NOT NULL,
          group_id TEXT NOT NULL,
          model_version TEXT NOT NULL,
          rule_id TEXT NOT NULL,
          rule_version TEXT NOT NULL,
          experiment_id TEXT NOT NULL,
          cohort_start_draw TEXT,
          history_cutoff_id TEXT NOT NULL,
          locked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          locked_before_draw INTEGER NOT NULL DEFAULT 1,
          base_code_count INTEGER NOT NULL,
          rule_code_count INTEGER NOT NULL,
          base_codes TEXT NOT NULL,
          rule_codes TEXT NOT NULL,
          result_special INTEGER,
          base_hit INTEGER,
          rule_hit INTEGER,
          rescue INTEGER,
          harm INTEGER,
          same_win INTEGER,
          same_lose INTEGER,
          net_contribution INTEGER,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          settled INTEGER DEFAULT 0,
          PRIMARY KEY(draw_id,group_id,rule_id,rule_version)
        )""")
        c.execute("""CREATE INDEX IF NOT EXISTS idx_rule_cf_rule
                     ON rule_counterfactual(group_id,rule_id,rule_version,settled,draw_id)""")
        c.execute("""CREATE TABLE IF NOT EXISTS v68_forecast(
          target_issue TEXT NOT NULL,
          ranker_id TEXT NOT NULL,
          model_version TEXT NOT NULL,
          history_cutoff_id TEXT NOT NULL,
          locked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          probs_json TEXT NOT NULL,
          ranked49 TEXT NOT NULL,
          top16 TEXT NOT NULL,
          top22 TEXT NOT NULL,
          top27 TEXT NOT NULL,
          settled INTEGER DEFAULT 0,
          actual_special INTEGER,
          actual_rank INTEGER,
          log_loss REAL,
          brier REAL,
          hit16 INTEGER,
          hit22 INTEGER,
          hit27 INTEGER,
          PRIMARY KEY(target_issue,ranker_id,model_version)
        )""")
        c.execute("""CREATE INDEX IF NOT EXISTS idx_v68_forecast_ranker
                     ON v68_forecast(ranker_id,model_version,settled,target_issue)""")
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
            "ALTER TABLE ai_model ADD COLUMN last_refit_seconds REAL DEFAULT 0.0",
            "ALTER TABLE strategy_audit ADD COLUMN snapshot_json TEXT"
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
    """Fill missing archived issues; existing live rows always take precedence."""
    path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "history.csv")
    if not os.path.exists(path):
        return 0
    with db_lock:
        c=connect()
        try:
            before=c.total_changes
            with open(path,"r",encoding="utf-8-sig",newline="") as f:
                for r in csv.DictReader(f):
                    issue=(r.get("期号") or "").strip()
                    try:
                        nums=[int(r[k]) for k in ["正码1","正码2","正码3","正码4","正码5","正码6","特码"]]
                    except (KeyError, TypeError, ValueError):
                        continue
                    if not issue.isdigit() or len(set(nums))!=7 or not all(1<=n<=49 for n in nums):
                        continue
                    zs=[normalize_z((r.get(k) or "").strip()) for k in
                        ["正码1生肖","正码2生肖","正码3生肖","正码4生肖","正码5生肖","正码6生肖"]]
                    zs.append(normalize_z((r.get("生肖") or "").strip()))
                    c.execute("""INSERT OR IGNORE INTO draws
                    (issue,n1,n2,n3,n4,n5,n6,special,z1,z2,z3,z4,z5,z6,z7,raw)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",[issue,*nums,*zs,"history.csv"])
            imported=c.total_changes-before
            c.commit()
            print(f"[MIGRATION] archive missing rows merged={imported}; existing rows preserved",flush=True)
            return imported
        finally:
            c.close()


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

def migration_high_water(count, issue):
    """Remember known pre-upgrade history requirements, including remote totals."""
    auto_state["migration_required_count"]=max(
        int(auto_state.get("migration_required_count") or 0), int(count or 0))
    issue=str(issue or "")
    previous=str(auto_state.get("migration_required_issue") or "")
    if issue.isdigit() and (not previous.isdigit() or int(issue)>int(previous)):
        auto_state["migration_required_issue"]=issue


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

        remote_latest=max((str(x.get("issue") or x.get("期号") or "")
                           for x in items if isinstance(x,dict) and
                           str(x.get("issue") or x.get("期号") or "").isdigit()),
                          key=int, default="")
        remote_total=payload.get("total",len(items)) if isinstance(payload,dict) else len(items)
        migration_high_water(remote_total,remote_latest)
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
        # A valid response can still contain only a truncated recent window.
        # Merge every distinct candidate; never replace an existing issue.
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
    """v67.4: a new draw must never blank F/A/B/C/D.

    The previous versions explicitly replaced all code arrays with [] here.
    That is the exact reason the page showed the new issue number but '--码'.
    We now publish a fresh lightweight BASE immediately after the draw is in DB.
    """
    try:
        snap=_v674_minimal_snapshot()
    except Exception as e:
        snap=None
        print(f"[PATCH68] minimal snapshot failed: {type(e).__name__}: {e}",flush=True)

    if isinstance(snap,dict) and str(snap.get("issue") or "")==str(issue):
        snap["latest_numbers"]=list(nums)
        snap["latest_special_zodiac"]=normalize_z(zs[6] if len(zs)>=7 else "")
        snap["latest_created_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
        snap["recalculating"]=True
        snap["fast_base_ready"]=True
        with live_cache_lock:
            live_cache["issue"]=str(issue)
            live_cache["data"]=snap
            live_cache["building"]=False
        return

    # Last-resort: keep the last visible codes rather than clearing them.
    # Mark them stale so the API immediately rebuilds on the next request.
    with live_cache_lock:
        data=dict(live_cache.get("data") or {})
        try:
            next_issue=_next_issue_id(issue)
        except Exception:
            next_issue=""
        data.update({
            "issue":str(issue),"next_issue":next_issue,"latest_numbers":list(nums),
            "latest_special_zodiac":normalize_z(zs[6] if len(zs)>=7 else ""),
            "latest_created_at":time.strftime("%Y-%m-%d %H:%M:%S"),
            "recalculating":True,"fast_base_ready":False,"stale_prediction":True
        })
        live_cache["issue"]=str(issue)
        live_cache["data"]=data
        live_cache["building"]=False

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
        # 1) Visible BASE first.  Do not make the UI wait for AI/Shadow work.
        try:
            refresh_history_cache()
        except Exception:
            pass
        rebuild_fast_live_cache()

        # 2) Train AI on the just-finished issue in the same background worker.
        train_ai_after_new_draw(issue, nums)
        auto_state["last_learning_issue"]=str(issue)

        # 3) Rebuild the full dashboard; official BASE is already visible.
        refresh_all_caches()

        # 4) Refresh learner/60-issue stats.
        _refresh_learning_and_stats()

        # 5) Record next-issue status if cache rebuild did not already do it.
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
    """Six deliberately different specialist models.

    Visible names:
      趋势 / 生肖 / 冷热 / 波色 / 单双 / 纠错

    波色 and 单双 are intentionally independent in v52.
    """
    nums=list(range(1,50))
    zmap=_number_zodiac_map(r)

    # 趋势
    t_raw,_zm,_zh,_ctx,_reg=_trend_only_number_scores(r,profile,strategy)
    T=_norm_values(t_raw,nums)

    # 生肖
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

    # 冷热
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

    tr=_trend_profiles(r)

    # 波色：只研究红/蓝/绿，不再混入单双。
    wv=_norm_values(tr.get("wave",{}),["红","蓝","绿"])
    W={n:wv.get(wave_of(n),.5) for n in nums}
    W=_norm_values(W,nums)

    # 单双：独立模型，只研究单/双走势。
    pa=_norm_values(tr.get("parity",{}),["单","双"])
    P={n:pa.get(parity_of(n),.5) for n in nums}
    P=_norm_values(P,nums)

    # 纠错
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

    return {"T":T,"Z":Z,"C":C,"W":W,"P":P,"A":A}

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




def _pool_dashboard():
    perf=_pool_performance()
    output_profiles={"F":F_CURRENT_PROFILE,"M16":CURRENT16_PROFILE,"P22":POOL22_PROFILE}
    profiles=[*POOL_MODEL_PROFILES.values(),*output_profiles.values()]
    placeholders=",".join("?" for _ in profiles)
    with db_lock:
        c=connect()
        try:
            rows=c.execute(f"""SELECT target_issue,profile,hit24,actual_special
                               FROM prediction_log
                               WHERE profile IN ({placeholders}) AND settled=1
                               ORDER BY CAST(target_issue AS INTEGER) DESC
                               LIMIT 540""",tuple(profiles)).fetchall()
        finally: c.close()
    prof_to_key={v:POOL_MODEL_DISPLAY[k] for k,v in POOL_MODEL_PROFILES.items()}
    prof_to_key.update({v:k for k,v in output_profiles.items()})
    by_issue={}
    for x in rows:
        row=by_issue.setdefault(str(x["target_issue"]),{"issue":str(x["target_issue"]),"actual":x["actual_special"]})
        row[prof_to_key[x["profile"]]]=int(x["hit24"]) if x["hit24"] is not None else None
    stats={POOL_MODEL_DISPLAY[k]:dict(v,name=POOL_MODEL_DISPLAY[k])
           for k,v in perf.get("stats",{}).items()}
    recent=[by_issue[i] for i in sorted(by_issue,key=int,reverse=True)[:20]]
    return {"stats":stats,"recent20":recent,"recent10":recent[:10],
            "mature_n":perf.get("mature_n",0),
            "model_names":[POOL_MODEL_DISPLAY[k] for k in POOL_MODEL_PROFILES],
            "legacy_errors":{}}








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









































def _audit_snapshot_load(raw):
    try:
        v=json.loads(raw or "{}")
        return v if isinstance(v,dict) else {}
    except Exception:
        return {}













def _long27_v7_choose_filter(r,profile,forced_head=None):
    """Choose the two weak wave×parity groups with cold-3 feasibility.

    Priority:
      1) weaker pair by previous-20 frequency;
      2) every cold zodiac must have >=2 codes from those killed groups;
      3) prefer solutions that also avoid the killed 0/4 head.

    If no pair can satisfy the head restriction, the pair must still satisfy
    the user's stricter rule: each cold zodiac has 2 codes from the killed
    wave×parity groups. Head collision is then recorded explicitly.
    """
    base=_long27_v4_window20(r)
    if forced_head in ("0头","4头",""):
        base["killed_head"]=forced_head

    counts=base.get("wave_counts") or {}
    killed_head=str(base.get("killed_head") or "")
    cold3=_long27_v4_cold3(r,profile)
    zmap=_number_zodiac_map(r)
    keys=list(WAVE_PARITY_KEYS)

    pairs=[]
    for i in range(len(keys)):
        for j in range(i+1,len(keys)):
            pair=[keys[i],keys[j]]
            pset=set(pair)
            safe_counts={}
            raw_counts={}
            for z in cold3:
                raw=[
                  n for n in range(1,50)
                  if zmap.get(n)==z and wave_parity_of(n) in pset
                ]
                safe=[
                  n for n in raw
                  if not killed_head or head_of(n)!=killed_head
                ]
                raw_counts[z]=len(raw)
                safe_counts[z]=len(safe)

            weak_sum=int(counts.get(pair[0],0))+int(counts.get(pair[1],0))
            weak_max=max(int(counts.get(pair[0],0)),int(counts.get(pair[1],0)))
            pairs.append({
              "pair":pair,
              "weak_sum":weak_sum,
              "weak_max":weak_max,
              "safe_counts":safe_counts,
              "raw_counts":raw_counts,
              "safe_ok":all(v>=2 for v in safe_counts.values()),
              "raw_ok":all(v>=2 for v in raw_counts.values()),
              "min_safe":min(safe_counts.values()) if safe_counts else 0,
              "min_raw":min(raw_counts.values()) if raw_counts else 0,
            })

    # Weakest feasible pair wins.
    safe=[x for x in pairs if x["safe_ok"]]
    if safe:
        chosen=min(safe,key=lambda x:(x["weak_sum"],x["weak_max"],x["pair"]))
        mode="冷三肖每肖2码+杀头均可满足"
    else:
        raw=[x for x in pairs if x["raw_ok"]]
        if raw:
            chosen=min(raw,key=lambda x:(x["weak_sum"],x["weak_max"],x["pair"]))
            mode="冷三肖每肖2码可满足；部分码需覆盖杀头"
        else:
            # Mathematical fallback: maximize cold-zodiac coverage, then keep
            # the weakest available pair. Never substitute non-killed groups.
            chosen=min(
              pairs,
              key=lambda x:(-x["min_raw"],x["weak_sum"],x["weak_max"],x["pair"])
            )
            mode="无完整可行双类；保留最接近的两弱类并暴露缺口"

    base["killed_wave_parity"]=list(chosen["pair"])
    base["coldest3"]=cold3
    base["pair_feasibility"]={
      "mode":mode,
      "safe_ok":bool(chosen["safe_ok"]),
      "raw_ok":bool(chosen["raw_ok"]),
      "safe_counts":chosen["safe_counts"],
      "raw_counts":chosen["raw_counts"],
      "weak_sum":chosen["weak_sum"],
      "all_pairs_checked":len(pairs)
    }
    return base











































# ===================== v67 frozen-live / atomic-shadow layer =====================



def _predict_f_v67(r, profile):
    pool_score, _mods, _perf = _pool_ensemble_score(r,profile,"20")
    ranked=sorted(range(1,50),key=lambda n:(-pool_score.get(n,-1e9),n))
    base=ranked[:22]
    models,perf,ranks,support,weighted,_top15=_f_consensus_map(r,profile)
    old_codes,old_meta=_predict20_hot(r,profile)

    # 4+/6 only, fixed 22 seats.
    four=[n for n in ranked if support.get(n,0)>=4]
    v4=_v67_force_include(base,four,ranked,22)

    # Strong 3/6 only, fixed 22 seats, at most one swap.
    trio_stats,trio_median,trio_q75,trio_baseline=_f_trio_value()
    strong=[]
    boundary=float(pool_score.get(base[-1],0.0))
    for n in ranked[22:]:
        if support.get(n,0)!=3: continue
        supporters=sorted(k for k in models if ranks[k].get(n,99)<=20)
        if len(supporters)!=3: continue
        trio_name="+".join(supporters)
        tv=float((trio_stats.get(trio_name) or {}).get("value",trio_baseline))
        gap=max(0.0,boundary-float(pool_score.get(n,0.0)))
        ok=(gap<=.035) and ((tv>=trio_q75) or (tv>=trio_median+.008 and float(weighted.get(n,0))>=.48))
        if ok: strong.append((tv,float(weighted.get(n,0)),float(pool_score.get(n,0)),n,trio_name))
    strong.sort(reverse=True)
    v3=list(base)
    if strong:
        n=strong[0][3]
        drop=max(v3,key=lambda x:ranked.index(x))
        v3[v3.index(drop)]=n
        v3=sorted(v3,key=lambda x:ranked.index(x))

    # Qualified 2/6 only, fixed 22 seats, at most one swap.
    pair_stats,pair_median,pair_baseline=_f_pair_value()
    twos=[]
    for n in ranked[22:]:
        if support.get(n,0)!=2: continue
        supporters=sorted(k for k in models if ranks[k].get(n,99)<=20)
        if len(supporters)!=2: continue
        pair_name="+".join(supporters)
        pv=float((pair_stats.get(pair_name) or {}).get("value",pair_baseline))
        gap=max(0.0,boundary-float(pool_score.get(n,0.0)))
        if gap<=.022 and pv>=pair_median+.015:
            twos.append((pv,float(pool_score.get(n,0)),n,pair_name))
    twos.sort(reverse=True)
    v2=list(base)
    if twos:
        n=twos[0][2]
        drop=max(v2,key=lambda x:ranked.index(x))
        v2[v2.index(drop)]=n
        v2=sorted(v2,key=lambda x:ranked.index(x))

    meta={
      "mode":"v67 F固定22原始集成Top22","code_count":22,"ranked49":ranked,
      "dynamic_count":22,"regime":"v67 F BASE固定Top22","audit_regime":"F67|base22",
      "consensus":{"protected4_count":len(four),"protected4":[f"{n:02d}" for n in four],
                   "protected4_actual_count":sum(1 for n in four if n in base)},
      "shadow4":v4,"shadow3":v3,"shadow2":v2,"legacy_v66":list(old_codes),
      "strong3_candidate":f"{strong[0][3]:02d}" if strong else "",
      "limit2_candidate":f"{twos[0][2]:02d}" if twos else "",
      "old_meta":old_meta
    }
    return base,meta,{"4vote":v4,"strong3":v3,"limit2":v2,"legacy":list(old_codes)}

def _predict_a_v67(r,profile,target_issue):
    pool_score,_models,perf=_pool_ensemble_score(r,profile,"27")
    ranked=sorted(range(1,50),key=lambda n:(-pool_score.get(n,-1e9),n))
    base=ranked[:27]
    hd=_pool_5head_kill_decision(r,profile,"27")
    killed=str(hd.get("killed_head") or "")
    kill=[n for n in ranked if not killed or head_of(n)!=killed][:27]
    fixed,fmeta=_v67_fixed_round_codes(A_FIXED_ROUND_V67_PROFILE,target_issue,base,10)
    legacy,lmeta=_predict27_tenblock_v62(r,profile,target_issue)
    meta={"mode":"v67 A原始Top27","ranked49":ranked,"raw_top27":base,"code_count":27,
          "killed_head":"","head_decision":{"active":False,"reason":"Live不杀码"},
          "block_start":"","block_end":"","round_position":1,
          "regime":"v67 A BASE Top27","audit_regime":"A67|base27",
          "shadow_killed_head":killed,"fixed_round_meta":fmeta}
    return base,meta,{"kill_head":kill,"fixed_round":fixed,"legacy":list(legacy)}

def _predict_b_v67(r,profile):
    legacy,lmeta=_predict27_dynamic_b(r,profile)
    ranked=list(lmeta.get("ranked49") or [])
    if not ranked:
        sc={int(k):float(v) for k,v in (lmeta.get("score49") or {}).items()}
        ranked=sorted(range(1,50),key=lambda n:(-sc.get(n,-1e9),n))
    base=ranked[:27]
    killed=str(lmeta.get("killed_head") or "")
    missing=list(lmeta.get("missing_tails") or [])
    head=[n for n in ranked if not killed or head_of(n)!=killed][:27]
    tail,tail_changes=_v67_tail_rescue(base,ranked,missing,27,"")
    both,both_changes=_v67_tail_rescue(head,ranked,missing,27,killed)
    meta=dict(lmeta)
    meta.update({"mode":"v67 B原始近10Top27","killed_head":"","missing_tail_rescue":{},
                 "regime":"v67 B BASE近10 Top27","audit_regime":"B67|base27",
                 "atomic_tail_changes":tail_changes,"atomic_tail_head_changes":both_changes,
                 "shadow_killed_head":killed})
    return base,meta,{"tail_only":tail,"head_only":head,"tail_head":both,"legacy":list(legacy)}

def _predict_c_v67(r,profile):
    _bleg,bmeta=_predict27_dynamic_b(r,profile)
    legacy,lmeta=_predict22_dynamic_c(r,profile)
    ranked=list(bmeta.get("ranked49") or lmeta.get("ranked49") or list(range(1,50)))
    base=ranked[:22]
    missing=list(bmeta.get("missing_tails") or [])
    killed=str(bmeta.get("killed_head") or "")
    tail,tail_changes=_v67_tail_rescue(base,ranked,missing,22,"")

    # Head/cold structural score only; tail rescue excluded.
    score22={int(k):float(v) for k,v in (lmeta.get("score22_base") or {}).items()}
    if score22:
        coldhead_rank=sorted(ranked,key=lambda n:(-score22.get(n,-1e9),n))
    else:
        coldhead_rank=list(lmeta.get("ranked49") or ranked)
    coldhead=coldhead_rank[:22]
    tail_cold,tc_changes=_v67_tail_rescue(coldhead,coldhead_rank,missing,22,killed)
    meta={"mode":"v67 C原始近10Top22","ranked49":ranked,"code_count":22,"killed_head":"",
          "coldest3":[],"missing_tails":missing,"missing_tail_rescue":{},
          "regime":"v67 C BASE近10 Top22","audit_regime":"C67|base22",
          "atomic_tail_changes":tail_changes,"atomic_tail_cold_changes":tc_changes,
          "shadow_killed_head":killed}
    return base,meta,{"tail_only":tail,"coldhead_only":coldhead,"tail_coldhead":tail_cold,"legacy":list(legacy)}


def _predict_d_v67(r,profile):
    """D v67.3: neutral BASE first; Shadows are isolated and optional.

    A Shadow failure must never prevent the pre-draw D BASE from being locked.
    """
    nums=list(range(1,50))
    neutral_score,_neutral_models,neutral_weights=_d_neutral_score_from_pool(r,profile)
    ranked=sorted(nums,key=lambda n:(-neutral_score.get(n,-1e9),n))
    base=ranked[:16]
    wave_counts=dict(Counter(wave_of(n) for n in base))
    max_wave=max(wave_counts.values()) if wave_counts else 0
    anomaly=("D BASE波色集中告警：单一波色>=14/16，仅诊断、不强制配额" if max_wave>=14 else "")

    meta={
      "mode":"v67.3 D中性BASE Top16",
      "ranked49":ranked,"code_count":16,
      "fused_wave":{},"fused_size":{},
      "wave_counts":wave_counts,
      "neutral_weights_pct":{k:round(100*v,1) for k,v in neutral_weights.items()},
      "anomaly":anomaly,
      "regime":"v67.3 D中性BASE Top16（Z/C/P/A）",
      "audit_regime":"D673|neutral-base16",
      "shadow_errors":[]
    }
    shadows={}

    # Old v66 soft layer is Shadow-only.  Correctly receive all four returns.
    try:
        _soft,soft_meta,_old_codes,_old_meta=_predict16_d_v66(r,profile)
        fw={k:float(v)/100.0 for k,v in (soft_meta.get("fused_wave") or {}).items()}
        fs={k:float(v)/100.0 for k,v in (soft_meta.get("fused_size") or {}).items()}
        meta["fused_wave"]=soft_meta.get("fused_wave") or {}
        meta["fused_size"]=soft_meta.get("fused_size") or {}

        def take(wave_w,size_w):
            base_w=max(0.0,1.0-wave_w-size_w)
            sc={n:base_w*float(neutral_score.get(n,.5))
                  +wave_w*fw.get(wave_of(n),.5)
                  +size_w*fs.get(size_of(n),.5) for n in nums}
            return sorted(nums,key=lambda n:(-sc[n],n))[:16]

        shadows["color_only"]=take(.16,0.0)
        shadows["size_only"]=take(0.0,.16)
        shadows["color_size"]=take(.10,.10)
    except Exception as e:
        meta["shadow_errors"].append(f"soft:{type(e).__name__}:{e}")

    try:
        legacy,_lmeta=_predict16_dynamic_d(r,profile)
        shadows["legacy"]=list(legacy)
    except Exception as e:
        meta["shadow_errors"].append(f"legacy:{type(e).__name__}:{e}")

    return base,meta,shadows



# ===================== v67.4 immediate frozen BASE =====================
# Keep references to the older v67 rule implementations.  They are used ONLY
# to generate Shadows; the official BASE is now a lightweight, deterministic,
# attribute-neutral ranking so display, locking and settlement always agree.
_v673_predict_f_shadow_impl = _predict_f_v67
_v673_predict_a_shadow_impl = _predict_a_v67
_v673_predict_b_shadow_impl = _predict_b_v67
_v673_predict_c_shadow_impl = _predict_c_v67
_v673_predict_d_shadow_impl = _predict_d_v67

def _v674_ranked(r, window=60):
    """Fast neutral ranker for official BASE.

    Uses only pre-draw special-number frequency and omission across the chosen
    window.  No wave/color, size, head, zodiac, tail rescue or kill rule is used.
    """
    nums=list(range(1,50))
    recent=list(r[:max(1,int(window))])
    if not recent:
        return nums[:]
    freq=Counter()
    gap={n:len(recent) for n in nums}
    for i,x in enumerate(recent):
        try:
            n=int(x["special"])
        except Exception:
            continue
        # Mild recency weighting, deliberately simple and frozen.
        w=exp_weight(i, max(2.0, float(window)/6.0))
        freq[n]+=w
        if gap[n]==len(recent):
            gap[n]=i
    fn=_norm_values(freq,nums)
    gn=_norm_values(gap,nums)
    score={n:.60*float(fn.get(n,.5))+.40*float(gn.get(n,.5)) for n in nums}
    return sorted(nums,key=lambda n:(-score[n],n))

def _v674_official_bases(r):
    long_rank=_v674_ranked(r,60)
    short_rank=_v674_ranked(r,10)
    return {
      "F":long_rank[:22],
      "A":long_rank[:27],
      "B":short_rank[:27],
      "C":short_rank[:22],
      "D":long_rank[:16],
      "rank_long":long_rank,
      "rank_short":short_rank,
    }

def _v674_default_shadows(base, keys):
    return {k:list(base) for k in keys}

def _predict_f_v67(r, profile):
    b=_v674_official_bases(r)
    base=list(b["F"]); ranked=list(b["rank_long"])
    shadows=_v674_default_shadows(base,["4vote","strong3","limit2","legacy"])
    shadow_error=""
    try:
        _oldbase,_oldmeta,oldsh=_v673_predict_f_shadow_impl(r,profile)
        for k in shadows:
            vals=list((oldsh or {}).get(k) or [])
            if len(vals)==22: shadows[k]=vals
    except Exception as e:
        shadow_error=f"{type(e).__name__}:{e}"
    meta={"mode":"v67.4 F即时中性Top22","code_count":22,"ranked49":ranked,
          "dynamic_count":22,"regime":"v67.4 F BASE中性Top22","audit_regime":"F674|neutral22",
          "consensus":{"protected4_count":0,"protected4":[],"protected4_actual_count":0},
          "shadow_error":shadow_error}
    return base,meta,shadows

def _predict_a_v67(r,profile,target_issue):
    b=_v674_official_bases(r)
    base=list(b["A"]); ranked=list(b["rank_long"])
    shadows=_v674_default_shadows(base,["kill_head","fixed_round","legacy"])
    shadow_error=""
    try:
        _oldbase,_oldmeta,oldsh=_v673_predict_a_shadow_impl(r,profile,target_issue)
        for k in shadows:
            vals=list((oldsh or {}).get(k) or [])
            if len(vals)==27: shadows[k]=vals
    except Exception as e:
        shadow_error=f"{type(e).__name__}:{e}"
    meta={"mode":"v67.4 A即时中性Top27","ranked49":ranked,"raw_top27":base,
          "code_count":27,"killed_head":"","head_decision":{"active":False,"reason":"BASE不杀码"},
          "block_start":"","block_end":"","round_position":1,
          "regime":"v67.4 A BASE中性Top27","audit_regime":"A674|neutral27",
          "shadow_error":shadow_error}
    return base,meta,shadows

def _predict_b_v67(r,profile):
    b=_v674_official_bases(r)
    base=list(b["B"]); ranked=list(b["rank_short"])
    shadows=_v674_default_shadows(base,["tail_only","head_only","tail_head","legacy"])
    shadow_error=""
    try:
        _oldbase,_oldmeta,oldsh=_v673_predict_b_shadow_impl(r,profile)
        for k in shadows:
            vals=list((oldsh or {}).get(k) or [])
            if len(vals)==27: shadows[k]=vals
    except Exception as e:
        shadow_error=f"{type(e).__name__}:{e}"
    meta={"mode":"v67.4 B即时近10中性Top27","ranked49":ranked,"code_count":27,
          "killed_head":"","missing_tail_rescue":{},
          "regime":"v67.4 B BASE近10中性Top27","audit_regime":"B674|neutral10-27",
          "shadow_error":shadow_error}
    return base,meta,shadows

def _predict_c_v67(r,profile):
    b=_v674_official_bases(r)
    base=list(b["C"]); ranked=list(b["rank_short"])
    shadows=_v674_default_shadows(base,["tail_only","coldhead_only","tail_coldhead","legacy"])
    shadow_error=""
    try:
        _oldbase,_oldmeta,oldsh=_v673_predict_c_shadow_impl(r,profile)
        for k in shadows:
            vals=list((oldsh or {}).get(k) or [])
            if len(vals)==22: shadows[k]=vals
    except Exception as e:
        shadow_error=f"{type(e).__name__}:{e}"
    meta={"mode":"v67.4 C即时近10中性Top22","ranked49":ranked,"code_count":22,
          "killed_head":"","coldest3":[],"missing_tails":[],"missing_tail_rescue":{},
          "regime":"v67.4 C BASE近10中性Top22","audit_regime":"C674|neutral10-22",
          "shadow_error":shadow_error}
    return base,meta,shadows

def _predict_d_v67(r,profile):
    b=_v674_official_bases(r)
    base=list(b["D"]); ranked=list(b["rank_long"])
    wave_counts=dict(Counter(wave_of(n) for n in base))
    max_wave=max(wave_counts.values()) if wave_counts else 0
    anomaly=("D BASE波色集中告警：单一波色>=14/16，仅诊断、不强制配额" if max_wave>=14 else "")
    shadows=_v674_default_shadows(base,["color_only","size_only","color_size","legacy"])
    shadow_error=""
    try:
        _oldbase,_oldmeta,oldsh=_v673_predict_d_shadow_impl(r,profile)
        for k in shadows:
            vals=list((oldsh or {}).get(k) or [])
            if len(vals)==16: shadows[k]=vals
    except Exception as e:
        shadow_error=f"{type(e).__name__}:{e}"
    meta={"mode":"v67.4 D即时中性Top16","ranked49":ranked,"code_count":16,
          "fused_wave":{},"fused_size":{},"wave_counts":wave_counts,
          "neutral_weights_pct":{},"anomaly":anomaly,
          "regime":"v67.4 D BASE中性Top16（不含波色/大小）",
          "audit_regime":"D674|neutral16","shadow_errors":[shadow_error] if shadow_error else []}
    return base,meta,shadows



# ===================== v68: 49-number calibrated-probability architecture =====================
def _v68_weighted_posterior(r, window, half_life, alpha_each=1.25):
    """Dirichlet-smoothed predictive distribution using ONLY prior draws.

    The result is a genuine 49-class probability vector that sums to 1.
    No zodiac/wave/head/tail/size kill rule is allowed in the official BASE.
    """
    nums=list(range(1,50))
    recent=list(r[:max(1,int(window))])
    counts={n:0.0 for n in nums}
    total=0.0
    hl=max(1e-6,float(half_life))
    for i,x in enumerate(recent):
        try: n=int(x["special"])
        except Exception: continue
        if not (1 <= n <= 49): continue
        w=0.5**(float(i)/hl)
        counts[n]+=w; total+=w
    den=total+49.0*float(alpha_each)
    if den<=0:
        return {n:1.0/49.0 for n in nums}
    return {n:(counts[n]+float(alpha_each))/den for n in nums}

def _v68_mix_prob(*weighted):
    nums=list(range(1,50))
    out={n:0.0 for n in nums}; sw=0.0
    for weight,prob in weighted:
        w=max(0.0,float(weight)); sw+=w
        for n in nums: out[n]+=w*float((prob or {}).get(n,0.0))
    if sw<=0: return {n:1.0/49.0 for n in nums}
    out={n:max(1e-15,v/sw) for n,v in out.items()}
    z=sum(out.values()) or 1.0
    return {n:v/z for n,v in out.items()}


def _v68_ranked(prob):
    return sorted(range(1,50),key=lambda n:(-float((prob or {}).get(n,0.0)),n))





def _v68_settle_forecasts(issue,actual_special):
    a=int(actual_special)
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT ranker_id,probs_json,ranked49,top16,top22,top27
                              FROM v68_forecast WHERE target_issue=? AND model_version=? AND settled=0""",
                           (str(issue),V68_MODEL_VERSION)).fetchall()
            for row in rows:
                try: probs={int(k):float(v) for k,v in json.loads(row["probs_json"]).items()}
                except Exception: probs={n:1.0/49.0 for n in range(1,50)}
                ranked=_csv_nums(row["ranked49"])
                rank=(ranked.index(a)+1) if a in ranked else 49
                pa=max(float(probs.get(a,1.0/49.0)),1e-15)
                ll=-math.log(pa)
                br=sum((float(probs.get(n,0.0))-(1.0 if n==a else 0.0))**2 for n in range(1,50))
                h16=int(a in set(_csv_nums(row["top16"]))); h22=int(a in set(_csv_nums(row["top22"]))); h27=int(a in set(_csv_nums(row["top27"])))
                c.execute("""UPDATE v68_forecast SET settled=1,actual_special=?,actual_rank=?,log_loss=?,brier=?,hit16=?,hit22=?,hit27=?
                             WHERE target_issue=? AND ranker_id=? AND model_version=?""",
                          (a,rank,float(ll),float(br),h16,h22,h27,str(issue),row["ranker_id"],V68_MODEL_VERSION))
            c.commit()
        finally: c.close()











# ===================== v69: Null-First / evidence-gated architecture =====================
def _v69_uniform_prob():
    return {n:1.0/49.0 for n in range(1,50)}





def _v69_settle_forecasts(issue,actual_special):
    a=int(actual_special)
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT ranker_id,probs_json,ranked49,top16,top22,top27
                              FROM v68_forecast WHERE target_issue=? AND model_version=? AND settled=0""",
                           (str(issue),V69_MODEL_VERSION)).fetchall()
            for row in rows:
                rid=str(row["ranker_id"])
                try: probs={int(k):float(v) for k,v in json.loads(row["probs_json"]).items()}
                except Exception: probs=_v69_uniform_prob()
                if rid=="U":
                    rank=25
                    h16=h22=h27=0
                else:
                    ranked=_csv_nums(row["ranked49"])
                    rank=(ranked.index(a)+1) if a in ranked else 49
                    h16=int(a in set(_csv_nums(row["top16"]))); h22=int(a in set(_csv_nums(row["top22"]))); h27=int(a in set(_csv_nums(row["top27"])))
                pa=max(float(probs.get(a,1.0/49.0)),1e-15)
                ll=-math.log(pa)
                br=sum((float(probs.get(n,0.0))-(1.0 if n==a else 0.0))**2 for n in range(1,50))
                c.execute("""UPDATE v68_forecast SET settled=1,actual_special=?,actual_rank=?,log_loss=?,brier=?,hit16=?,hit22=?,hit27=?
                             WHERE target_issue=? AND ranker_id=? AND model_version=?""",
                          (a,rank,float(ll),float(br),h16,h22,h27,str(issue),rid,V69_MODEL_VERSION))
            c.commit()
        finally: c.close()




# ===================== v71: honest forward challenger rankers =====================
def _v71_transition_prob(r, window=240, half_life=80, alpha_each=1.5):
    """First-order transition posterior P(next special | latest special).

    Uses only pairs fully contained in the pre-draw history. Heavy smoothing keeps
    the distribution near Uniform when the transition evidence is weak.
    """
    nums=list(range(1,50)); recent=list(r[:max(3,int(window)+1)])
    if len(recent)<3:
        return {n:1.0/49.0 for n in nums}
    try: current=int(recent[0]["special"])
    except Exception: return {n:1.0/49.0 for n in nums}
    counts={n:0.0 for n in nums}; total=0.0; hl=max(1e-6,float(half_life))
    # recent is newest -> oldest. Pair older(i+1) -> newer(i).
    for i in range(len(recent)-1):
        try:
            newer=int(recent[i]["special"]); older=int(recent[i+1]["special"])
        except Exception:
            continue
        if older!=current or not (1<=newer<=49):
            continue
        w=0.5**(float(i)/hl); counts[newer]+=w; total+=w
    den=total+49.0*float(alpha_each)
    return {n:(counts[n]+float(alpha_each))/den for n in nums} if den>0 else {n:1/49 for n in nums}


def _current_ranker_probs(r):
    """One fixed multiscale ranking; shared probability helpers are not model seats."""
    p60=_v68_weighted_posterior(r,60,18,1.55)
    p360=_v68_weighted_posterior(r,360,120,1.85)
    pt=_v71_transition_prob(r,420,140,2.20)
    return _v68_mix_prob((.40,p60),(.40,p360),(.20,pt))




def _v71_settle_forecasts(issue,actual_special):
    a=int(actual_special)
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT ranker_id,probs_json,ranked49,top16,top22,top27
                              FROM v68_forecast WHERE target_issue=? AND model_version=? AND settled=0""",
                           (str(issue),V71_MODEL_VERSION)).fetchall()
            for row in rows:
                try: probs={int(k):float(v) for k,v in json.loads(row["probs_json"]).items()}
                except Exception: probs={n:1.0/49.0 for n in range(1,50)}
                ranked=_csv_nums(row["ranked49"]); rank=(ranked.index(a)+1) if a in ranked else 49
                pa=max(float(probs.get(a,1.0/49.0)),1e-15); ll=-math.log(pa)
                br=sum((float(probs.get(n,0.0))-(1.0 if n==a else 0.0))**2 for n in range(1,50))
                h16=int(a in set(_csv_nums(row["top16"]))); h22=int(a in set(_csv_nums(row["top22"]))); h27=int(a in set(_csv_nums(row["top27"])))
                c.execute("""UPDATE v68_forecast SET settled=1,actual_special=?,actual_rank=?,log_loss=?,brier=?,hit16=?,hit22=?,hit27=?
                             WHERE target_issue=? AND ranker_id=? AND model_version=?""",
                          (a,rank,float(ll),float(br),h16,h22,h27,str(issue),row["ranker_id"],V71_MODEL_VERSION))
            c.commit()
        finally: c.close()









# ===================== v67.1 fast live BASE layer =====================









def _v674_minimal_snapshot():
    return build_model()


def rebuild_fast_live_cache():
    t0=time.time()
    try:
        data=_v674_minimal_snapshot()
        if not data:
            return None
        latest=latest_row()
        latest_issue=str(latest["issue"] if latest else "")
        if str(data.get("issue") or "")!=latest_issue:
            return None
        with live_cache_lock:
            live_cache["data"]=data
            live_cache["issue"]=data.get("issue")
            live_cache["building"]=False
        print(f"[FAST68] BASE ready issue={data.get('issue')} next={data.get('next_issue')} in {time.time()-t0:.3f}s",flush=True)
        return data
    except Exception as e:
        print(f"[FAST68] BASE failed: {type(e).__name__}: {e}",flush=True)
        return None





def _settle_rule_counterfactual(issue,actual_special):
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT group_id,rule_id,rule_version,base_codes,rule_codes
                              FROM rule_counterfactual WHERE draw_id=? AND settled=0""",
                           (str(issue),)).fetchall()
            for row in rows:
                base=set(_csv_nums(row["base_codes"]))
                rule=set(_csv_nums(row["rule_codes"]))
                bh=int(int(actual_special) in base); rh=int(int(actual_special) in rule)
                rescue=int(rh and not bh); harm=int(bh and not rh)
                sw=int(rh and bh); sl=int((not rh) and (not bh))
                c.execute("""UPDATE rule_counterfactual SET settled=1,result_special=?,base_hit=?,rule_hit=?,
                             rescue=?,harm=?,same_win=?,same_lose=?,net_contribution=?
                             WHERE draw_id=? AND group_id=? AND rule_id=? AND rule_version=?""",
                          (int(actual_special),bh,rh,rescue,harm,sw,sl,rescue-harm,
                           str(issue),row["group_id"],row["rule_id"],row["rule_version"]))
            c.commit()
        finally: c.close()


def _stats20round_profile(profile,target_issue=None):
    """20期一轮 + 当前策略累计真实前瞻成绩。"""
    with db_lock:
        c=connect()
        try:
            rows=c.execute("""SELECT target_issue,hit24,settled
                              FROM prediction_log
                              WHERE profile=?
                              ORDER BY CAST(target_issue AS INTEGER) ASC""",
                           (profile,)).fetchall()
        finally:
            c.close()

    items=[dict(x) for x in rows]
    target=str(target_issue or "")
    issues=[str(x["target_issue"]) for x in items]
    total=len(items)

    if target and target in issues:
        idx=issues.index(target)
        gstart=(idx//20)*20
        chunk=items[gstart:gstart+20]
        start_issue=str(chunk[0]["target_issue"]) if chunk else target
    else:
        rem=total%20
        if rem==0:
            chunk=[]
            start_issue=target or (_next_issue_id(items[-1]["target_issue"]) if items else "")
        else:
            gstart=total-rem
            chunk=items[gstart:]
            start_issue=str(chunk[0]["target_issue"])

    settled=[x for x in chunk if int(x.get("settled") or 0)==1]
    n=len(settled)
    hits=sum(int(x.get("hit24") or 0) for x in settled)
    lifetime=_profile_lifetime_stats(profile)
    return {
      "start":start_issue,
      "end":_issue_add(start_issue,19) if start_issue else "",
      "n":n,
      "hits":hits,
      "misses":max(0,n-hits),
      "rate":round(100*hits/n,1) if n else 0.0,
      "round_size":20,
      "round_complete":bool(n>=20),
      "round_no":(total//20+1),
      "lifetime":lifetime
    }

def _predict_pingte_yixiao_b20(r):
    """平特一肖 v56：真正多节奏动态，只看最新20期。

    目标是预测下一期7个开奖号里“至少出现一次”的生肖。
    每期开奖后重算，不假设“这期出现了，下期还会出现”。

    组成：
      - 近3期突变
      - 近6期走势
      - 近10期中短节奏
      - 近20期底座
      - 连续出现 / 连续遗漏修正
      - 上一期 -> 下一期生肖转移（仅用这20期内历史转移）
    """
    recent=list(r[:20])
    if not recent:
        return "",{"samples":0,"mode":"多节奏动态"}

    def draw_zset(x):
        zs={normalize_z(x[f"z{k}"] or "") for k in range(1,8)}
        zs.discard("")
        return zs

    # Precompute each draw's zodiac presence set.
    draw_sets=[draw_zset(x) for x in recent]

    # --------------------------------------------------------
    # Multi-window presence strength.
    # A zodiac counts once per draw even if it appears multiple times.
    # --------------------------------------------------------
    def presence_score(block,half):
        c=Counter()
        tw=0.0
        for i,zs in enumerate(block):
            w=exp_weight(i,half)
            tw+=w
            for z in zs:
                c[z]+=w
        return {z:(c[z]/tw if tw else 0.0) for z in ALL_ZODIACS}

    p3=presence_score(draw_sets[:3],1.35)
    p6=presence_score(draw_sets[:6],2.4)
    p10=presence_score(draw_sets[:10],4.0)
    p20=presence_score(draw_sets[:20],7.0)

    # --------------------------------------------------------
    # Current streak / omission.
    # --------------------------------------------------------
    streak={}
    omission={}
    for z in ALL_ZODIACS:
        st=0
        for zs in draw_sets:
            if z in zs:
                st+=1
            else:
                break
        streak[z]=st

        om=20
        for i,zs in enumerate(draw_sets):
            if z in zs:
                om=i
                break
        omission[z]=om

    # Streak term: mild continuation up to 2, then damp to avoid sticky picks.
    # Omission term: moderate rebound, not enough to overpower active trend.
    streak_term={}
    omission_term={}
    for z in ALL_ZODIACS:
        st=streak[z]
        if st==0:
            streak_term[z]=0.46
        elif st==1:
            streak_term[z]=0.62
        elif st==2:
            streak_term[z]=0.68
        elif st==3:
            streak_term[z]=0.60
        else:
            streak_term[z]=0.50

        om=omission[z]
        omission_term[z]=min(1.0,0.28+0.09*om)

    # --------------------------------------------------------
    # Previous draw -> next draw zodiac transition, only using recent20.
    # For each historical pair (older -> newer), if source zodiac was present,
    # count which zodiacs appeared in the following draw.
    # Current source = latest draw's zodiac set.
    # --------------------------------------------------------
    latest_set=draw_sets[0] if draw_sets else set()
    trans_counts=Counter()
    trans_total=0.0

    # recent is newest->oldest. For i from older index to next newer index:
    # source=draw_sets[i], destination=draw_sets[i-1]
    for i in range(1,len(draw_sets)):
        src=draw_sets[i]
        dst=draw_sets[i-1]
        overlap=len(src & latest_set)
        if overlap<=0:
            continue
        # More similar source sets get higher weight; recency also matters.
        w=(0.55+0.18*overlap)*exp_weight(i-1,5.5)
        trans_total+=w
        for z in dst:
            trans_counts[z]+=w

    transition={
      z:(trans_counts[z]/trans_total if trans_total else p20.get(z,0.0))
      for z in ALL_ZODIACS
    }

    # --------------------------------------------------------
    # Final score.
    # Faster windows dominate so ranking can move after each draw.
    # --------------------------------------------------------
    scores={}
    for z in ALL_ZODIACS:
        scores[z]=(
          .24*p3.get(z,0.0)
          +.22*p6.get(z,0.0)
          +.17*p10.get(z,0.0)
          +.13*p20.get(z,0.0)
          +.10*streak_term.get(z,0.5)
          +.06*omission_term.get(z,0.5)
          +.08*transition.get(z,0.0)
        )

    ranked=sorted(ALL_ZODIACS,key=lambda z:(-scores[z],z))
    top=ranked[0]
    second=ranked[1] if len(ranked)>1 else ""
    gap=(scores[top]-scores[second]) if second else 0.0

    # 0-100 relative display score for readability.
    vals=list(scores.values())
    lo=min(vals) if vals else 0.0
    hi=max(vals) if vals else 1.0
    def display_score(z):
        if hi<=lo:
            return 50.0
        return round(50.0+50.0*(scores[z]-lo)/(hi-lo),1)

    if gap>=0.055:
        confidence="强优势"
    elif gap>=0.025:
        confidence="中优势"
    else:
        confidence="弱优势"

    return top,{
      "samples":len(recent),
      "mode":"近3+6+10+20期多节奏动态",
      "ranked":ranked,
      "scores":{z:round(scores[z],5) for z in ranked},
      "display_scores":{z:display_score(z) for z in ranked},
      "top":top,
      "second":second,
      "lead_gap":round(gap,5),
      "confidence":confidence,
      "streak":streak,
      "omission":omission,
      "transition_samples":sum(1 for i in range(1,len(draw_sets)) if len(draw_sets[i] & latest_set)>0),
      "window_issues":[str(x["issue"]) for x in recent if x["issue"]]
    }





















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

                if str(row["profile"])==TEN27_V7_PROFILE:
                    snap=_audit_snapshot_load(row["snapshot_json"] if "snapshot_json" in row.keys() else "")
                    raw=set(int(x) for x in (snap.get("raw_top27") or []) if str(x).isdigit())
                    cold3=set(snap.get("coldest3") or [])
                    cold_codes=snap.get("cold3_codes") or {}
                    killed_wp=set(snap.get("killed_wave_parity") or [])
                    zcounts=snap.get("zodiac_counts") or {}
                    z=normalize_z(str(actual_zodiac or ""))
                    if hit:
                        reason="命中"
                    elif int(actual_special) not in raw:
                        reason="底层模型没抓到"
                    elif z in cold3 and int(actual_special) not in set(int(x) for x in (cold_codes.get(z) or [])):
                        reason="冷肖每肖2码没覆盖"
                    elif killed and head_success==0:
                        reason="0/4头杀错"
                    elif wave_parity_of(int(actual_special)) in killed_wp:
                        reason="波色单双杀错"
                    elif int(zcounts.get(z,0) or 0)>=3:
                        reason="生肖3码上限挤出"
                    else:
                        reason="结构截断"
                    cold_err=int(reason=="冷肖每肖2码没覆盖")
                else:
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




def _profile_lifetime_stats(profile):
    """All settled pre-draw locks for one live strategy profile."""
    with db_lock:
        c=connect()
        try:
            row=c.execute("""SELECT
                                COUNT(*) AS n,
                                COALESCE(SUM(CASE WHEN hit24=1 THEN 1 ELSE 0 END),0) AS hits
                              FROM prediction_log
                              WHERE profile=? AND settled=1""",(profile,)).fetchone()
        finally:
            c.close()
    n=int(row["n"] or 0) if row else 0
    hits=int(row["hits"] or 0) if row else 0
    return {
      "n":n,
      "hits":hits,
      "misses":max(0,n-hits),
      "rate":round(100*hits/n,1) if n else 0.0
    }





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
        _settle_rule_counterfactual(issue,actual_special)
    except Exception as e:
        print(f"[AUDIT] settle failed: {e}",flush=True)
    try:
        _v68_settle_forecasts(issue,actual_special)
        _v69_settle_forecasts(issue,actual_special)
        _v71_settle_forecasts(issue,actual_special)
        v72_evidence.settle_random(connect,db_lock,issue,actual_special)
    except Exception as e:
        print(f"[V68] forecast settle failed: {type(e).__name__}: {e}",flush=True)

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

def _pool22_ranked(specialists):
    """Independent weighted ensemble of all six specialist scores, not Q1 cuts."""
    perf=_pool_performance()
    weights=perf.get("weights") or {}
    keys=list(POOL_MODEL_PROFILES)
    total=sum(max(0.0,float(weights.get(k,1.0/len(keys)))) for k in keys) or 1.0
    score={n:sum(max(0.0,float(weights.get(k,1.0/len(keys)))) *
                 float(specialists.get(k,{}).get(n,.5)) for k in keys)/total
           for n in range(1,50)}
    return sorted(range(1,50),key=lambda n:(-score[n],n))


def record_shadow_predictions(r):
    """Lock three outputs and six specialists atomically before the next draw."""
    if not r: return
    target=_next_issue_id(r[0]["issue"])
    py,_=_predict_pingte_yixiao_b20(r)
    ranked=_v68_ranked(_current_ranker_probs(r))
    specialists=_specialist_model_scores(r,"平衡","20")
    pool22=_pool22_ranked(specialists)[:22]
    with db_lock:
        c=connect()
        try:
            # Prevent a completed outcome racing the scoring work or transaction.
            c.execute("BEGIN IMMEDIATE")
            newest=c.execute("SELECT issue FROM draws ORDER BY CAST(issue AS INTEGER) DESC LIMIT 1").fetchone()
            if newest and str(newest["issue"])!=str(r[0]["issue"]):
                return
            if c.execute("SELECT 1 FROM draws WHERE issue=?",(target,)).fetchone():
                return
            old22=c.execute("SELECT special24 FROM prediction_log WHERE target_issue=? AND profile=?",
                            (target,F_CURRENT_PROFILE)).fetchone()
            codes22=_csv_nums(old22["special24"]) if old22 else ranked[:22]
            # Existing v73 F ranking is authoritative, including its Top16 cut.
            records=[(target,F_CURRENT_PROFILE,",".join(map(str,codes22)),"","",py),
                     (target,CURRENT16_PROFILE,",".join(map(str,codes22[:16])),"","",py),
                     (target,POOL22_PROFILE,",".join(map(str,pool22)),"","",py)]
            for key,prof in POOL_MODEL_PROFILES.items():
                order=sorted(range(1,50),key=lambda n:(-specialists[key].get(n,-1e9),n))
                records.append((target,prof,",".join(map(str,order[:20])),"","",py))
            c.executemany("""INSERT OR IGNORE INTO prediction_log
                (target_issue,profile,special24,main4,zodiac4,pingte)
                VALUES (?,?,?,?,?,?)""",records)
            c.commit()
        finally: c.close()
    print(f"[V74] locked algorithm22 + algorithm16 + pool22 + six specialists target={target}",flush=True)
    threading.Thread(target=write_checkpoint_atomic,daemon=True,name="learning-checkpoint").start()







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
                              ORDER BY CAST(issue AS INTEGER) DESC""").fetchall()
            scores=c.execute("SELECT * FROM learner_scores ORDER BY profile").fetchall()
        finally:
            c.close()
    return {
      "version":"v67",
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



def initialize_quick_live_cache():
    """v67.4 boot cache: publish five BASE lists immediately; never placeholders."""
    try:
        data=_v674_minimal_snapshot()
        if not data:
            return
        with live_cache_lock:
            live_cache["issue"]=data.get("issue")
            live_cache["data"]=data
            live_cache["building"]=False
        print(f"[BOOT68] immediate BASE ready issue={data.get('issue')} next={data.get('next_issue')}",flush=True)
    except Exception as e:
        print(f"[BOOT68] immediate BASE failed: {type(e).__name__}: {e}",flush=True)

def build_model():
    """Same Q1 mother ranking for 22/16; independent six-model pool for pool22."""
    r=recent_rows(1200)
    if not r:
        return {"issue":None,"count":0,"special20":[],"special16":[],"pool22":[],"model_pool":{},"recalculating":False}
    latest=r[0]; target=_next_issue_id(latest["issue"])
    output_profiles={"F":F_CURRENT_PROFILE,"M16":CURRENT16_PROFILE,"P22":POOL22_PROFILE}
    with db_lock:
        c=connect()
        try:
            locked_rows=c.execute("""SELECT profile,special24 FROM prediction_log
                                   WHERE target_issue=? AND profile IN (?,?,?)""",
                                  (target,*output_profiles.values())).fetchall()
        finally: c.close()
    locked={x["profile"]:_csv_nums(x["special24"]) for x in locked_rows}
    codes22=locked.get(F_CURRENT_PROFILE)
    if codes22 is None: codes22=_v68_ranked(_current_ranker_probs(r))[:22]
    codes16=locked.get(CURRENT16_PROFILE,codes22[:16])
    codespool=locked.get(POOL22_PROFILE)
    if codespool is None:
        specialists=_specialist_model_scores(r,"平衡","20")
        codespool=_pool22_ranked(specialists)[:22]
    states={k:p in locked for k,p in output_profiles.items()}
    stats={k:_stats20round_profile(p,target) for k,p in output_profiles.items()}
    py,pm=_predict_pingte_yixiao_b20(r)
    return {"issue":latest["issue"],"next_issue":target,"count":history_cache.get("total",0),
            "latest_numbers":[latest[f"n{i}"] for i in range(1,7)]+[latest["special"]],
            "latest_special_zodiac":normalize_z(latest["z7"] or ""),
            "latest_created_at":latest["created_at"] or "",
            "special20":[f"{n:02d}" for n in sorted(codes22)],
            "special24":[f"{n:02d}" for n in sorted(codes22)],
            "special16":[f"{n:02d}" for n in sorted(codes16)],
            "pool22":[f"{n:02d}" for n in sorted(codespool)],
            "statsF":stats["F"],"stats16":stats["M16"],"statsPool22":stats["P22"],
            "stats20":_profile_hit_stats(F_CURRENT_PROFILE,60),"model_pool":_pool_dashboard(),
            "pingte_yixiao":py,"pingte_meta":pm,"pingte_b20_stats":_pingte_current_stats(),
            "profile":F_CURRENT_PROFILE,"prediction_locked":all(states.values()),"group_locked":states,
            "forecast":{"target_issue":target,"mode":"算法22/16 + 独立多策略22"},
            "telegram":bool(BOT_TOKEN),"recalculating":False,"stale_prediction":False}




def _pingte_current_stats():
    with db_lock:
        c=connect()
        try:
            rr=c.execute("SELECT hitping FROM prediction_log WHERE profile=? AND settled=1 AND hitping IS NOT NULL ORDER BY CAST(target_issue AS INTEGER) DESC LIMIT 60",(F_CURRENT_PROFILE,)).fetchall()
        finally: c.close()
    h=sum(int(x["hitping"] or 0) for x in rr)
    return {"n":len(rr),"hits":h,"misses":len(rr)-h}



def rebuild_live_cache():
    with live_cache_lock:
        if live_cache.get("building"):
            return live_cache.get("data")
        live_cache["building"]=True
        previous=live_cache.get("data")
    try:
        rr=recent_rows(1200)
        if rr: record_shadow_predictions(rr)
        data=build_model()
        latest=latest_row()
        if str(data.get("issue") or "")!=str(latest["issue"] if latest else ""):
            return previous
        with live_cache_lock:
            live_cache["data"]=data
            live_cache["issue"]=data.get("issue")
        model_state["recalc_finished_at"]=time.strftime("%Y-%m-%d %H:%M:%S")
        return data
    except Exception as e:
        print(f"[V73] model rebuild failed: {type(e).__name__}: {e}",flush=True)
        return previous
    finally:
        with live_cache_lock:
            live_cache["building"]=False


def _visible_base_ready(data):
    return (isinstance(data,dict) and len(data.get("special20") or [])==22
            and len(data.get("special16") or [])==16 and len(data.get("pool22") or [])==22)




def model():
    """Return the newest BASE immediately; blank same-issue caches self-heal."""
    with live_cache_lock:
        data=live_cache.get("data")
        building=live_cache.get("building")
    try:
        latest=latest_row()
    except Exception:
        latest=None

    latest_issue=str(latest["issue"] or "") if latest is not None else ""
    cached_issue=str((data or {}).get("issue") or "")

    # Critical v67.2 fix: a quick-cache row with the correct issue but empty
    # code arrays is NOT considered healthy.  Build the five BASE lists now.
    needs_fast = (data is None or not _visible_base_ready(data) or
                  (latest_issue and cached_issue!=latest_issue))
    if needs_fast and latest is not None:
        fresh=rebuild_fast_live_cache()
        if isinstance(fresh,dict) and str(fresh.get("issue") or "")==latest_issue and _visible_base_ready(fresh):
            if not building:
                threading.Thread(target=rebuild_live_cache,daemon=True,name="full-model-after-fast").start()
            return fresh

    # If the background fast path happened between reads, use it.
    with live_cache_lock:
        data=live_cache.get("data")
        building=live_cache.get("building")
    if isinstance(data,dict):
        cached_issue=str(data.get("issue") or "")
        if (not latest_issue or cached_issue==latest_issue) and _visible_base_ready(data):
            ans=dict(data); ans["stale_prediction"]=False
            return ans

    # Last attempt: return a direct robust snapshot instead of blank arrays.
    if latest is not None:
        try:
            direct=_v674_minimal_snapshot()
            if direct and _visible_base_ready(direct):
                with live_cache_lock:
                    live_cache["data"]=direct; live_cache["issue"]=direct.get("issue")
                return direct
        except Exception as e:
            print(f"[MODEL] direct BASE fallback failed: {type(e).__name__}: {e}",flush=True)

    # Only if history itself is unavailable do we return blanks.
    return {
      "issue":latest_issue or None,
      "next_issue":_next_issue_id(latest_issue) if latest_issue else "",
      "count":history_cache.get("total",0),"recalculating":True,"stale_prediction":True,
      "special20":[],"special27":[],"special27b":[],"special22c":[],"special16d":[],"special24":[],
      "forecast":{"target_issue":_next_issue_id(latest_issue) if latest_issue else "",
                  "mode":"历史数据暂不可读，等待自动恢复"}
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
                    "last_prediction_issue":auto_state.get("last_prediction_issue",""),
                    "v67_rule_version":V67_RULE_VERSION,
                    "migration_latest_issue":auto_state.get("migration_latest_issue",""),
                    "migration_history_count":auto_state.get("migration_history_count",0)})

@app.get("/api/prediction")
def prediction():
    # Cached live endpoint: normally no full-history calculation happens here.
    resp=jsonify(model())
    resp.headers["Cache-Control"]="no-store, no-cache, must-revalidate, max-age=0"
    return resp


@app.get("/api/stats")
def stats_api():
    return jsonify({"current":_profile_hit_stats(F_CURRENT_PROFILE,60),
                    "current16":_profile_hit_stats(CURRENT16_PROFILE,60),
                    "pool22":_profile_hit_stats(POOL22_PROFILE,60),"pingte":_pingte_current_stats()})



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







@app.get("/api/model-pool")
def model_pool_api():
    return jsonify({"model_pool":_pool_dashboard()})


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
    with db_lock:
        _c=connect()
        try:
            _n=_c.execute("SELECT COUNT(*) FROM draws").fetchone()[0]
            _r=_c.execute("SELECT issue FROM draws ORDER BY CAST(issue AS INTEGER) DESC LIMIT 1").fetchone()
            migration_high_water(_n, _r["issue"] if _r else "")
        finally:
            _c.close()
    if REMOTE_BACKUP_ENABLED:
        download_checkpoint_from_supabase()
    restore_checkpoint_if_better()
    import_history_once()

    # Preserve latest bot-collected history before takeover.
    sync_best_available_history()
    try:
        _latest=latest_row()
        with db_lock:
            _c=connect()
            try:
                _n=int(_c.execute("SELECT COUNT(*) AS n FROM draws").fetchone()["n"])
            finally:
                _c.close()
        auto_state["migration_latest_issue"]=str(_latest["issue"] if _latest else "")
        auto_state["migration_history_count"]=_n
        print(f"[MIGRATION] v72.1 inherited latest={auto_state['migration_latest_issue']} count={_n}",flush=True)
    except Exception as e:
        print(f"[MIGRATION] audit failed: {e}",flush=True)

    # v72 deploy guard: operators can pin the exact previous live high-water
    # mark. Abort deployment rather than silently fall back to bundled CSV.
    _required_count=max(int(os.getenv("V72_REQUIRED_HISTORY_COUNT", "15744")),
                        int(auto_state.get("migration_required_count") or 0))
    _configured_issue=str(os.getenv("V72_REQUIRED_LATEST_ISSUE", "20261011454")).strip()
    _required_issue=max((x for x in [_configured_issue,
                        str(auto_state.get("migration_required_issue") or "")]
                        if x.isdigit()),key=int,default="")
    _actual_count=int(auto_state.get("migration_history_count") or 0)
    _actual_issue=str(auto_state.get("migration_latest_issue") or "")
    if _actual_count < _required_count or ( _required_issue.isdigit() and
            (not _actual_issue.isdigit() or int(_actual_issue)<int(_required_issue))):
        raise RuntimeError(f"V72 MIGRATION GUARD: found {_actual_count} draws / {_actual_issue}, "
                           f"required >= {_required_count} / {_required_issue}. "
                           "Restore previous live history before enabling this version.")
    print(f"[V72] migration guard PASS count={_actual_count} latest={_actual_issue}",flush=True)

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
    threading.Thread(target=rebuild_fast_live_cache,daemon=True,name="initial-fast-base").start()

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
