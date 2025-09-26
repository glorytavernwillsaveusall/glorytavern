# coh_top10_async_ws.py (PROD)
# ============================
# Production backend with real RPC + frontend WebSocket + payout control pipeline.
#
# Features added from TEST (1→5):
# 1) Backend-controlled round via round.txt (source of truth) + auto-increment after PAYOUT_3.
# 2) Emit CURRENT_ROUND_ID before every TOP10_UPDATE (with explicit console log).
# 3) Pause generation/broadcast while payouts are in progress (until PAYOUT_3 sent).
# 4) Per-round filesystem layout: payouts/ROUND_X/ with auto-creation of empty payout*_sig.txt.
# 5) Posts/signatures/winners.scoped to the round dir (backend round).
#
# Existing prod logic preserved:
# - Rewards: 60% of fees split 50/30/20 for ranks 1/2/3.
# - '0' + ENTER -> sends SET_COUNTER=0 (freeze UI counter).
# - Real RPC (Helius/Solana), KOL mapping, etc.
#
# Event logic (unchanged in spirit):
# 1) Front -> { type:"PAYOUT_WRITE", lines:[ "1. <wallet> - <X> SOL", "2. ...", "3. ..." ] }
#    -> writes payouts/ROUND_X/winners.txt, parses ranks, resets payout sequence, FREEZE loop until all payouts sent.
# 2) Operator (stdin):
#       ENTER -> Broadcast PAYOUT_1 { tx, solscan, ts } using first non-empty line of payouts/ROUND_X/payout1_sig.txt
#                 ALSO writes payouts/ROUND_X/post_1.txt
#       ENTER -> PAYOUT_2 (+ post_2.txt)
#       ENTER -> PAYOUT_3 (+ post_3.txt)  -> then backend writes website.txt, increments round.txt and UNFREEZE
# 3) '0' + ENTER -> sends SET_COUNTER=0 (freeze UI counter). Backend keeps running.

# pip install aiohttp python-dotenv websockets
import os, sys, json, asyncio, random, time, contextlib, pathlib, re
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

import aiohttp
import websockets
from websockets import WebSocketServerProtocol
from dotenv import load_dotenv, find_dotenv

# Load .env without overriding existing env
load_dotenv(find_dotenv(), override=False)

# ================== CONFIG ==================
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY") or ""
HELIUS_RPC = (f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
              if HELIUS_API_KEY else "https://api.mainnet-beta.solana.com")

# Refresh / timeouts
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC") or "10")
SOL_PRICE_TTL_SEC = int(os.getenv("SOL_PRICE_TTL_SEC") or "60")
RPC_TIMEOUT_SEC   = int(os.getenv("RPC_TIMEOUT_SEC") or "45")
RPC_RETRIES       = int(os.getenv("RPC_RETRIES") or "4")

# WS server (frontend)
WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT") or "8787")
WS_PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL") or "20")
WS_PING_TIMEOUT  = int(os.getenv("WS_PING_TIMEOUT") or "20")

# Fees file (SOL)
FEES_FILE = os.getenv("FEES_FILE", "fees_round.txt")

# ===== Rewards parameters (PROD) =====
# Only 60% of fees are distributed to rewards, split 50/30/20 across ranks 1/2/3
REWARD_POOL_RATE = 0.60
REWARD_PCT = [0.50, 0.30, 0.20]

# Program IDs
TOKEN_PROGRAM_ID   = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SYSTEM_PROGRAM_ID  = "11111111111111111111111111111111"

# Price sources
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINBASE_URL  = "https://api.coinbase.com/v2/prices/SOL-USD/spot"
COINCAP_URL   = "https://api.coincap.io/v2/assets/solana"

# KOL CSV (wallet, name)
KOL_CSV = os.getenv("KOL_CSV", "kolscan_leaderboard_monthly.csv")

# ----- Files & Round backend control -----
ROUND_FILE = "round.txt"                         # backend source of truth
ROUND_ID   = os.getenv("ROUND_ID", "1")          # default initial round if file missing

# Base dir for payouts; all per-round files live under payouts/ROUND_X/
PAYOUTS_DIR_BASE = os.getenv("PAYOUTS_DIR", "payouts")

# BASENAMES (we keep env override for names, but paths will be under payouts/ROUND_X/)
WINNERS_BASENAME = os.getenv("WINNERS_FILE", "winners.txt")
SIG_BASENAMES = {
    1: os.getenv("PAYOUT1_SIG_FILE", "payout1_sig.txt"),
    2: os.getenv("PAYOUT2_SIG_FILE", "payout2_sig.txt"),
    3: os.getenv("PAYOUT3_SIG_FILE", "payout3_sig.txt"),
}
POST_BASENAMES = {
    1: os.getenv("POST1_FILE", "post_1.txt"),
    2: os.getenv("POST2_FILE", "post_2.txt"),
    3: os.getenv("POST3_FILE", "post_3.txt"),
}

# ================== UTILS ==================
def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def short(pk: str, n: int = 4):
    return pk if len(pk) <= 2*n else f"{pk[:n]}…{pk[-n:]}"

async def http_get_json(session: aiohttp.ClientSession, url: str, *, params=None, timeout=30):
    async with session.get(url, params=params, timeout=timeout) as r:
        txt = await r.text()
        r.raise_for_status()
        try:
            return json.loads(txt)
        except Exception:
            return {}

async def rpc_post_json(session: aiohttp.ClientSession, method: str, params: list,
                        *, timeout=45, retries=4):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    delay = 0.6
    for attempt in range(1, retries+1):
        try:
            async with session.post(HELIUS_RPC, json=payload, timeout=timeout) as r:
                txt = await r.text()
                if r.status in (429, 503):
                    await asyncio.sleep(delay + random.uniform(0, 0.4))
                    delay = min(delay * 1.8, 6.0)
                    continue
                r.raise_for_status()
                js = json.loads(txt)
                if "error" in js:
                    msg = (js["error"].get("message") or "").lower()
                    if "rate" in msg or "busy" in msg:
                        await asyncio.sleep(delay + random.uniform(0, 0.4))
                        delay = min(delay * 1.8, 6.0)
                        continue
                    raise RuntimeError(js["error"])
                return js
        except Exception:
            if attempt >= retries:
                raise
            await asyncio.sleep(delay + random.uniform(0, 0.3))
            delay = min(delay * 1.8, 6.0)
    raise RuntimeError("RPC failed after retries")

# File helpers
def read_first_non_empty_line(path: str) -> str:
    p = pathlib.Path(path)
    if not p.exists():
        return ""
    try:
        with open(p, "r", encoding="utf-8") as f:
            for ln in f:
                s = ln.strip()
                if s:
                    return s
    except Exception:
        pass
    return ""

def write_text_file(path: str, content: str):
    # ensure parent dir exists
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content.rstrip() + "\n")
        print(f"[POST] wrote → {path}", flush=True)
    except Exception as e:
        print(f"[POST] write failed ({path}): {e}", flush=True)

# ================== ROUND & PER-ROUND PATHS (BACKEND-CONTROLLED) ==================
def round_dir(round_id: str) -> str:
    return os.path.join(PAYOUTS_DIR_BASE, f"ROUND_{round_id}")

def _ensure_round_file_exists():
    p = pathlib.Path(ROUND_FILE)
    if not p.exists():
        try:
            with open(p, "w", encoding="utf-8") as f:
                f.write(str(int(ROUND_ID)) + "\n")
            print(f"[ROUND] Created {ROUND_FILE} with initial value {ROUND_ID}", flush=True)
        except Exception as e:
            print(f"[ROUND] Failed to create {ROUND_FILE}: {e}", flush=True)

def _ensure_round_dir_and_sig_files(round_id: str):
    """Ensure payouts/ROUND_X exists and payout sig files exist (empty if missing)."""
    d = round_dir(round_id)
    pathlib.Path(d).mkdir(parents=True, exist_ok=True)
    for i in (1, 2, 3):
        p = os.path.join(d, SIG_BASENAMES[i])
        if not pathlib.Path(p).exists():
            try:
                with open(p, "w", encoding="utf-8") as f:
                    f.write("")  # leave empty for manual fill
                print(f"[PAYOUT] created empty sig file → {p}", flush=True)
            except Exception as e:
                print(f"[PAYOUT] failed to create sig file ({p}): {e}", flush=True)

def read_round_file() -> str:
    _ensure_round_file_exists()
    try:
        with open(ROUND_FILE, "r", encoding="utf-8") as f:
            s = f.read().strip()
            n = int(s)
            r = str(n)
            _ensure_round_dir_and_sig_files(r)
            return r
    except Exception as e:
        print(f"[ROUND] Failed to read {ROUND_FILE}: {e} — falling back to {ROUND_ID}", flush=True)
        r = str(int(ROUND_ID))
        _ensure_round_dir_and_sig_files(r)
        return r

def write_round_file(new_round: int):
    try:
        with open(ROUND_FILE, "w", encoding="utf-8") as f:
            f.write(str(int(new_round)) + "\n")
        print(f"[ROUND] Updated {ROUND_FILE} → {new_round}", flush=True)
        _ensure_round_dir_and_sig_files(str(int(new_round)))
    except Exception as e:
        print(f"[ROUND] Failed to write {ROUND_FILE}: {e}", flush=True)

def path_winners(round_id: str) -> str:
    return os.path.join(round_dir(round_id), WINNERS_BASENAME)

def path_sig(round_id: str, step: int) -> str:
    return os.path.join(round_dir(round_id), SIG_BASENAMES[step])

def path_post(round_id: str, step: int) -> str:
    return os.path.join(round_dir(round_id), POST_BASENAMES[step])

# >>> NEW: website.txt path helper <<<
def path_website(round_id: str) -> str:
    return os.path.join(round_dir(round_id), "website.txt")

async def send_round_update():
    """Emit CURRENT_ROUND_ID (backend-controlled) before any TOP10_UPDATE."""
    current_round = read_round_file()
    msg = {"type": "CURRENT_ROUND_ID", "round": current_round, "ts": now_iso()}
    try:
        print(f"[ROUND] Emitting CURRENT_ROUND_ID → round={current_round} (clients={len(_ws_clients)})", flush=True)
    except Exception:
        pass
    await ws_broadcast(msg)

# ================== PRICE CACHE ==================
class SolPriceCache:
    def __init__(self, ttl_sec: int = SOL_PRICE_TTL_SEC):
        self.ttl = ttl_sec
        self.value = 0.0
        self.ts = 0.0

    async def get(self, session: aiohttp.ClientSession) -> float:
        now = time.time()
        if self.value > 0 and (now - self.ts) < self.ttl:
            return self.value

        # CoinGecko
        try:
            js = await http_get_json(session, COINGECKO_URL, params={"ids":"solana","vs_currencies":"usd"}, timeout=12)
            v = float(((js or {}).get("solana") or {}).get("usd") or 0)
            if v > 0:
                self.value, self.ts = v, now
                return v
        except Exception:
            pass

        # Coinbase
        try:
            js = await http_get_json(session, COINBASE_URL, timeout=10)
            v = float(((js or {}).get("data") or {}).get("amount") or 0)
            if v > 0:
                self.value, self.ts = v, now
                return v
        except Exception:
            pass

        # CoinCap
        try:
            js = await http_get_json(session, COINCAP_URL, timeout=10)
            v = float(((js or {}).get("data") or {}).get("priceUsd") or 0)
            if v > 0:
                self.value, self.ts = v, now
                return v
        except Exception:
            pass

        return self.value

# ================== CORE HOLDERS ==================
async def get_token_largest_accounts(session: aiohttp.ClientSession, mint: str, limit: int = 200):
    js = await rpc_post_json(session, "getTokenLargestAccounts", [mint, {"commitment":"confirmed"}],
                             timeout=RPC_TIMEOUT_SEC, retries=RPC_RETRIES)
    value = (js or {}).get("result", {}).get("value", []) or []
    return value[:limit]

async def get_multiple_accounts_jsonparsed(session: aiohttp.ClientSession, pubkeys: List[str]):
    js = await rpc_post_json(
        session,
        "getMultipleAccounts",
        [pubkeys, {"encoding": "jsonParsed", "commitment":"confirmed"}],
        timeout=RPC_TIMEOUT_SEC, retries=RPC_RETRIES
    )
    return ((js or {}).get("result") or {}).get("value") or []

async def fetch_topN_aggregated_by_wallet(session: aiohttp.ClientSession, mint: str, top_n: int = 10) -> List[Tuple[str, float]]:
    """
    - getTokenLargestAccounts → token accounts (TAs)
    - getMultipleAccounts (jsonParsed) → owner of each TA
    - Aggregate by owner (sum uiAmount)
    - Filter owners whose PROGRAM OWNER == System Program (EOA only; exclude PDAs/LPs)
    - Return Top-N [(wallet, total_tokens)]
    """
    largest = await get_token_largest_accounts(session, mint, limit=200)
    if not largest:
        return []

    token_accounts = [x["address"] for x in largest]
    infos = await get_multiple_accounts_jsonparsed(session, token_accounts)

    totals: Dict[str, float] = {}
    owners_list: List[str] = []
    for i, acc in enumerate(infos):
        if not acc:
            continue
        try:
            parsed = acc["data"]["parsed"]
            owner = parsed["info"]["owner"]
            amt = float(largest[i]["uiAmount"] or 0.0)
        except Exception:
            continue
        if amt <= 0:
            continue
        totals[owner] = totals.get(owner, 0.0) + amt
        owners_list.append(owner)

    if not totals:
        return []

    unique_owners = sorted(set(owners_list))
    owners_info = await get_multiple_accounts_jsonparsed(session, unique_owners)

    eoa_ok: Dict[str, bool] = {}
    for pk, acc in zip(unique_owners, owners_info):
        try:
            program_owner = (acc or {}).get("owner") or ""
            eoa_ok[pk] = (program_owner == SYSTEM_PROGRAM_ID)
        except Exception:
            eoa_ok[pk] = False

    filtered = [(w, amt) for (w, amt) in totals.items() if eoa_ok.get(w, False)]
    if not filtered:
        return []

    filtered.sort(key=lambda kv: kv[1], reverse=True)
    return filtered[:top_n]

# ================== FEES ==================
def read_fees_sol(path: str) -> float:
    try:
        with open(path, "r", encoding="utf-8") as f:
            v = float(f.read().strip())
            return max(0.0, v)
    except Exception:
        return 0.0

# ================== KOL CSV ==================
_INVIS = "".join([chr(c) for c in [0x200B,0x200C,0x200D,0xFEFF]])
def _clean_wallet(s: str) -> str:
    return (s or "").translate({ord(c): None for c in _INVIS}).strip().strip('\'"')

class KolMap:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self._map: Dict[str, str] = {}
        self._mtime = 0.0

    def load_if_changed(self):
        p = pathlib.Path(self.csv_path)
        if not p.exists():
            self._map = {}
            self._mtime = 0.0
            return
        m = p.stat().st_mtime
        if m <= self._mtime:
            return
        self._mtime = m
        try:
            with open(p, "r", encoding="utf-8") as f:
                lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()]
            mp: Dict[str, str] = {}
            for i, ln in enumerate(lines):
                if i == 0 and ln.lower().replace(" ", "") in ("kol,wallet", "kol;wallet"):
                    continue
                parts = [x.strip() for x in ln.replace("\t", ",").replace(";", ",").split(",")]
                if len(parts) < 2:
                    continue
                kol, wal = parts[0], _clean_wallet(parts[1])
                if kol and wal:
                    mp[wal] = kol
            self._map = mp
            print(f"[KOL] Loaded {len(self._map)} entries from {self.csv_path}", flush=True)
        except Exception as e:
            print(f"[KOL] Failed to load {self.csv_path}: {e}", flush=True)

    def get(self, wallet: str) -> str:
        return self._map.get(wallet, "")

# ================== POSTS (English, fun tone; NO hashtags) ==================
TITLES = {1: "King of the Hodlers", 2: "Champion of the Chain", 3: "Guardian of the Treasury"}

def build_post_text(round_id: str, rank: int, wallet: str, tx: str, sol_amount: Optional[float]) -> str:
    title = TITLES.get(rank, f"Top {rank}")
    flavor_lines = {
        1: "What an epic siege! The throne was taken after a relentless battle at the top. 👑⚔️",
        2: "A fearless duel to the very end — a true contender of pure on-chain grit. 🛡️⚔️",
        3: "Endurance, strategy, and heart. Held the line till the last tick. 🏰🔥",
    }
    congrats = "Congratulations and respect. You fought to the last block!"
    sol_line = f"SOL Reward: {sol_amount:.6f} SOL" if sol_amount is not None else None

    body = [
        f"COH - Round {round_id} - {title}",
        flavor_lines.get(rank, "A glorious on-chain skirmish — and a well-earned spot on the podium!"),
        congrats,
        f"Wallet: {wallet}",
    ]
    if sol_line:
        body.append(sol_line)
    body.append(f"Tx: {tx or 'TBD'}")
    return "\n".join(body)

# >>> NEW: website.txt HTML builder helpers <<<
def format_sol(x: Optional[float]) -> str:
    if x is None:
        return "0"
    s = f"{float(x):.6f}".rstrip("0").rstrip(".")
    return s if s else "0"

def build_website_card_html(round_id: str,
                            winners: Dict[int, Tuple[str, Optional[float]]],
                            sigs: Dict[int, str]) -> str:
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = []
    lines.append('<div class="card">')
    lines.append(f'  <div class="badge">Round {round_id}</div>')
    for rank in (1, 2, 3):
        title = TITLES.get(rank, f"Top {rank}")
        wallet, sol_amt = winners.get(rank, ("", None))
        sig = (sigs.get(rank) or "").strip()
        sol_txt = format_sol(sol_amt)
        href = sig if sig else '#'
        lines.append('  <div class="rank">')
        lines.append(f'    <b>{medals.get(rank, "")}</b>')
        lines.append('    <div>')
        lines.append(f'      <div style="font-weight:700">{title}</div>')
        lines.append(f'      <div class="mono">{wallet}</div>')
        lines.append(f'      <div>{sol_txt} SOL · <a href="{href}" target="_blank">tx</a></div>')
        lines.append('    </div>')
        lines.append('  </div>')
    lines.append('</div>')
    return "\n".join(lines)

# ===== Winners parsing =====
WIN_LINE_RE = re.compile(
    r"^\s*(?P<rank>[123])[\s\.\):-]*\s*(?P<wallet>[A-Za-z0-9]{20,})\s*[-–—:]\s*(?P<sol>[0-9]+(?:\.[0-9]+)?)\s*SOL\b",
    re.IGNORECASE
)

def parse_winner_line(line: str) -> Optional[Tuple[int, str, float]]:
    m = WIN_LINE_RE.search(line.strip())
    if not m:
        try:
            left, right = line.split("-", 1)
            rank = int(re.findall(r"[123]", left)[0])
            wallet = re.findall(r"[A-Za-z0-9]{20,}", left + right)[0]
            sol = float(re.findall(r"\d+(?:\.\d+)?", right)[0])
            return rank, wallet, sol
        except Exception:
            return None
    return int(m.group("rank")), m.group("wallet").strip(), float(m.group("sol"))

def parse_winners_from_lines(lines: List[str]) -> Dict[int, Tuple[str, float]]:
    out: Dict[int, Tuple[str, float]] = {}
    for ln in lines[:3]:
        tpl = parse_winner_line(ln)
        if tpl:
            r, w, s = tpl
            out[r] = (w, s)
    return out

# ================== WS SERVER (frontend) ==================
_ws_clients: set[WebSocketServerProtocol] = set()

# Payout state
_winners_lines: List[str] = []                         # raw text lines from front
_parsed_winners: Dict[int, Tuple[str, float]] = {}     # {rank: (wallet, sol)}
_payout_next_idx: int = 0                              # 0->PAYOUT_1 ; 1->PAYOUT_2 ; 2->PAYOUT_3

def payout_in_progress() -> bool:
    """Freeze sim/broadcast until all three payouts were sent."""
    return bool(_winners_lines) and (_payout_next_idx < 3)

def reset_payout_state():
    global _payout_next_idx, _parsed_winners
    _payout_next_idx = 0
    _parsed_winners = parse_winners_from_lines(_winners_lines)

async def ws_handler(ws: WebSocketServerProtocol):
    global _winners_lines
    _ws_clients.add(ws)
    try:
        async for raw in ws:
            # Allow bidirectional commands; currently handling PAYOUT_WRITE
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            mtype = (msg.get("type") or "").upper()

            if mtype == "PAYOUT_WRITE":
                # Round is fully backend-controlled; ignore any 'round' from frontend
                current_round = read_round_file()
                rd = round_dir(current_round)

                lines = msg.get("lines") or []
                # write winners.txt under payouts/ROUND_X/
                try:
                    pathlib.Path(rd).mkdir(parents=True, exist_ok=True)
                    with open(path_winners(current_round), "w", encoding="utf-8") as f:
                        for ln in lines[:3]:
                            f.write(str(ln).rstrip() + "\n")
                    print(f"[PAYOUT] winners written → {path_winners(current_round)} ({len(lines[:3])} lines)", flush=True)
                except Exception as e:
                    print(f"[PAYOUT] write winners failed: {e}", flush=True)

                _winners_lines = [str(ln).strip() for ln in lines[:3]]
                reset_payout_state()

                # Informative PAUSE log: loop is frozen until PAYOUT_3 completes
                print("[PAUSE] Winners received. Freezing TOP10 generation until PAYOUT_3 is sent.", flush=True)

                sig1_path = path_sig(current_round, 1)
                sig1 = read_first_non_empty_line(sig1_path) or "(no signature found)"
                print(f"[PAYOUT] Ready to send PAYOUT_1. Signature ({sig1_path}): {sig1}", flush=True)
                print(f"[PAYOUT] Press ENTER to SEND PAYOUT_1  |  '0'+ENTER to SET_COUNTER=0", flush=True)

    except Exception:
        pass
    finally:
        _ws_clients.discard(ws)

async def ws_broadcast(obj: dict):
    if not _ws_clients:
        return
    msg = json.dumps(obj, ensure_ascii=False)
    dead = []
    for ws in list(_ws_clients):
        try:
            await ws.send(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        with contextlib.suppress(Exception):
            _ws_clients.discard(ws)

async def start_ws_server():
    server = await websockets.serve(
        ws_handler, WS_HOST, WS_PORT,
        ping_interval=WS_PING_INTERVAL,
        ping_timeout=WS_PING_TIMEOUT,
        max_queue=32,
    )
    print(f"[WS] listening on ws://{WS_HOST}:{WS_PORT}", flush=True)
    return server

# ================== PRINT & BROADCAST ==================
def print_top_block(*, mint: str, rpc: str, topN: List[Tuple[str, float]],
                    fees_sol: float, sol_usd: float, kol_get=lambda w: ""):
    def usd(x): return x * sol_usd
    reward_pool_sol = fees_sol * REWARD_POOL_RATE
    parts_sol = [reward_pool_sol * p for p in REWARD_PCT]
    parts_usd = [usd(x) for x in parts_sol]

    print("=" * 100, flush=True)
    print(f"[{now_iso()}] MINT={mint} | RPC={rpc}", flush=True)
    print(f"Fees (file): {fees_sol:.6f} SOL  (~{usd(fees_sol):,.2f} USD)  |  SOL=${sol_usd:,.2f}", flush=True)
    print(f"Rewards pool (60%): {reward_pool_sol:.6f} SOL  (~{usd(reward_pool_sol):,.2f} USD)", flush=True)
    print("-" * 100, flush=True)

    for i in range(min(10, len(topN))):
        owner, total = topN[i]
        label = f"#{i+1}"
        reward_txt = ""
        if i < 3:
            reward_txt = f" | reward={parts_sol[i]:.6f} SOL (~{parts_usd[i]:,.2f} USD)"
        kol_name = kol_get(owner)
        kol_txt = f" | KOL={kol_name}" if kol_name else ""
        print(f"{label:<4} owner={short(owner):<12} holder_total={total:.6f}{reward_txt}{kol_txt}", flush=True)

    for i in range(len(topN), 10):
        label = f"#{i+1}"
        reward_txt = ""
        if i < 3:
            reward_txt = f" | reward={parts_sol[i]:.6f} SOL (~{parts_usd[i]:,.2f} USD)"
        print(f"{label:<4} -- no eligible holder --{reward_txt}", flush=True)

    print("=" * 100 + "\n", flush=True)

def make_payload(*, mint: str, topN: List[Tuple[str, float]], fees_sol: float,
                 sol_usd: float, kol_get=lambda w: "") -> dict:
    """Stable schema for frontend, extended to top-10 with 'kol' per holder.
       Rewards are computed on 60% of fees, split 50/30/20."""
    to_usd = lambda x: x * sol_usd
    reward_pool_sol = fees_sol * REWARD_POOL_RATE
    rewards_sol = [reward_pool_sol * p for p in REWARD_PCT]
    holders = []
    for i, (w, amt) in enumerate(topN[:10]):
        holders.append({
            "rank": i+1,
            "wallet": w,
            "total_tokens": float(f"{amt:.8f}"),
            "kol": kol_get(w) or ""
        })
    return {
        "type": "TOP10_UPDATE",
        "ts": now_iso(),
        "mint": mint,
        "sol_usd": float(f"{sol_usd:.8f}"),
        "fees": {  # raw fees as read from file
            "sol": float(f"{fees_sol:.8f}"),
            "usd": float(f"{to_usd(fees_sol):.8f}")
        },
        "rewards": {  # rewards reflect 60% pool split 50/30/20
            "top1": {"sol": float(f"{rewards_sol[0]:.8f}"), "usd": float(f"{to_usd(rewards_sol[0]):.8f}")},
            "top2": {"sol": float(f"{rewards_sol[1]:.8f}"), "usd": float(f"{to_usd(rewards_sol[1]):.8f}")},
            "top3": {"sol": float(f"{rewards_sol[2]:.8f}"), "usd": float(f"{to_usd(rewards_sol[2]):.8f}")},
        },
        "holders": holders
    }

# ================== CONTROL (stdin) ==================
async def stdin_command_task(stop_event: asyncio.Event):
    """
    - ENTER (empty line): send next PAYOUT_n with tx from payouts/ROUND_X/payoutn_sig.txt; also create post_n.txt
    - '0' + ENTER: send SET_COUNTER=0 (backend keeps running)
    """
    loop = asyncio.get_running_loop()
    print("[CTL] Press ENTER to send next PAYOUT (1→2→3). Type 0 + ENTER to SET_COUNTER=0.", flush=True)
    global _payout_next_idx, _winners_lines, _parsed_winners

    while not stop_event.is_set():
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if line is None:
            await asyncio.sleep(0.05)
            continue
        cmd = line.rstrip("\n").strip()

        if cmd == "0":
            msg = {"type": "SET_COUNTER", "value": 0, "ts": now_iso()}
            print("[CTL] Emission SET_COUNTER → 0", flush=True)
            try:
                await ws_broadcast(msg)
            except Exception as e:
                print(f"[CTL] SET_COUNTER send failed: {e}", flush=True)
            continue

        if cmd == "":
            if _payout_next_idx >= 3:
                print("[PAYOUT] Already sent PAYOUT_1/2/3. Waiting for new winners…", flush=True)
                continue

            current_round = read_round_file()
            step = _payout_next_idx + 1
            sig_file = path_sig(current_round, step)
            sig = read_first_non_empty_line(sig_file) if sig_file else ""
            if not sig:
                print(f"[PAYOUT] No signature found in {sig_file or '(unknown)'} — sending without link.", flush=True)

            evt = {"type": f"PAYOUT_{step}", "tx": sig, "solscan": sig, "ts": now_iso()}
            print(f"[PAYOUT] CONFIRMED → sending {evt['type']} with tx='{sig or '(empty)'}'", flush=True)
            try:
                await ws_broadcast(evt)
                _payout_next_idx += 1
            except Exception as e:
                print(f"[PAYOUT] Broadcast {evt['type']} failed: {e}", flush=True)

            # Build & write post for this rank under payouts/ROUND_X/
            wallet, sol_amt = ("", None)
            t = _parsed_winners.get(step)
            if t:
                wallet, sol_amt = t[0], t[1]
            post_text = build_post_text(current_round, step, wallet or "TBD", sig or "TBD", sol_amt)
            write_text_file(path_post(current_round, step), post_text)

            # >>> NEW: After PAYOUT_3, also write website.txt then increment round <<<
            if _payout_next_idx >= 3:
                try:
                    finished_round = read_round_file()
                    sigs = {
                        1: read_first_non_empty_line(path_sig(finished_round, 1)) or "",
                        2: read_first_non_empty_line(path_sig(finished_round, 2)) or "",
                        3: read_first_non_empty_line(path_sig(finished_round, 3)) or "",
                    }
                    winners = {
                        1: (_parsed_winners.get(1, ("", None))[0],
                            _parsed_winners.get(1, ("", None))[1] if _parsed_winners.get(1) else None),
                        2: (_parsed_winners.get(2, ("", None))[0],
                            _parsed_winners.get(2, ("", None))[1] if _parsed_winners.get(2) else None),
                        3: (_parsed_winners.get(3, ("", None))[0],
                            _parsed_winners.get(3, ("", None))[1] if _parsed_winners.get(3) else None),
                    }
                    website_html = build_website_card_html(finished_round, winners, sigs)
                    write_text_file(path_website(finished_round), website_html)
                    print(f"[WEBSITE] website.txt generated for ROUND {finished_round}", flush=True)

                    # Now increment round
                    curr = int(finished_round)
                    write_round_file(curr + 1)
                except Exception as e:
                    print(f"[ROUND] Failed to finalize website/increment after PAYOUT_3: {e}", flush=True)

                _winners_lines = []
                print("[PAUSE] Payout sequence complete → unfreezing TOP10 generation.", flush=True)
                print("[PAYOUT] Sequence PAYOUT_1/2/3 complete. Waiting for new winners…", flush=True)
            else:
                nxt = _payout_next_idx + 1
                nxt_path = path_sig(current_round, nxt)
                nxt_sig = read_first_non_empty_line(nxt_path) or "(no signature found)"
                print(f"[PAYOUT] Ready to send PAYOUT_{nxt}. Signature ({nxt_path}): {nxt_sig}", flush=True)
                print("[PAYOUT] Press ENTER to SEND.", flush=True)

# ================== MAIN LOOP ==================
async def main_async(mint: str):
    price_cache = SolPriceCache(ttl_sec=SOL_PRICE_TTL_SEC)
    kol = KolMap(KOL_CSV)
    kol.load_if_changed()

    # Ensure round.txt and per-round directory/sig files are present at startup
    _ensure_round_file_exists()
    _ensure_round_dir_and_sig_files(read_round_file())

    conn = aiohttp.TCPConnector(limit=64, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        ws_server = await start_ws_server()

        stop_event = asyncio.Event()
        ctl_task = asyncio.create_task(stdin_command_task(stop_event))

        try:
            while not stop_event.is_set():
                try:
                    # hot-reload KOL csv if changed
                    kol.load_if_changed()

                    if payout_in_progress():
                        # Do not advance price/top10 or broadcast while payouts pending
                        # print("[PAUSE] Payout in progress (waiting for PAYOUT_3). Skipping TOP10 fetch & broadcasts.", flush=True)
                    else:
                        sol_usd = await price_cache.get(session) or 0.0
                        topN = await fetch_topN_aggregated_by_wallet(session, mint, top_n=10)
                        fees_sol = read_fees_sol(FEES_FILE)

                        # Console preview
                        print_top_block(
                            mint=mint, rpc=HELIUS_RPC, topN=topN,
                            fees_sol=fees_sol, sol_usd=sol_usd,
                            kol_get=kol.get
                        )

                        # Emit round then TOP10 payload
                        await send_round_update()
                        payload = make_payload(
                            mint=mint, topN=topN, fees_sol=fees_sol, sol_usd=sol_usd,
                            kol_get=kol.get
                        )
                        await ws_broadcast(payload)

                except Exception as e:
                    print(f"[ERR] {e}", flush=True)

                await asyncio.sleep(POLL_INTERVAL_SEC)
        finally:
            ctl_task.cancel()
            with contextlib.suppress(Exception):
                await ctl_task
            ws_server.close()
            await ws_server.wait_closed()

# ================== ENTRY ==================
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python coh_top10_async_ws.py <MINT_CA>")
        sys.exit(1)
    m = sys.argv[1].strip()
    try:
        asyncio.run(main_async(m))
    except KeyboardInterrupt:
        print("Bye.")
