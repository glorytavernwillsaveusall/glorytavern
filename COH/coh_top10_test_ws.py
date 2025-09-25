# coh_top10_test_ws.py
# =====================
# TEST mode (no real RPC) + keyboard-driven payouts (press ENTER).
#
# Added (Round logic controlled by backend):
# - Backend reads current round from round.txt (created if missing with ROUND_ID default).
# - Sends ROUND_UPDATE before every TOP10_UPDATE (including snapshots).
# - Ignores round from frontend; PAYOUT_WRITE no longer updates round.
# - After PAYOUT_3 and post_3 creation, increments round in round.txt.
#
# New in this version:
# - After PAYOUT_3, also writes payouts/ROUND_X/website.txt with the HTML card summary.
#
# Flow (unchanged otherwise):
# 1) Front -> { type:"PAYOUT_WRITE", lines:[ "1. <wallet> - <X> SOL", "2. ...", "3. ..." ] }
#    -> writes winners.txt, resets payout sequence (ready for PAYOUT_1).
# 2) Operator:
#       ENTER -> Back reads payout1_sig.txt, broadcasts PAYOUT_1 {tx}, ALSO writes post_1.txt
#       ENTER -> PAYOUT_2 (+ post_2.txt)
#       ENTER -> PAYOUT_3 (+ post_3.txt)  -> then backend writes website.txt and increments round.txt
# 3) '0' + ENTER -> sends SET_COUNTER=0 (freeze UI). Script keeps running.
#
# Files:
# - round.txt          (current round number, controlled by backend)
# - winners.txt
# - payout1_sig.txt / payout2_sig.txt / payout3_sig.txt
# - post_1.txt, post_2.txt, post_3.txt
# - website.txt (new) — HTML snippet for the website card
#
# Env:
# - ROUND_ID (default "1")
# - POST1_FILE/POST2_FILE/POST3_FILE (optional overrides)

import os, sys, json, asyncio, random, time, contextlib, pathlib, re
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional, Callable
import websockets
from websockets import WebSocketServerProtocol
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=False)

# ================== CONFIG ==================
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC") or "5")
SOL_PRICE_TTL_SEC = int(os.getenv("SOL_PRICE_TTL_SEC") or "60")

WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT") or "8787")
WS_PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL") or "20")
WS_PING_TIMEOUT  = int(os.getenv("WS_PING_TIMEOUT") or "20")

FEES_FILE    = os.getenv("FEES_FILE", "fees_round.txt")

# [DIR LOGIC] Base dir for payouts; all per-round files live under payouts/ROUND_X/
PAYOUTS_DIR_BASE = os.getenv("PAYOUTS_DIR", "payouts")

# Keep basenames from env, but we will place them under the round dir
WINNERS_BASENAME = os.getenv("WINNERS_FILE", "winners.txt")
ROUND_FILE   = "round.txt"  # <- fixed name as requested

# Signature (tx / solscan) basenames
SIG_BASENAMES = {
    1: os.getenv("PAYOUT1_SIG_FILE", "payout1_sig.txt"),
    2: os.getenv("PAYOUT2_SIG_FILE", "payout2_sig.txt"),
    3: os.getenv("PAYOUT3_SIG_FILE", "payout3_sig.txt"),
}

# Post basenames (for X)
POST_BASENAMES = {
    1: os.getenv("POST1_FILE", "post_1.txt"),
    2: os.getenv("POST2_FILE", "post_2.txt"),
    3: os.getenv("POST3_FILE", "post_3.txt"),
}

# Default round (used only to initialize round.txt if missing)
ROUND_ID = os.getenv("ROUND_ID", "1")

# >>> Backend-controlled current round (loaded from round.txt) <<<
CURRENT_ROUND_ID = ROUND_ID  # will be replaced at startup by read_round_file()

PCT_TOP = [0.50, 0.30, 0.10]
KOL_CSV = os.getenv("KOL_CSV", "kolscan_leaderboard_monthly.csv")

# ================== UTILS ==================
def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def short(pk: str, n: int = 4):
    return pk if len(pk) <= 2*n else f"{pk[:n]}…{pk[-n:]}"

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

def format_sol(x: Optional[float]) -> str:
    if x is None:
        return "0"
    s = f"{float(x):.6f}".rstrip("0").rstrip(".")
    return s if s else "0"

# ---- Round dir helpers (BACKEND-CONTROLLED) ----
def round_dir(round_id: str) -> str:
    return os.path.join(PAYOUTS_DIR_BASE, f"ROUND_{round_id}")

def _ensure_round_dir_and_sig_files(round_id: str):
    """Ensure payouts/ROUND_X exists and payout sig files exist (empty if missing)."""
    d = round_dir(round_id)
    pathlib.Path(d).mkdir(parents=True, exist_ok=True)
    # Create empty payout sig files if missing
    for i in (1, 2, 3):
        p = os.path.join(d, SIG_BASENAMES[i])
        if not pathlib.Path(p).exists():
            try:
                with open(p, "w", encoding="utf-8") as f:
                    f.write("")  # leave empty for manual fill
                print(f"[PAYOUT] created empty sig file → {p}", flush=True)
            except Exception as e:
                print(f"[PAYOUT] failed to create sig file ({p}): {e}", flush=True)

def path_winners(round_id: str) -> str:
    return os.path.join(round_dir(round_id), WINNERS_BASENAME)

def path_sig(round_id: str, step: int) -> str:
    return os.path.join(round_dir(round_id), SIG_BASENAMES[step])

def path_post(round_id: str, step: int) -> str:
    return os.path.join(round_dir(round_id), POST_BASENAMES[step])

def path_website(round_id: str) -> str:
    return os.path.join(round_dir(round_id), "website.txt")

# ---- Round file helpers (BACKEND-CONTROLLED) ----
def _ensure_round_file_exists():
    p = pathlib.Path(ROUND_FILE)
    if not p.exists():
        try:
            with open(p, "w", encoding="utf-8") as f:
                f.write(str(int(ROUND_ID)) + "\n")
            print(f"[ROUND] Created {ROUND_FILE} with initial value {ROUND_ID}", flush=True)
        except Exception as e:
            print(f"[ROUND] Failed to create {ROUND_FILE}: {e}", flush=True)

def read_round_file() -> str:
    _ensure_round_file_exists()
    try:
        with open(ROUND_FILE, "r", encoding="utf-8") as f:
            s = f.read().strip()
            n = int(s)
            r = str(n)
            # Ensure per-round dir/files exist whenever we read the round
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
        # Prepare new round dir & empty sig files
        _ensure_round_dir_and_sig_files(str(int(new_round)))
    except Exception as e:
        print(f"[ROUND] Failed to write {ROUND_FILE}: {e}", flush=True)

async def send_round_update():
    # Always read latest from file in case of external edits
    global CURRENT_ROUND_ID
    CURRENT_ROUND_ID = read_round_file()
    msg = {"type": "CURRENT_ROUND_ID", "round": CURRENT_ROUND_ID, "ts": now_iso()}

    # 👇 LOG clair côté console à CHAQUE émission
    try:
        print(f"[ROUND] Emitting CURRENT_ROUND_ID → round={CURRENT_ROUND_ID} (clients={len(_ws_clients)})", flush=True)
    except Exception:
        pass

    await ws_broadcast(msg)

# === Winners parsing ===
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

# === Post text builder ===
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

def build_website_card_html(round_id: str,
                            winners: Dict[int, Tuple[str, Optional[float]]],
                            sigs: Dict[int, str]) -> str:
    """Return the HTML snippet for website.txt exactly as requested."""
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = []
    lines.append('<div class="card">')
    lines.append(f'  <div class="badge">Round {round_id}</div>')
    for rank in (1, 2, 3):
        title = TITLES.get(rank, f"Top {rank}")
        wallet, sol_amt = winners.get(rank, ("", None))
        sig = (sigs.get(rank) or "").strip()
        sol_txt = format_sol(sol_amt)
        href = f'{sig}' if sig else '#'
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

    def random_kol_wallet(self) -> Optional[Tuple[str,str]]:
        if not self._map:
            return None
        wal = random.choice(list(self._map.keys()))
        return wal, self._map.get(wal, "")

# ================== WS SERVER (frontend) ==================
_ws_clients: set[WebSocketServerProtocol] = set()

# Payout state
_winners_lines: List[str] = []         # raw text lines from front
_parsed_winners: Dict[int, Tuple[str, float]] = {}  # {rank: (wallet, sol)}
_payout_next_idx: int = 0              # 0->next is PAYOUT_1 ; 1->PAYOUT_2 ; 2->PAYOUT_3

def reset_payout_state():
    global _payout_next_idx, _parsed_winners
    _payout_next_idx = 0
    _parsed_winners = parse_winners_from_lines(_winners_lines)

async def ws_handler(ws: WebSocketServerProtocol):
    global _winners_lines
    _ws_clients.add(ws)
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            mtype = (msg.get("type") or "").upper()

            if mtype == "PAYOUT_WRITE":
                # Round is now fully backend-controlled; ignore any 'round' from frontend
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

                # [PAUSE LOGIC] Informative log to indicate we are now frozen.
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

# ================== PRINT & PAYLOAD (unchanged sim loop) ==================
def print_top_block(*, mint: str, rpc: str, topN: List[Tuple[str, float]],
                    fees_sol: float, sol_usd: float, kol_get: Callable[[str], str] = lambda w: ""):
    def usd(x): return x * sol_usd
    parts_sol = [fees_sol * p for p in PCT_TOP]
    parts_usd = [usd(x) for x in parts_sol]

    print("=" * 100, flush=True)
    print(f"[{now_iso()}] MINT={mint} | RPC={rpc}", flush=True)
    print(f"Fees (file): {fees_sol:.6f} SOL  (~{usd(fees_sol):,.2f} USD)  |  SOL=${sol_usd:,.2f}", flush=True)
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
                 sol_usd: float, kol_get: Callable[[str], str] = lambda w: "") -> dict:
    to_usd = lambda x: x * sol_usd
    rewards_sol = [fees_sol * p for p in PCT_TOP]
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
        "fees": {
            "sol": float(f"{fees_sol:.8f}"),
            "usd": float(f"{to_usd(fees_sol):.8f}")
        },
        "rewards": {
            "top1": {"sol": float(f"{rewards_sol[0]:.8f}"), "usd": float(f"{to_usd(rewards_sol[0]):.8f}")},
            "top2": {"sol": float(f"{rewards_sol[1]:.8f}"), "usd": float(f"{to_usd(rewards_sol[1]):.8f}")},
            "top3": {"sol": float(f"{rewards_sol[2]:.8f}"), "usd": float(f"{to_usd(rewards_sol[2]):.8f}")},
        },
        "holders": holders
    }

# ================== SIMULATE PRICE ==================
class SimSolPrice:
    def __init__(self, start_price: float = 150.0, ttl_sec: int = 60):
        self.value = max(1.0, start_price)
        self.ts = 0.0
        self.ttl = ttl_sec

    async def get(self) -> float:
        now = time.time()
        if (now - self.ts) < self.ttl:
            return self.value
        drift = random.uniform(-0.01, 0.01)
        self.value = max(1.0, self.value * (1.0 + drift))
        self.ts = now
        return self.value

# ================== SIMULATE TOP10 ==================
_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
def rand_wallet(length: int = 44) -> str:
    return "".join(random.choice(_BASE58) for _ in range(length))

class Top10Simulator:
    def __init__(self, kol: KolMap, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
        self.kol = kol
        self.state: List[Tuple[str, float]] = self._bootstrap_initial()
        self.top3_shake_prob = 0.80
        self.top3_swap_prob  = 0.95
        self.top3_boost_min  = 1.05
        self.top3_boost_max  = 1.22
        self.promote_prob      = 0.65
        self.promote_top3_bias = 0.80

    def _bootstrap_initial(self) -> List[Tuple[str, float]]:
        top: List[Tuple[str, float]] = []
        kol_keys = list(self.kol._map.keys()) if self.kol._map else []
        for i in range(10):
            wal = random.choice(kol_keys) if (kol_keys and random.random() < 0.35) else rand_wallet()
            base = max(1.0, 1000.0 - i * random.uniform(30, 90))
            amt = base * random.uniform(0.9, 1.2)
            top.append((wal, amt))
        top.sort(key=lambda kv: kv[1], reverse=True)
        seen = set(); dedup: List[Tuple[str, float]] = []
        for w, a in top:
            if w in seen:
                w = rand_wallet()
            seen.add(w); dedup.append((w, a))
        return dedup[:10]

    def _inject_newcomer(self):
        idx = random.choice([7, 8, 9])
        base_low = self.state[7][1]
        base_mid = self.state[4][1]
        new_amt = random.uniform(0.5*base_mid, 1.1*base_low)
        wal = rand_wallet()
        if self.kol._map and random.random() < 0.4:
            wal = random.choice(list(self.kol._map.keys()))
        if any(w == wal for (w, _) in self.state):
            wal = rand_wallet()
        self.state[idx] = (wal, new_amt)

    def _promote_random(self):
        idx = random.randrange(0, 10 if random.random() >= self.promote_top3_bias else 3)
        w, a = self.state[idx]
        self.state[idx] = (w, a * random.uniform(1.05, 1.35))

    def _swap_neighbors(self):
        i = random.randrange(0, 9)
        self.state[i], self.state[i+1] = self.state[i+1], self.state[i]

    def _replace_with_kol(self):
        if not self.kol._map:
            return
        wal, _ = self.kol.random_kol_wallet() or (None, None)
        if not wal or any(w == wal for (w, _) in self.state):
            return
        idx = random.choice([7, 8, 9])
        ref = self.state[idx][1]
        self.state[idx] = (wal, ref * random.uniform(1.05, 1.25))

    def _top3_shake(self):
        picks = [0, 1, 2]
        random.shuffle(picks)
        k = 1 if random.random() < 0.6 else 2
        for i in picks[:k]:
            w, a = self.state[i]
            self.state[i] = (w, a * random.uniform(self.top3_boost_min, self.top3_boost_max))
        if random.random() < self.top3_swap_prob:
            i = 0 if random.random() < 0.5 else 1
            self.state[i], self.state[i+1] = self.state[i+1], self.state[i]

    def tick(self) -> List[Tuple[str, float]]:
        self.state = [(w, max(0.000001, a*random.uniform(0.98, 1.02))) for (w, a) in self.state]
        if random.random() < self.promote_prob: self._promote_random()
        if random.random() < 0.35: self._inject_newcomer()
        if random.random() < 0.30: self._swap_neighbors()
        if random.random() < 0.25: self._replace_with_kol()
        if random.random() < self.top3_shake_prob: self._top3_shake()
        self.state.sort(key=lambda kv: kv[1], reverse=True)
        w0, a0 = self.state[0]
        self.state[0] = (w0, a0 * random.uniform(0.995, 1.01))
        cap = 1500.0
        if self.state[0][1] > cap:
            factor = cap / self.state[0][1]
            self.state = [(w, a*factor) for (w, a) in self.state]
        return self.state[:10]

    def force_change_rank(self, n: int) -> bool:
        if n < 1 or n > len(self.state):
            return False
        idx = n - 1
        if n == 10:
            j = idx - 1
        else:
            j = idx + 1
        if j < 0 or j >= len(self.state):
            return False
        self.state[idx], self.state[j] = self.state[j], self.state[idx]
        return True

# ================== GLOBALS for instant snapshot ==================
_sim: Optional[Top10Simulator] = None
_last_mint: str = ""
_last_fees_sol: float = 0.0
_last_sol_usd: float = 150.0
_kol_get_fn: Callable[[str], str] = (lambda w: "")

def payout_in_progress() -> bool:
    """[PAUSE LOGIC] Freeze sim/top10 until all three payouts were sent."""
    return bool(_winners_lines) and (_payout_next_idx < 3)

async def broadcast_snapshot():
    """Broadcast current TOP10 using last known fees/price/mint, preceded by ROUND_UPDATE."""
    if _sim is None:
        return
    try:
        # Send round first
        await send_round_update()

        topN = _sim.state[:10]
        payload = make_payload(
            mint=_last_mint,
            topN=topN,
            fees_sol=_last_fees_sol,
            sol_usd=_last_sol_usd,
            kol_get=_kol_get_fn
        )
        await ws_broadcast(payload)
    except Exception as e:
        print(f"[SNAPSHOT] broadcast failed: {e}", flush=True)

# ================== CONTROL (stdin) ==================
async def stdin_command_task(stop_event: asyncio.Event):
    """
    - ENTER (empty line): send next PAYOUT_n with tx from payoutn_sig.txt; also create post_n.txt
                          After PAYOUT_3 + post_3, write website.txt and increment round.txt
    - '0' + ENTER: send SET_COUNTER=0
    - '1'..'10' + ENTER: force-change that rank only, then broadcast snapshot.
    """
    loop = asyncio.get_running_loop()
    print("[CTL] ENTER -> next PAYOUT (1→2→3). '0'+ENTER -> SET_COUNTER=0. '1'..'10'+ENTER -> force-change rank.", flush=True)
    global _payout_next_idx, _winners_lines, _parsed_winners

    while not stop_event.is_set():
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if line is None:
            await asyncio.sleep(0.05)
            continue
        cmd = (line or "").strip()

        # Numeric: force local change of a specific rank
        if cmd.isdigit():
            n = int(cmd)
            if 1 <= n <= 10:
                if _sim and _sim.force_change_rank(n):
                    print(f"[CTL] Forced change on rank #{n}. Broadcasting snapshot…", flush=True)
                    await broadcast_snapshot()
                else:
                    print(f"[CTL] Cannot force-change rank #{n} (sim not ready or invalid).", flush=True)
                continue

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
            sig_file = path_sig(current_round, _payout_next_idx + 1)
            sig = read_first_non_empty_line(sig_file) if sig_file else ""
            if not sig:
                print(f"[PAYOUT] No signature found in {sig_file or '(unknown)'} — sending without link.", flush=True)

            step = _payout_next_idx + 1
            evt = {"type": f"PAYOUT_{step}", "tx": sig, "solscan": sig, "ts": now_iso()}
            print(f"[PAYOUT] CONFIRMED → sending {evt['type']} with tx='{sig or '(empty)'}'", flush=True)
            try:
                await ws_broadcast(evt)
                _payout_next_idx += 1
            except Exception as e:
                print(f"[PAYOUT] Broadcast {evt['type']} failed: {e}", flush=True)

            # === Build & write the X post for this rank under payouts/ROUND_X/
            wallet, sol_amt = ("", None)
            t = _parsed_winners.get(step)
            if t:
                wallet, sol_amt = t[0], t[1]

            current_round_for_post = read_round_file()
            post_text = build_post_text(current_round_for_post, step, wallet or "TBD", sig or "TBD", sol_amt)
            write_text_file(path_post(current_round_for_post, step), post_text)

            # If finished 1..3 → write website.txt then increment round.txt
            if _payout_next_idx >= 3:
                try:
                    # Build website.txt for the JUST-FINISHED round
                    finished_round = read_round_file()
                    sigs = {
                        1: read_first_non_empty_line(path_sig(finished_round, 1)) or "",
                        2: read_first_non_empty_line(path_sig(finished_round, 2)) or "",
                        3: read_first_non_empty_line(path_sig(finished_round, 3)) or "",
                    }
                    winners = {
                        1: (_parsed_winners.get(1, ("", None))[0], _parsed_winners.get(1, ("", None))[1] if _parsed_winners.get(1) else None),
                        2: (_parsed_winners.get(2, ("", None))[0], _parsed_winners.get(2, ("", None))[1] if _parsed_winners.get(2) else None),
                        3: (_parsed_winners.get(3, ("", None))[0], _parsed_winners.get(3, ("", None))[1] if _parsed_winners.get(3) else None),
                    }
                    website_html = build_website_card_html(finished_round, winners, sigs)
                    write_text_file(path_website(finished_round), website_html)
                    print(f"[WEBSITE] website.txt generated for ROUND {finished_round}", flush=True)

                    # Now increment round
                    curr_int = int(finished_round)
                    write_round_file(curr_int + 1)
                except Exception as e:
                    print(f"[ROUND] Failed to finalize website/increment after PAYOUT_3: {e}", flush=True)

                # [PAUSE LOGIC] Unfreeze generation
                _winners_lines = []
                _parsed_winners = {}
                print("[PAUSE] Payout sequence complete → unfreezing TOP10 generation.", flush=True)
                print("[PAYOUT] Sequence PAYOUT_1/2/3 complete. Waiting for new winners…", flush=True)
            else:
                nxt = _payout_next_idx + 1
                nxt_path = path_sig(current_round, nxt)
                nxt_sig = read_first_non_empty_line(nxt_path) or "(no signature found)"
                print(f"[PAYOUT] Ready to send PAYOUT_{nxt}. Signature ({nxt_path}): {nxt_sig}", flush=True)
                print("[PAYOUT] Press ENTER to SEND.", flush=True)

# ================== MAIN LOOP (TEST) ==================
def usd(x, price): return x * price

def read_fees_sol_safe():
    return read_fees_sol(FEES_FILE)

async def main_async(mint: str):
    global _sim, _last_mint, _last_fees_sol, _last_sol_usd, _kol_get_fn, CURRENT_ROUND_ID

    # Initialize round.txt and cache CURRENT_ROUND_ID
    _ensure_round_file_exists()
    CURRENT_ROUND_ID = read_round_file()
    print(f"[ROUND] Current round = {CURRENT_ROUND_ID}", flush=True)

    kol = KolMap(KOL_CSV)
    kol.load_if_changed()
    _kol_get_fn = kol.get

    price_sim = SimSolPrice(
        start_price=float(os.getenv("SIM_SOL_PRICE_START") or "150.0"),
        ttl_sec=SOL_PRICE_TTL_SEC
    )
    seed_env = os.getenv("SIM_SEED")
    _sim = Top10Simulator(kol, seed=int(seed_env)) if seed_env else Top10Simulator(kol)

    ws_server = await start_ws_server()

    stop_event = asyncio.Event()
    ctl_task = asyncio.create_task(stdin_command_task(stop_event))

    _last_mint = mint

    try:
        while not stop_event.is_set():
            try:
                kol.load_if_changed()

                if payout_in_progress():
                    # [PAUSE LOGIC] Do not advance price/top10 or broadcast while payouts pending
                    print("[PAUSE] Payout in progress (waiting for PAYOUT_3). Skipping TOP10 tick & broadcasts.", flush=True)
                else:
                    sol_usd = await price_sim.get()
                    topN = _sim.tick()
                    fees_sol = read_fees_sol_safe()

                    # cache last known values for instant snapshots
                    _last_sol_usd = sol_usd
                    _last_fees_sol = fees_sol

                    print_top_block(
                        mint=mint, rpc="[TEST-MODE: no RPC]", topN=topN,
                        fees_sol=fees_sol, sol_usd=sol_usd, kol_get=kol.get
                    )

                    # ---- Send CURRENT_ROUND_ID then TOP10_UPDATE ----
                    await send_round_update()
                    payload = make_payload(
                        mint=mint, topN=topN, fees_sol=fees_sol, sol_usd=sol_usd, kol_get=kol.get
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
        print("Usage: python coh_top10_test_ws.py <MINT_CA>")
        sys.exit(1)
    m = sys.argv[1].strip()
    try:
        asyncio.run(main_async(m))
    except KeyboardInterrupt:
        print("Bye.")
