"""
Fayda PDF Rental Bot — v2
World-class rewrite: fully async I/O, inline-keyboard UI, connection pooling,
retrying HTTP client, cached reads, concurrent broadcast, and centralized
error handling.
"""

import os
import time
import logging
import asyncio
from logging.handlers import RotatingFileHandler
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass
import threading

import aiosqlite
import httpx

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, User
)
from telegram.constants import ParseMode, ChatAction
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler, ContextTypes,
)
from telegram.error import Forbidden, BadRequest, TelegramError

# ============ CONFIGURATION ============
TOKEN = os.environ.get("BOT_TOKEN")
API_BASE_URL = os.environ.get("API_BASE_URL", "https://faydaapi-production.up.railway.app/api/v1")
API_KEY = os.environ.get("API_KEY")
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "dhtechs_admin")
CHANNEL_USERNAME = os.environ.get("CHANNEL_USERNAME", "faydatech")
ADMIN_IDS = [int(i) for i in os.environ.get("ADMIN_IDS", "").split(",") if i.strip()]
PAYMENT_PHONE = os.environ.get("PAYMENT_PHONE", "0919545335")

# On Railway, only a mounted Volume survives redeploys/restarts. Set DATA_DIR
# to that volume's mount path (e.g. "/data") in your Railway service
# variables. Defaults to the working directory for local runs.
DATA_DIR = os.environ.get("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.environ.get("DB_PATH", os.path.join(DATA_DIR, "fayda_bot.db"))
SCREENSHOTS_DIR = os.path.join(DATA_DIR, "screenshots")
LOGS_DIR = os.path.join(DATA_DIR, "logs")

if not TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required!")
if not API_KEY:
    raise ValueError("API_KEY environment variable is required!")

# Pricing
PRICE_PER_PDF = 5
MINIMUM_PURCHASE = 150
UNLIMITED_WEEK = 1000
UNLIMITED_MONTH = 5000
TRIAL_PDFS = 1
INVITE_BONUS = 2

# Conversation states
MAIN_MENU, PAYMENT_AMOUNT, WAITING_SCREENSHOT, FAN_INPUT, OTP_INPUT, ADMIN_MENU = range(6)

# ============ LOGGING ============
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),  # Railway captures stdout/stderr as your deploy logs
        RotatingFileHandler(os.path.join(LOGS_DIR, "bot.log"), maxBytes=2_000_000, backupCount=3),
    ],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logger = logging.getLogger("fayda_bot")

# ============ HEALTH CHECK SERVER ============
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        elif self.path == "/":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h1>Fayda Rental Bot is running!</h1>")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info(f"Health check server running on port {port}")
    server.serve_forever()


# ============ DATA MODELS ============
@dataclass
class UserData:
    user_id: int
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    balance: int = 0
    pdf_balance: int = 0
    total_downloads: int = 0
    is_admin: bool = False
    is_banned: bool = False
    trial_used: bool = False
    unlimited_expiry: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""
    invited_by: Optional[int] = None
    invite_count: int = 0


@dataclass
class PaymentRequest:
    id: int
    user_id: int
    amount: int
    screenshot_path: str
    status: str
    created_at: str
    processed_at: Optional[str] = None


# ============ ASYNC DATABASE ============
class Database:
    """Fully async DB layer (aiosqlite) with WAL mode and a short-lived
    in-memory cache for hot reads (get_user is called on nearly every
    handler). Writes invalidate the relevant cache entries."""

    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
        self._user_cache: Dict[int, Tuple[UserData, float]] = {}
        self._cache_ttl = 6.0

    async def connect(self):
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._init_schema()
        logger.info("Database connected (WAL mode)")

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def _init_schema(self):
        await self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                balance INTEGER DEFAULT 0,
                pdf_balance INTEGER DEFAULT 0,
                total_downloads INTEGER DEFAULT 0,
                is_admin INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                trial_used INTEGER DEFAULT 0,
                unlimited_expiry TEXT,
                created_at TEXT,
                updated_at TEXT,
                invited_by INTEGER,
                invite_count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS payment_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                screenshot_path TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT,
                processed_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            CREATE TABLE IF NOT EXISTS download_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                session_id TEXT,
                timestamp TEXT,
                status TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            CREATE INDEX IF NOT EXISTS idx_payments_status ON payment_requests(status);
            CREATE INDEX IF NOT EXISTS idx_downloads_user ON download_logs(user_id);
        """)
        await self._conn.commit()

    @staticmethod
    def _row_to_user(row: aiosqlite.Row) -> UserData:
        d = dict(row)
        d["is_admin"] = bool(d["is_admin"])
        d["is_banned"] = bool(d["is_banned"])
        d["trial_used"] = bool(d["trial_used"])
        return UserData(**d)

    async def get_user(self, user_id: int) -> Optional[UserData]:
        now = time.monotonic()
        cached = self._user_cache.get(user_id)
        if cached and now - cached[1] < self._cache_ttl:
            return cached[0]
        async with self._lock:
            cur = await self._conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
        if not row:
            return None
        user = self._row_to_user(row)
        self._user_cache[user_id] = (user, now)
        return user

    def _invalidate(self, user_id: int):
        self._user_cache.pop(user_id, None)

    async def create_user(self, user: User, invited_by: Optional[int] = None) -> UserData:
        now = datetime.now().isoformat()
        async with self._lock:
            await self._conn.execute(
                """INSERT INTO users (user_id, username, first_name, last_name, created_at, updated_at, invited_by)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user.id, user.username or "", user.first_name or "", user.last_name or "", now, now, invited_by),
            )
            if invited_by:
                await self._conn.execute(
                    "UPDATE users SET pdf_balance = pdf_balance + ? WHERE user_id = ?",
                    (INVITE_BONUS, invited_by),
                )
                await self._conn.execute(
                    "UPDATE users SET invite_count = invite_count + 1 WHERE user_id = ?",
                    (invited_by,),
                )
            await self._conn.commit()
        self._invalidate(user.id)
        if invited_by:
            self._invalidate(invited_by)
        return await self.get_user(user.id)

    async def update_user(self, user_data: UserData):
        async with self._lock:
            await self._conn.execute(
                """UPDATE users SET
                    username = ?, first_name = ?, last_name = ?,
                    balance = ?, pdf_balance = ?, total_downloads = ?,
                    is_admin = ?, is_banned = ?, trial_used = ?,
                    unlimited_expiry = ?, updated_at = ?, invite_count = ?
                   WHERE user_id = ?""",
                (
                    user_data.username, user_data.first_name, user_data.last_name,
                    user_data.balance, user_data.pdf_balance, user_data.total_downloads,
                    int(user_data.is_admin), int(user_data.is_banned), int(user_data.trial_used),
                    user_data.unlimited_expiry, datetime.now().isoformat(), user_data.invite_count,
                    user_data.user_id,
                ),
            )
            await self._conn.commit()
        self._invalidate(user_data.user_id)

    async def get_all_users(self) -> List[UserData]:
        async with self._lock:
            cur = await self._conn.execute("SELECT * FROM users ORDER BY created_at DESC")
            rows = await cur.fetchall()
        return [self._row_to_user(r) for r in rows]

    async def create_payment_request(self, user_id: int, amount: int, screenshot_path: str) -> int:
        now = datetime.now().isoformat()
        async with self._lock:
            cur = await self._conn.execute(
                """INSERT INTO payment_requests (user_id, amount, screenshot_path, created_at)
                   VALUES (?, ?, ?, ?)""",
                (user_id, amount, screenshot_path, now),
            )
            await self._conn.commit()
            return cur.lastrowid

    async def get_pending_payments(self) -> List[PaymentRequest]:
        async with self._lock:
            cur = await self._conn.execute(
                "SELECT * FROM payment_requests WHERE status = 'pending' ORDER BY created_at ASC"
            )
            rows = await cur.fetchall()
        return [PaymentRequest(**dict(r)) for r in rows]

    async def approve_payment(self, payment_id: int) -> Optional[Tuple[int, int]]:
        """Approves a payment and credits the user. Returns (user_id, amount) or None."""
        async with self._lock:
            now = datetime.now().isoformat()
            await self._conn.execute(
                "UPDATE payment_requests SET status = 'approved', processed_at = ? WHERE id = ? AND status = 'pending'",
                (now, payment_id),
            )
            cur = await self._conn.execute(
                "SELECT user_id, amount FROM payment_requests WHERE id = ?", (payment_id,)
            )
            payment = await cur.fetchone()
            if not payment:
                await self._conn.commit()
                return None
            user_id, amount = payment["user_id"], payment["amount"]

            if amount >= UNLIMITED_MONTH:
                expiry = (datetime.now() + timedelta(days=30)).isoformat()
                await self._conn.execute(
                    "UPDATE users SET balance = balance + ?, unlimited_expiry = ? WHERE user_id = ?",
                    (amount, expiry, user_id),
                )
            elif amount >= UNLIMITED_WEEK:
                expiry = (datetime.now() + timedelta(days=7)).isoformat()
                await self._conn.execute(
                    "UPDATE users SET balance = balance + ?, unlimited_expiry = ? WHERE user_id = ?",
                    (amount, expiry, user_id),
                )
            else:
                pdfs = amount // PRICE_PER_PDF
                await self._conn.execute(
                    "UPDATE users SET pdf_balance = pdf_balance + ? WHERE user_id = ?",
                    (pdfs, user_id),
                )
            await self._conn.commit()
        self._invalidate(user_id)
        return (user_id, amount)

    async def reject_payment(self, payment_id: int) -> Optional[int]:
        async with self._lock:
            now = datetime.now().isoformat()
            cur = await self._conn.execute(
                "SELECT user_id FROM payment_requests WHERE id = ? AND status = 'pending'", (payment_id,)
            )
            row = await cur.fetchone()
            if not row:
                return None
            await self._conn.execute(
                "UPDATE payment_requests SET status = 'rejected', processed_at = ? WHERE id = ?",
                (now, payment_id),
            )
            await self._conn.commit()
        return row["user_id"]

    async def log_download(self, user_id: int, session_id: str, status: str):
        async with self._lock:
            await self._conn.execute(
                "INSERT INTO download_logs (user_id, session_id, timestamp, status) VALUES (?, ?, ?, ?)",
                (user_id, session_id, datetime.now().isoformat(), status),
            )
            await self._conn.execute(
                "UPDATE users SET total_downloads = total_downloads + 1 WHERE user_id = ?", (user_id,)
            )
            await self._conn.commit()
        self._invalidate(user_id)

    async def get_download_stats(self, user_id: int) -> Dict:
        async with self._lock:
            cur = await self._conn.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as successful,
                          SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
                   FROM download_logs WHERE user_id = ?""",
                (user_id,),
            )
            row = await cur.fetchone()
        return dict(row)

    async def get_total_stats(self) -> Dict:
        async with self._lock:
            cur = await self._conn.execute("SELECT COUNT(*) as n FROM users")
            total_users = (await cur.fetchone())["n"]
            cur = await self._conn.execute("SELECT COUNT(*) as n FROM users WHERE is_banned = 0")
            active_users = (await cur.fetchone())["n"]
            cur = await self._conn.execute("SELECT SUM(total_downloads) as n FROM users")
            total_downloads = (await cur.fetchone())["n"] or 0
            cur = await self._conn.execute("SELECT COUNT(*) as n FROM payment_requests WHERE status='pending'")
            pending_payments = (await cur.fetchone())["n"]
        return {
            "total_users": total_users,
            "active_users": active_users,
            "total_downloads": total_downloads,
            "pending_payments": pending_payments,
        }


db = Database()

# ============ RESILIENT API CLIENT ============
class FaydaAPIClient:
    """Async HTTP client with pooling + retry/backoff for the Fayda API."""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.headers = {"x-api-key": api_key, "Content-Type": "application/json"}
        self.client = httpx.AsyncClient(
            headers=self.headers,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )

    async def close(self):
        await self.client.aclose()

    async def _post(self, path: str, json: dict, timeout: float, retries: int = 2) -> httpx.Response:
        last_exc = None
        for attempt in range(retries + 1):
            try:
                return await self.client.post(f"{self.base_url}{path}", json=json, timeout=timeout)
            except (httpx.TimeoutException, httpx.NetworkError) as e:
                last_exc = e
                if attempt < retries:
                    await asyncio.sleep(1.5 * (attempt + 1))
        raise last_exc

    async def send_otp(self, fan: str) -> dict:
        resp = await self._post("/session/send-otp", {"fan": fan, "server": "server3"}, timeout=30)
        return resp.json()

    async def verify_otp(self, session_id: str, otp: str) -> httpx.Response:
        return await self._post(
            "/session/verify-otp",
            {"sessionId": session_id, "otp": otp, "responseMode": "pdf", "includeScreenshots": False},
            timeout=60,
            retries=0,  # never retry a spent OTP
        )


api_client: Optional[FaydaAPIClient] = None

# ============ HELPERS ============
def is_admin_cached_user(user: Optional[UserData]) -> bool:
    return bool(user and user.is_admin)


def get_user_credit(user: UserData) -> int:
    if user.unlimited_expiry:
        try:
            if datetime.fromisoformat(user.unlimited_expiry) > datetime.now():
                return 999_999
        except ValueError:
            pass
    return user.pdf_balance


def format_user_status(user: UserData) -> str:
    credit = get_user_credit(user)
    credit_display = "♾️ Unlimited" if credit >= 999_999 else str(credit)
    unlimited = ""
    if user.unlimited_expiry:
        try:
            expiry = datetime.fromisoformat(user.unlimited_expiry)
            if expiry > datetime.now():
                days_left = (expiry - datetime.now()).days
                unlimited = f"\n♾️ *Unlimited until:* {expiry.strftime('%Y-%m-%d')} ({days_left}d left)"
        except ValueError:
            pass
    return (
        f"👤 *Your Profile*\n"
        f"━━━━━━━━━━━━━━\n"
        f"🆔 `{user.user_id}`\n"
        f"📛 {user.first_name} {user.last_name or ''}\n"
        f"💰 Balance: *{user.balance} Birr*\n"
        f"📄 PDF Credits: *{credit_display}*{unlimited}\n"
        f"📥 Total Downloads: *{user.total_downloads}*\n"
        f"👥 Invites: *{user.invite_count}*\n"
        f"━━━━━━━━━━━━━━"
    )


async def safe_send(bot, chat_id: int, **kwargs) -> bool:
    """Send a message, swallowing expected failures (blocked bot, etc.)."""
    try:
        await bot.send_message(chat_id, **kwargs)
        return True
    except (Forbidden, BadRequest):
        return False
    except TelegramError as e:
        logger.warning(f"send_message failed for {chat_id}: {e}")
        return False


def mask_fan(fan: str) -> str:
    return f"{fan[:4]}****{fan[-4:]}"


# ============ KEYBOARDS ============
def kb_main(user: UserData) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📄 Download PDF", callback_data="menu:download")],
        [InlineKeyboardButton("📊 My Status", callback_data="menu:status"),
         InlineKeyboardButton("💰 Buy Credits", callback_data="menu:buy")],
        [InlineKeyboardButton("🎁 Invite Friends", callback_data="menu:invite"),
         InlineKeyboardButton("📞 Support", callback_data="menu:support")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="menu:help")],
    ]
    if user.is_admin:
        rows.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="menu:admin")])
    return InlineKeyboardMarkup(rows)


def kb_back(target: str = "nav:main", label: str = "🔙 Back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=target)]])


def kb_buy() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"💳 {MINIMUM_PURCHASE} Birr ({MINIMUM_PURCHASE // PRICE_PER_PDF} PDFs)", callback_data=f"buy:{MINIMUM_PURCHASE}")],
        [InlineKeyboardButton(f"♾️ {UNLIMITED_WEEK} Birr (1 Week Unlimited)", callback_data=f"buy:{UNLIMITED_WEEK}")],
        [InlineKeyboardButton(f"♾️ {UNLIMITED_MONTH} Birr (1 Month Unlimited)", callback_data=f"buy:{UNLIMITED_MONTH}")],
        [InlineKeyboardButton("🔙 Back", callback_data="nav:main")],
    ])


def kb_invite() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="nav:main")]])


def kb_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="nav:main")]])


def kb_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics", callback_data="admin:stats"),
         InlineKeyboardButton("💰 Pending Payments", callback_data="admin:pending")],
        [InlineKeyboardButton("✅ Approve", callback_data="admin:approve"),
         InlineKeyboardButton("❌ Reject", callback_data="admin:reject")],
        [InlineKeyboardButton("👥 Users", callback_data="admin:users"),
         InlineKeyboardButton("🚫 Ban User", callback_data="admin:ban")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin:broadcast"),
         InlineKeyboardButton("📥 Logs", callback_data="admin:logs")],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="nav:main")],
    ])


# ===== helper to (re)render the main menu, editing in place when possible =====
async def render_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, greeting: Optional[str] = None):
    user_id = update.effective_user.id
    user = await db.get_user(user_id)
    text = greeting or "🏠 *Main Menu*\n\nWhat would you like to do?"
    markup = kb_main(user)
    query = update.callback_query
    if query:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
    return MAIN_MENU


# ===== START =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    user_id = user.id
    existing = await db.get_user(user_id)

    if not existing:
        invited_by = None
        if context.args:
            try:
                candidate = int(context.args[0])
                if candidate != user_id and await db.get_user(candidate):
                    invited_by = candidate
            except ValueError:
                pass

        await db.create_user(user, invited_by)

        await update.message.reply_text(
            f"🎉 *Welcome to Fayda PDF Download Bot!*\n\n"
            f"📌 Join our channel for updates:\n"
            f"👉 [t.me/{CHANNEL_USERNAME}](https://t.me/{CHANNEL_USERNAME})\n\n"
            f"📖 *How it works:*\n"
            f"1️⃣ Get your FAN number (16 digits)\n"
            f"2️⃣ Use credits to download PDFs\n"
            f"3️⃣ Each download costs 5 Birr\n\n"
            f"🎁 You have *{TRIAL_PDFS} free trial download*!",
            reply_markup=kb_main(await db.get_user(user_id)),
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )

        if ADMIN_IDS:
            mention = f"@{user.username}" if user.username else f"[{user.first_name}](tg://user?id={user_id})"
            text = (
                f"🆕 *New User Joined!*\n\n👤 {mention}\n🆔 `{user_id}`\n"
                f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            await asyncio.gather(*(
                safe_send(context.bot, aid, text=text, parse_mode=ParseMode.MARKDOWN)
                for aid in ADMIN_IDS
            ))
    else:
        await update.message.reply_text(
            f"👋 Welcome back, {user.first_name}!",
            reply_markup=kb_main(existing),
        )

    return MAIN_MENU


# ===== MAIN MENU CALLBACKS =====
async def cb_nav_main(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.callback_query.answer()
    context.user_data.clear()
    return await render_main_menu(update, context)


async def cb_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    user = await db.get_user(user_id)

    if user and user.is_banned:
        await query.edit_message_text("🚫 You are banned from using this bot.")
        return MAIN_MENU

    action = query.data.split(":", 1)[1]

    if action == "download":
        return await start_download(update, context)

    if action == "status":
        await query.edit_message_text(
            format_user_status(user), parse_mode=ParseMode.MARKDOWN, reply_markup=kb_back()
        )
        return MAIN_MENU

    if action == "buy":
        await query.edit_message_text(
            f"💰 *Purchase Credits*\n\n"
            f"💳 {MINIMUM_PURCHASE} Birr → {MINIMUM_PURCHASE // PRICE_PER_PDF} PDF downloads\n"
            f"♾️ {UNLIMITED_WEEK} Birr → Unlimited for 1 week\n"
            f"♾️ {UNLIMITED_MONTH} Birr → Unlimited for 1 month\n\n"
            f"Choose an option below:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_buy(),
        )
        return PAYMENT_AMOUNT

    if action == "invite":
        await query.edit_message_text(
            f"🎁 *Invite Friends & Earn!*\n\n"
            f"Get *{INVITE_BONUS} free PDF downloads* for every friend who joins with your link.\n\n"
            f"👥 Your invites so far: *{user.invite_count}*\n\n"
            f"🔗 Your link:\n`https://t.me/{context.bot.username}?start={user_id}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_invite(),
        )
        return MAIN_MENU

    if action == "support":
        await query.edit_message_text(
            f"📞 *Support*\n\n"
            f"👤 @{ADMIN_USERNAME}\n"
            f"📱 {PAYMENT_PHONE}\n\n"
            f"📌 [t.me/{CHANNEL_USERNAME}](https://t.me/{CHANNEL_USERNAME})",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_back(),
            disable_web_page_preview=True,
        )
        return MAIN_MENU

    if action == "help":
        await query.edit_message_text(
            f"ℹ️ *Help Guide*\n\n"
            f"*How to Download:*\n"
            f"1. Tap 📄 Download PDF\n"
            f"2. Enter your 16-digit FAN number\n"
            f"3. Enter the OTP you receive\n\n"
            f"*Pricing:*\n"
            f"• {PRICE_PER_PDF} Birr per PDF\n"
            f"• Minimum: {MINIMUM_PURCHASE} Birr ({MINIMUM_PURCHASE // PRICE_PER_PDF} PDFs)\n"
            f"• Unlimited: {UNLIMITED_WEEK} Birr/week or {UNLIMITED_MONTH} Birr/month\n\n"
            f"*Bonuses:*\n"
            f"• Free trial: {TRIAL_PDFS} PDF\n"
            f"• Referral: {INVITE_BONUS} PDFs/invite\n\n"
            f"*Payment:* TeleBirr → {PAYMENT_PHONE}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_back(),
        )
        return MAIN_MENU

    if action == "admin":
        return await show_admin_panel(update, context)

    return MAIN_MENU


# ===== DOWNLOAD FLOW =====
async def start_download(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user_id = update.effective_user.id
    user = await db.get_user(user_id)
    credit = get_user_credit(user)

    if credit <= 0 and not user.trial_used:
        user.pdf_balance += TRIAL_PDFS
        user.trial_used = True
        await db.update_user(user)
        await query.edit_message_text(
            f"🎁 *Free trial activated!* You have {TRIAL_PDFS} free download.\n\n"
            f"📄 Enter your 16-digit FAN number:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_cancel(),
        )
        return FAN_INPUT

    if credit <= 0:
        await query.edit_message_text(
            f"⚠️ *Insufficient balance!*\n\nYou have 0 PDF credits.\nEach download costs {PRICE_PER_PDF} Birr.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💰 Buy Credits", callback_data="menu:buy")],
                [InlineKeyboardButton("🔙 Back", callback_data="nav:main")],
            ]),
        )
        return MAIN_MENU

    credit_display = "♾️ Unlimited" if credit >= 999_999 else str(credit)
    await query.edit_message_text(
        f"📄 *Enter your 16-digit FAN number:*\n\nAvailable PDFs: {credit_display}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_cancel(),
    )
    return FAN_INPUT


async def handle_fan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    user_id = update.effective_user.id

    if not text.isdigit() or len(text) != 16:
        await update.message.reply_text(
            "❌ Invalid FAN number. Please enter exactly 16 digits, or tap Cancel:",
            reply_markup=kb_cancel(),
        )
        return FAN_INPUT

    context.user_data["fan"] = text
    status_msg = await update.message.reply_text(
        f"📤 Sending OTP to FAN `{mask_fan(text)}`…", parse_mode=ParseMode.MARKDOWN
    )
    await context.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)

    try:
        data = await api_client.send_otp(text)
        if data.get("success"):
            session_id = data.get("data", {}).get("sessionId")
            if session_id:
                context.user_data["session_id"] = session_id
                await status_msg.edit_text(
                    "✅ OTP sent successfully!\n\n📱 Enter the OTP code you received:",
                    reply_markup=kb_cancel(),
                )
                return OTP_INPUT
            await status_msg.edit_text(
                "❌ Failed to get a session ID. Please try again.",
                reply_markup=kb_back(),
            )
            return MAIN_MENU
        error_msg = data.get("message", "Unknown error")
        await status_msg.edit_text(
            f"❌ Failed to send OTP: {error_msg}", reply_markup=kb_back()
        )
        return MAIN_MENU
    except Exception as e:
        logger.exception("send_otp failed")
        await status_msg.edit_text(
            f"❌ Network error: {str(e)[:150]}\nPlease try again later.",
            reply_markup=kb_back(),
        )
        return MAIN_MENU


async def handle_otp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    user_id = update.effective_user.id
    user = await db.get_user(user_id)

    session_id = context.user_data.get("session_id")
    if not session_id:
        await update.message.reply_text("❌ Session expired. Please start again.", reply_markup=kb_back())
        return MAIN_MENU

    status_msg = await update.message.reply_text("⏳ Verifying OTP and generating your PDF…")
    await context.bot.send_chat_action(update.effective_chat.id, ChatAction.UPLOAD_DOCUMENT)

    try:
        response = await api_client.verify_otp(session_id, text)

        if response.status_code == 200 and "application/pdf" in response.headers.get("content-type", ""):
            if get_user_credit(user) < 999_999:
                user.pdf_balance = max(0, user.pdf_balance - 1)
            await db.update_user(user)
            await db.log_download(user_id, session_id, "success")
            user = await db.get_user(user_id)

            await status_msg.delete()
            credit_display = "♾️ Unlimited" if get_user_credit(user) >= 999_999 else str(get_user_credit(user))
            await update.message.reply_document(
                document=InputFile(response.content, filename=f"fayda_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"),
                caption=(
                    f"✅ *PDF downloaded successfully!*\n\n"
                    f"📄 Remaining credits: {credit_display}\n"
                    f"📥 Total downloads: {user.total_downloads}"
                ),
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_main(user),
            )
            context.user_data.clear()
            return MAIN_MENU

        try:
            error_msg = response.json().get("message", "Unknown error")
        except Exception:
            error_msg = f"Server error ({response.status_code})"
        await db.log_download(user_id, session_id, "failed")
        await status_msg.edit_text(f"❌ Download failed: {error_msg}", reply_markup=kb_back())
        context.user_data.clear()
        return MAIN_MENU

    except Exception as e:
        logger.exception("verify_otp failed")
        await status_msg.edit_text(
            f"❌ Error: {str(e)[:150]}\nPlease try again.", reply_markup=kb_back()
        )
        return MAIN_MENU


# ===== PAYMENT FLOW =====
async def cb_buy_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    amount = int(query.data.split(":", 1)[1])
    context.user_data["payment_amount"] = amount

    await query.edit_message_text(
        f"💰 *Payment Request*\n\n"
        f"Amount: *{amount} Birr*\n\n"
        f"📱 Send payment to *{PAYMENT_PHONE}* via TeleBirr,\n"
        f"then send a screenshot of the confirmation here.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_cancel(),
    )
    return WAITING_SCREENSHOT


async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id

    if not update.message.photo:
        await update.message.reply_text(
            "📸 Please send a payment screenshot photo, or tap Cancel.",
            reply_markup=kb_cancel(),
        )
        return WAITING_SCREENSHOT

    amount = context.user_data.get("payment_amount")
    if not amount:
        await update.message.reply_text("❌ Payment session expired. Please start again.", reply_markup=kb_back())
        return MAIN_MENU

    photo = update.message.photo[-1]
    file = await photo.get_file()
    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
    file_path = os.path.join(SCREENSHOTS_DIR, f"payment_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg")
    await file.download_to_drive(file_path)

    payment_id = await db.create_payment_request(user_id, amount, file_path)

    await update.message.reply_text(
        f"✅ *Payment request #{payment_id} received!*\n\n"
        f"💰 Amount: {amount} Birr\n"
        f"⏳ Please wait for admin approval — you'll be notified automatically.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_main(await db.get_user(user_id)),
    )

    if ADMIN_IDS:
        caption = (
            f"💰 *New Payment Request #{payment_id}*\n\n"
            f"👤 @{update.effective_user.username or 'N/A'}\n"
            f"🆔 `{user_id}`\n"
            f"💳 Amount: {amount} Birr\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )

        async def notify(admin_id: int):
            try:
                await context.bot.send_photo(
                    admin_id, photo=InputFile(file_path), caption=caption, parse_mode=ParseMode.MARKDOWN
                )
            except TelegramError as e:
                logger.warning(f"admin notify failed for {admin_id}: {e}")

        await asyncio.gather(*(notify(a) for a in ADMIN_IDS))

    context.user_data.clear()
    return MAIN_MENU


# ===== ADMIN PANEL =====
async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = await db.get_user(user_id)
    if not is_admin_cached_user(user):
        if update.callback_query:
            await update.callback_query.answer("⛔ Access denied.", show_alert=True)
        return MAIN_MENU

    stats = await db.get_total_stats()
    text = (
        f"⚙️ *Admin Panel*\n\n"
        f"👥 Total Users: {stats['total_users']}\n"
        f"🟢 Active Users: {stats['active_users']}\n"
        f"📥 Total Downloads: {stats['total_downloads']}\n"
        f"💰 Pending Payments: {stats['pending_payments']}"
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb_admin())
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb_admin())
    return ADMIN_MENU


async def cb_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user_id = update.effective_user.id
    user = await db.get_user(user_id)
    if not is_admin_cached_user(user):
        await query.answer("⛔ Access denied.", show_alert=True)
        return MAIN_MENU
    await query.answer()

    action = query.data.split(":", 1)[1]

    if action == "stats":
        stats = await db.get_total_stats()
        users = await db.get_all_users()
        total_pdfs = sum(get_user_credit(u) for u in users if get_user_credit(u) < 999_999)
        await query.edit_message_text(
            f"📊 *Full Statistics*\n\n"
            f"👥 Total Users: {stats['total_users']}\n"
            f"🟢 Active: {stats['active_users']}\n"
            f"🚫 Banned: {stats['total_users'] - stats['active_users']}\n"
            f"📥 Total Downloads: {stats['total_downloads']}\n"
            f"📄 Total PDF Credits (finite): {total_pdfs}\n"
            f"💰 Pending Payments: {stats['pending_payments']}\n\n"
            f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_admin(),
        )
        return ADMIN_MENU

    if action == "pending":
        payments = await db.get_pending_payments()
        if not payments:
            await query.edit_message_text("✅ No pending payments.", reply_markup=kb_admin())
            return ADMIN_MENU
        msg = "💰 *Pending Payments*\n\n"
        for p in payments[:10]:
            u = await db.get_user(p.user_id)
            name = f"@{u.username}" if u and u.username else f"ID {p.user_id}"
            msg += f"`#{p.id}` — {name} — *{p.amount} Birr*\n"
        if len(payments) > 10:
            msg += f"\n… and {len(payments) - 10} more"
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=kb_admin())
        return ADMIN_MENU

    if action == "approve":
        context.user_data["admin_action"] = "approve"
        await query.edit_message_text(
            "✏️ Send the payment ID to *approve*:", parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_back("nav:admin"),
        )
        return ADMIN_MENU

    if action == "reject":
        context.user_data["admin_action"] = "reject"
        await query.edit_message_text(
            "✏️ Send the payment ID to *reject*:", parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_back("nav:admin"),
        )
        return ADMIN_MENU

    if action == "users":
        users = await db.get_all_users()
        msg = "👥 *All Users*\n\n"
        for u in users[:25]:
            status_icon = "🚫" if u.is_banned else "🟢"
            credit = get_user_credit(u)
            credit_str = "♾️" if credit >= 999_999 else str(credit)
            msg += f"{status_icon} `{u.user_id}` @{u.username or 'N/A'} 📄{credit_str}\n"
        if len(users) > 25:
            msg += f"\n… and {len(users) - 25} more"
        await query.edit_message_text(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=kb_admin())
        return ADMIN_MENU

    if action == "ban":
        context.user_data["admin_action"] = "ban"
        await query.edit_message_text(
            "✏️ Send the user ID to *ban*:", parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_back("nav:admin"),
        )
        return ADMIN_MENU

    if action == "broadcast":
        context.user_data["admin_action"] = "broadcast"
        await query.edit_message_text(
            "✏️ Send the message to broadcast to *all users*:", parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_back("nav:admin"),
        )
        return ADMIN_MENU

    if action == "logs":
        await query.edit_message_text("📥 Generating download logs…", reply_markup=kb_admin())
        users = await db.get_all_users()
        lines = [f"📥 DOWNLOAD LOGS", f"Generated: {datetime.now().isoformat()}", "=" * 50, ""]
        for u in users:
            s = await db.get_download_stats(u.user_id)
            lines.append(f"@{u.username or u.first_name or u.user_id}")
            lines.append(f"  Total: {s['total'] or 0} | Success: {s['successful'] or 0} | Failed: {s['failed'] or 0}")
        os.makedirs(LOGS_DIR, exist_ok=True)
        log_path = os.path.join(LOGS_DIR, f"downloads_{datetime.now().strftime('%Y%m%d')}.txt")
        with open(log_path, "w") as f:
            f.write("\n".join(lines))
        await context.bot.send_document(
            update.effective_chat.id,
            document=InputFile(log_path, filename=os.path.basename(log_path)),
            caption="📥 Download logs",
        )
        return ADMIN_MENU

    return ADMIN_MENU


async def cb_nav_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.callback_query.answer()
    context.user_data.pop("admin_action", None)
    return await show_admin_panel(update, context)


async def handle_admin_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles free-text replies for approve/reject/ban/broadcast prompts."""
    user_id = update.effective_user.id
    user = await db.get_user(user_id)
    if not is_admin_cached_user(user):
        await update.message.reply_text("⛔ Access denied.")
        return MAIN_MENU

    action = context.user_data.get("admin_action")
    text = update.message.text.strip()

    if action == "approve":
        try:
            payment_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ Invalid payment ID. Enter a number.", reply_markup=kb_admin())
            context.user_data.pop("admin_action", None)
            return ADMIN_MENU
        result = await db.approve_payment(payment_id)
        if result:
            target_id, amount = result
            await update.message.reply_text(f"✅ Payment #{payment_id} approved!", reply_markup=kb_admin())
            await safe_send(
                context.bot, target_id,
                text=f"✅ *Payment Approved!*\n\n💰 {amount} Birr credited.\nUse /start to download PDFs.",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await update.message.reply_text(f"❌ Payment #{payment_id} not found or already processed.", reply_markup=kb_admin())

    elif action == "reject":
        try:
            payment_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ Invalid payment ID. Enter a number.", reply_markup=kb_admin())
            context.user_data.pop("admin_action", None)
            return ADMIN_MENU
        target_id = await db.reject_payment(payment_id)
        if target_id:
            await update.message.reply_text(f"❌ Payment #{payment_id} rejected.", reply_markup=kb_admin())
            await safe_send(
                context.bot, target_id,
                text=f"❌ *Payment Rejected*\n\nContact support if you believe this is a mistake.",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await update.message.reply_text(f"❌ Payment #{payment_id} not found or already processed.", reply_markup=kb_admin())

    elif action == "ban":
        try:
            target_id = int(text)
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID.", reply_markup=kb_admin())
            context.user_data.pop("admin_action", None)
            return ADMIN_MENU
        if target_id == user_id:
            await update.message.reply_text("❌ You cannot ban yourself!", reply_markup=kb_admin())
        else:
            target = await db.get_user(target_id)
            if target:
                target.is_banned = True
                await db.update_user(target)
                await update.message.reply_text(f"🚫 User {target_id} banned.", reply_markup=kb_admin())
                await safe_send(context.bot, target_id, text="🚫 You have been banned from using this bot.")
            else:
                await update.message.reply_text(f"❌ User {target_id} not found.", reply_markup=kb_admin())

    elif action == "broadcast":
        users = await db.get_all_users()
        active = [u for u in users if not u.is_banned]
        progress = await update.message.reply_text(f"📢 Broadcasting to {len(active)} users…")

        sem = asyncio.Semaphore(25)

        async def send_one(uid: int) -> bool:
            async with sem:
                ok = await safe_send(
                    context.bot, uid, text=f"📢 *Announcement*\n\n{text}", parse_mode=ParseMode.MARKDOWN
                )
                await asyncio.sleep(0.04)
                return ok

        results = await asyncio.gather(*(send_one(u.user_id) for u in active))
        success = sum(results)
        failed = len(results) - success
        await progress.edit_text(f"📢 Broadcast complete!\n✅ Sent: {success}\n❌ Failed: {failed}")
        await update.message.reply_text("Back to admin panel:", reply_markup=kb_admin())

    else:
        await update.message.reply_text("Please use the admin buttons.", reply_markup=kb_admin())

    context.user_data.pop("admin_action", None)
    return ADMIN_MENU


# ===== FALLBACK / CANCEL =====
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    user = await db.get_user(update.effective_user.id)
    await update.message.reply_text("Operation cancelled.", reply_markup=kb_main(user))
    return MAIN_MENU


async def fallback_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Catches stray text sent outside any expected step."""
    user = await db.get_user(update.effective_user.id)
    await update.message.reply_text(
        "Please use the buttons below, or /start to reset.", reply_markup=kb_main(user)
    )
    return MAIN_MENU


# ===== GLOBAL ERROR HANDLER =====
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                "⚠️ Something went wrong on our end. Please try /start again."
            )
    except Exception:
        pass


# ===== LIFECYCLE =====
async def post_init(application: Application):
    global api_client
    await db.connect()
    api_client = FaydaAPIClient(API_BASE_URL, API_KEY)
    for admin_id in ADMIN_IDS:
        u = await db.get_user(admin_id)
        if u and not u.is_admin:
            u.is_admin = True
            await db.update_user(u)
    logger.info(f"Bot ready. Username: @{application.bot.username}")


async def post_shutdown(application: Application):
    if api_client:
        await api_client.close()
    await db.close()


# ===== MAIN =====
def main():
    logger.info("Starting Fayda Rental Bot…")
    logger.info(f"API URL: {API_BASE_URL}")
    logger.info(f"Admin IDs: {ADMIN_IDS}")

    threading.Thread(target=run_health_server, daemon=True).start()

    app = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MAIN_MENU: [
                CallbackQueryHandler(cb_main_menu, pattern=r"^menu:"),
            ],
            PAYMENT_AMOUNT: [
                CallbackQueryHandler(cb_buy_amount, pattern=r"^buy:"),
            ],
            WAITING_SCREENSHOT: [
                MessageHandler(filters.PHOTO, handle_screenshot),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_screenshot),
            ],
            FAN_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_fan),
            ],
            OTP_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_otp),
            ],
            ADMIN_MENU: [
                CallbackQueryHandler(cb_admin_menu, pattern=r"^admin:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_text_input),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", start),
            CallbackQueryHandler(cb_nav_main, pattern=r"^nav:main$"),
            CallbackQueryHandler(cb_nav_admin, pattern=r"^nav:admin$"),
            MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_text),
        ],
        per_chat=True,
        per_user=True,
        allow_reentry=True,
    )

    app.add_handler(conv_handler)
    app.add_error_handler(error_handler)

    logger.info("Bot polling started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
