import os
import logging
import json
import sqlite3
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from enum import Enum

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, InputFile, User
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler, ContextTypes
)
import requests

# ============ CONFIGURATION ============
TOKEN = os.environ.get("BOT_TOKEN")
API_BASE_URL = os.environ.get("API_BASE_URL", "https://faydaapi-production.up.railway.app/api/v1")
API_KEY = os.environ.get("API_KEY")
ADMIN_USERNAME = "dhtechs_admin"
CHANNEL_USERNAME = "faydatech"
ADMIN_IDS = [int(id) for id in os.environ.get("ADMIN_IDS", "").split(",") if id]
PAYMENT_PHONE = os.environ.get("PAYMENT_PHONE", "0919545335")

# Validate required config
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

# ============ HEALTH CHECK SERVER ============
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        elif self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'<h1>Fayda Rental Bot is running!</h1>')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress logs

def run_health_server():
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f"✅ Health check server running on port {port}")
    server.serve_forever()

# ============ DATABASE ============
DB_PATH = "fayda_bot.db"

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

class Database:
    def __init__(self):
        self._init_db()

    @contextmanager
    def get_cursor(self):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self.get_cursor() as cur:
            cur.execute("""
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
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS payment_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount INTEGER,
                    screenshot_path TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT,
                    processed_at TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS download_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    session_id TEXT,
                    timestamp TEXT,
                    status TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)

    def get_user(self, user_id: int) -> Optional[UserData]:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            if row:
                return UserData(**dict(row))
            return None

    def create_user(self, user: User, invited_by: Optional[int] = None) -> UserData:
        now = datetime.now().isoformat()
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id, username, first_name, last_name, 
                    created_at, updated_at, invited_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                user.id, user.username or "", user.first_name or "",
                user.last_name or "", now, now, invited_by
            ))
            if invited_by:
                cur.execute(
                    "UPDATE users SET pdf_balance = pdf_balance + ? WHERE user_id = ?",
                    (INVITE_BONUS, invited_by)
                )
            return self.get_user(user.id)

    def update_user(self, user_data: UserData):
        with self.get_cursor() as cur:
            cur.execute("""
                UPDATE users SET
                    username = ?, first_name = ?, last_name = ?,
                    balance = ?, pdf_balance = ?, total_downloads = ?,
                    is_admin = ?, is_banned = ?, trial_used = ?,
                    unlimited_expiry = ?, updated_at = ?, invite_count = ?
                WHERE user_id = ?
            """, (
                user_data.username, user_data.first_name, user_data.last_name,
                user_data.balance, user_data.pdf_balance, user_data.total_downloads,
                int(user_data.is_admin), int(user_data.is_banned),
                int(user_data.trial_used), user_data.unlimited_expiry,
                datetime.now().isoformat(), user_data.invite_count,
                user_data.user_id
            ))

    def get_all_users(self) -> List[UserData]:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM users ORDER BY created_at DESC")
            return [UserData(**dict(row)) for row in cur.fetchall()]

    def get_active_users(self) -> List[UserData]:
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT * FROM users 
                WHERE is_banned = 0 AND (balance > 0 OR pdf_balance > 0 OR 
                    unlimited_expiry > datetime('now'))
                ORDER BY created_at DESC
            """)
            return [UserData(**dict(row)) for row in cur.fetchall()]

    def create_payment_request(self, user_id: int, amount: int, screenshot_path: str) -> int:
        now = datetime.now().isoformat()
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO payment_requests (user_id, amount, screenshot_path, created_at)
                VALUES (?, ?, ?, ?)
            """, (user_id, amount, screenshot_path, now))
            return cur.lastrowid

    def get_pending_payments(self) -> List[PaymentRequest]:
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT * FROM payment_requests WHERE status = 'pending' 
                ORDER BY created_at ASC
            """)
            return [PaymentRequest(**dict(row)) for row in cur.fetchall()]

    def approve_payment(self, payment_id: int):
        with self.get_cursor() as cur:
            now = datetime.now().isoformat()
            cur.execute("""
                UPDATE payment_requests SET status = 'approved', processed_at = ?
                WHERE id = ?
            """, (now, payment_id))
            
            cur.execute("SELECT user_id, amount FROM payment_requests WHERE id = ?", (payment_id,))
            payment = cur.fetchone()
            if payment:
                user_id, amount = payment['user_id'], payment['amount']
                if amount >= UNLIMITED_MONTH:
                    expiry = (datetime.now() + timedelta(days=30)).isoformat()
                    cur.execute("""
                        UPDATE users SET balance = balance + ?, unlimited_expiry = ?
                        WHERE user_id = ?
                    """, (amount, expiry, user_id))
                elif amount >= UNLIMITED_WEEK:
                    expiry = (datetime.now() + timedelta(days=7)).isoformat()
                    cur.execute("""
                        UPDATE users SET balance = balance + ?, unlimited_expiry = ?
                        WHERE user_id = ?
                    """, (amount, expiry, user_id))
                else:
                    pdfs = amount // PRICE_PER_PDF
                    cur.execute("""
                        UPDATE users SET pdf_balance = pdf_balance + ?
                        WHERE user_id = ?
                    """, (pdfs, user_id))

    def reject_payment(self, payment_id: int):
        with self.get_cursor() as cur:
            now = datetime.now().isoformat()
            cur.execute("""
                UPDATE payment_requests SET status = 'rejected', processed_at = ?
                WHERE id = ?
            """, (now, payment_id))

    def log_download(self, user_id: int, session_id: str, status: str):
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO download_logs (user_id, session_id, timestamp, status)
                VALUES (?, ?, ?, ?)
            """, (user_id, session_id, datetime.now().isoformat(), status))
            cur.execute(
                "UPDATE users SET total_downloads = total_downloads + 1 WHERE user_id = ?",
                (user_id,)
            )

    def get_download_stats(self, user_id: int) -> Dict:
        with self.get_cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) as total, 
                       SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                FROM download_logs WHERE user_id = ?
            """, (user_id,))
            return dict(cur.fetchone())

    def get_total_stats(self) -> Dict:
        with self.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) as total_users FROM users")
            total_users = cur.fetchone()['total_users']
            cur.execute("SELECT COUNT(*) as active_users FROM users WHERE is_banned = 0")
            active_users = cur.fetchone()['active_users']
            cur.execute("SELECT SUM(total_downloads) as total_downloads FROM users")
            total_downloads = cur.fetchone()['total_downloads'] or 0
            cur.execute("SELECT COUNT(*) as pending_payments FROM payment_requests WHERE status = 'pending'")
            pending_payments = cur.fetchone()['pending_payments']
            return {
                "total_users": total_users,
                "active_users": active_users,
                "total_downloads": total_downloads,
                "pending_payments": pending_payments
            }

# ============ BOT HANDLERS ============
db = Database()
logger = logging.getLogger(__name__)

MAIN_MENU, PAYMENT_AMOUNT, WAITING_SCREENSHOT, FAN_INPUT, OTP_INPUT, ADMIN_MENU = range(6)

def is_admin(user_id: int) -> bool:
    user = db.get_user(user_id)
    return user and user.is_admin

def is_banned(user_id: int) -> bool:
    user = db.get_user(user_id)
    return user and user.is_banned

def get_user_credit(user: UserData) -> int:
    if user.unlimited_expiry:
        expiry = datetime.fromisoformat(user.unlimited_expiry)
        if expiry > datetime.now():
            return 999999
    return user.pdf_balance

def format_user_status(user: UserData) -> str:
    credit = get_user_credit(user)
    unlimited = ""
    if user.unlimited_expiry:
        expiry = datetime.fromisoformat(user.unlimited_expiry)
        if expiry > datetime.now():
            days_left = (expiry - datetime.now()).days
            unlimited = f"\n♾️ Unlimited until: {expiry.strftime('%Y-%m-%d')} ({days_left} days left)"
    return f"""
👤 *User Profile*
─────────────────
🆔 ID: `{user.user_id}`
📛 Name: {user.first_name} {user.last_name or ''}
👤 @{user.username or 'No username'}

💰 Balance: {user.balance} Birr
📄 PDF Credits: {credit} {unlimited}
📥 Total Downloads: {user.total_downloads}
👥 Invites: {user.invite_count}
─────────────────
"""

def get_main_keyboard(user_id: int = None) -> ReplyKeyboardMarkup:
    keyboard = [
        ["📄 Download PDF", "📊 My Status"],
        ["💰 Buy Credits", "🎁 Invite Friends"],
        ["📞 Support", "ℹ️ Help"]
    ]
    if user_id and is_admin(user_id):
        keyboard.append(["⚙️ Admin Panel"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        ["📊 Statistics", "💰 Pending Payments"],
        ["👥 Users", "🚫 Ban User"],
        ["✅ Approve Payment", "❌ Reject Payment"],
        ["📢 Broadcast", "📥 Download Logs"],
        ["🔙 Back to Main"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ===== START HANDLER =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    user_id = user.id
    
    existing = db.get_user(user_id)
    if not existing:
        invited_by = None
        if context.args:
            try:
                invited_by = int(context.args[0])
                if invited_by == user_id:
                    invited_by = None
            except ValueError:
                pass
        
        db.create_user(user, invited_by)
        user_data = db.get_user(user_id)
        
        await update.message.reply_text(
            f"🎉 *Welcome to Fayda PDF Download Bot!*\n\n"
            f"📌 *Please join our channel for updates:*\n"
            f"👉 [t.me/{CHANNEL_USERNAME}](https://t.me/{CHANNEL_USERNAME})\n\n"
            f"📖 *How it works:*\n"
            f"1️⃣ Get your FAN number (16 digits)\n"
            f"2️⃣ Use credits to download PDFs\n"
            f"3️⃣ Each download costs 5 Birr\n\n"
            f"🎁 You have {TRIAL_PDFS} free trial download!",
            reply_markup=get_main_keyboard(user_id),
            parse_mode="Markdown"
        )
        
        if ADMIN_IDS:
            mention = f"@{user.username}" if user.username else f"[{user.first_name}](tg://user?id={user_id})"
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        admin_id,
                        f"🆕 *New User Joined!*\n\n"
                        f"👤 {mention}\n"
                        f"🆔 `{user_id}`\n"
                        f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                        parse_mode="Markdown"
                    )
                except:
                    pass
    else:
        await update.message.reply_text(
            f"Welcome back, {user.first_name}! 👋\n"
            f"Use the buttons below to get started.",
            reply_markup=get_main_keyboard(user_id)
        )
    
    return MAIN_MENU

# ===== MENU HANDLER =====
async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    user_id = update.effective_user.id
    
    if is_banned(user_id):
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return MAIN_MENU
    
    if text == "📄 Download PDF":
        return await start_download(update, context)
    
    elif text == "📊 My Status":
        user = db.get_user(user_id)
        await update.message.reply_text(
            format_user_status(user),
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    elif text == "💰 Buy Credits":
        keyboard = [
            [f"💳 {MINIMUM_PURCHASE} Birr ({MINIMUM_PURCHASE//PRICE_PER_PDF} PDFs)"],
            [f"♾️ {UNLIMITED_WEEK} Birr (Unlimited 1 Week)"],
            [f"♾️ {UNLIMITED_MONTH} Birr (Unlimited 1 Month)"],
            ["🔙 Back"]
        ]
        await update.message.reply_text(
            f"💰 *Purchase Credits*\n\n"
            f"💳 {MINIMUM_PURCHASE} Birr = {MINIMUM_PURCHASE//PRICE_PER_PDF} PDF downloads\n"
            f"♾️ {UNLIMITED_WEEK} Birr = Unlimited downloads for 1 week\n"
            f"♾️ {UNLIMITED_MONTH} Birr = Unlimited downloads for 1 month\n\n"
            f"📱 Send payment to *{PAYMENT_PHONE}* via TeleBirr\n"
            f"Then send the payment screenshot.",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
        return PAYMENT_AMOUNT
    
    elif text == "🎁 Invite Friends":
        user = db.get_user(user_id)
        keyboard = [
            [f"📤 Share Invite Link"],
            [f"👥 My Invites: {user.invite_count}"],
            ["🔙 Back"]
        ]
        await update.message.reply_text(
            f"🎁 *Invite Friends & Earn!*\n\n"
            f"For each friend who joins using your link, you get *{INVITE_BONUS} free PDF downloads*!\n\n"
            f"Your invite link:\n"
            f"`https://t.me/{context.bot.username}?start={user_id}`\n\n"
            f"Share this link with your friends!",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
        return MAIN_MENU
    
    elif text == "📤 Share Invite Link":
        user_id = update.effective_user.id
        await update.message.reply_text(
            f"📤 *Share this link:*\n\n"
            f"`https://t.me/{context.bot.username}?start={user_id}`\n\n"
            f"👥 Friends who join get a free trial!\n"
            f"🎁 You get {INVITE_BONUS} free PDFs per invite!",
            parse_mode="Markdown"
        )
        return MAIN_MENU
    
    elif text == "📞 Support":
        await update.message.reply_text(
            f"📞 *Support*\n\n"
            f"Contact our support team:\n"
            f"👤 @{ADMIN_USERNAME}\n"
            f"📱 {PAYMENT_PHONE}\n\n"
            f"For quick help, check:\n"
            f"📌 [t.me/{CHANNEL_USERNAME}](https://t.me/{CHANNEL_USERNAME})",
            parse_mode="Markdown"
        )
        return MAIN_MENU
    
    elif text == "ℹ️ Help":
        await update.message.reply_text(
            f"ℹ️ *Help Guide*\n\n"
            f"📄 *How to Download:*\n"
            f"1. Enter your 16-digit FAN number\n"
            f"2. Enter the OTP received\n"
            f"3. PDF will be downloaded\n\n"
            f"💰 *Pricing:*\n"
            f"• 5 Birr per PDF\n"
            f"• Minimum: {MINIMUM_PURCHASE} Birr ({MINIMUM_PURCHASE//PRICE_PER_PDF} PDFs)\n"
            f"• Unlimited: {UNLIMITED_WEEK} Birr/week or {UNLIMITED_MONTH} Birr/month\n\n"
            f"🎁 *Bonuses:*\n"
            f"• Free trial: {TRIAL_PDFS} PDF\n"
            f"• Referral: {INVITE_BONUS} PDFs per invite\n\n"
            f"📱 *Payment:*\n"
            f"Send to TeleBirr: {PAYMENT_PHONE}",
            parse_mode="Markdown"
        )
        return MAIN_MENU
    
    elif text == "⚙️ Admin Panel" and is_admin(user_id):
        return await show_admin_panel(update, context)
    
    elif text in ["🔙 Back", "🔙 Back to Main"]:
        await update.message.reply_text(
            "Main menu:",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    else:
        await update.message.reply_text(
            "Please use the buttons below.",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU

# ===== DOWNLOAD HANDLER =====
async def start_download(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    credit = get_user_credit(user)
    if credit <= 0 and not user.trial_used:
        user.pdf_balance += TRIAL_PDFS
        user.trial_used = True
        db.update_user(user)
        await update.message.reply_text(
            f"🎁 *Free Trial Activated!*\n"
            f"You have {TRIAL_PDFS} free PDF download(s)!\n\n"
            f"Enter your 16-digit FAN number:",
            parse_mode="Markdown"
        )
        context.user_data['awaiting_fan'] = True
        return FAN_INPUT
    
    elif credit <= 0:
        await update.message.reply_text(
            f"⚠️ *Insufficient Balance!*\n\n"
            f"You have {credit} PDF credits available.\n"
            f"Each download costs 5 Birr.\n\n"
            f"💰 Buy more credits using the 'Buy Credits' button.",
            parse_mode="Markdown"
        )
        return MAIN_MENU
    
    await update.message.reply_text(
        f"📄 *Enter your 16-digit FAN number:*\n\n"
        f"Available PDFs: {credit}\n"
        f"(Type /cancel to cancel)",
        parse_mode="Markdown"
    )
    context.user_data['awaiting_fan'] = True
    return FAN_INPUT

async def handle_fan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    user_id = update.effective_user.id
    
    if text == "/cancel":
        await update.message.reply_text(
            "Cancelled. Use /start to return to menu.",
            reply_markup=get_main_keyboard(user_id)
        )
        context.user_data.pop('awaiting_fan', None)
        return MAIN_MENU
    
    if not text.isdigit() or len(text) != 16:
        await update.message.reply_text(
            "❌ Invalid FAN number. Please enter exactly 16 digits:"
        )
        return FAN_INPUT
    
    context.user_data['fan'] = text
    await update.message.reply_text(
        f"📤 Sending OTP to FAN: `{text[:4]}****{text[-4:]}`\n"
        f"Please wait...",
        parse_mode="Markdown"
    )
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/session/send-otp",
            json={"fan": text, "server": "server3"},
            headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
            timeout=30
        )
        data = response.json()
        
        if data.get("success"):
            session_id = data.get("data", {}).get("sessionId")
            if session_id:
                context.user_data['session_id'] = session_id
                await update.message.reply_text(
                    f"✅ OTP sent successfully!\n\n"
                    f"📱 Enter the OTP code you received:\n"
                    f"(Type /cancel to cancel)",
                    reply_markup=None
                )
                return OTP_INPUT
            else:
                await update.message.reply_text(
                    f"❌ Failed to get session ID. Please try again.\n"
                    f"Response: {data}",
                    reply_markup=get_main_keyboard(user_id)
                )
                context.user_data.pop('awaiting_fan', None)
                return MAIN_MENU
        else:
            error_msg = data.get("message", "Unknown error")
            await update.message.reply_text(
                f"❌ Failed to send OTP: {error_msg}\n\n"
                f"Please try again or contact support.",
                reply_markup=get_main_keyboard(user_id)
            )
            context.user_data.pop('awaiting_fan', None)
            return MAIN_MENU
    
    except Exception as e:
        await update.message.reply_text(
            f"❌ Error: {str(e)}\n"
            f"Please try again later.",
            reply_markup=get_main_keyboard(user_id)
        )
        context.user_data.pop('awaiting_fan', None)
        return MAIN_MENU

async def handle_otp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    if text == "/cancel":
        await update.message.reply_text(
            "Cancelled. Use /start to return.",
            reply_markup=get_main_keyboard(user_id)
        )
        context.user_data.pop('session_id', None)
        context.user_data.pop('fan', None)
        return MAIN_MENU
    
    session_id = context.user_data.get('session_id')
    if not session_id:
        await update.message.reply_text(
            "❌ Session expired. Please start again.",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    await update.message.reply_text("⏳ Verifying OTP and downloading PDF...")
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/session/verify-otp",
            json={
                "sessionId": session_id,
                "otp": text,
                "responseMode": "pdf",
                "includeScreenshots": False
            },
            headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
            timeout=60
        )
        
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '')
            
            if 'application/pdf' in content_type:
                if get_user_credit(user) > 0:
                    user.pdf_balance -= 1
                    db.update_user(user)
                
                db.log_download(user_id, session_id, "success")
                
                await update.message.reply_document(
                    document=InputFile(response.content, filename=f"fayda_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"),
                    caption=f"✅ PDF downloaded successfully!\n\n"
                            f"📄 PDFs remaining: {get_user_credit(user)}\n"
                            f"📥 Total downloads: {user.total_downloads + 1}\n\n"
                            f"Use /start to download more.",
                    reply_markup=get_main_keyboard(user_id)
                )
                
                context.user_data.pop('session_id', None)
                context.user_data.pop('fan', None)
                return MAIN_MENU
            else:
                try:
                    error_data = response.json()
                    error_msg = error_data.get("message", "Unknown error")
                except:
                    error_msg = "Invalid response from server"
                
                db.log_download(user_id, session_id, "failed")
                await update.message.reply_text(
                    f"❌ Download failed: {error_msg}\n\n"
                    f"Please try again or contact support.",
                    reply_markup=get_main_keyboard(user_id)
                )
                return MAIN_MENU
        else:
            await update.message.reply_text(
                f"❌ Server error: {response.status_code}\n"
                f"Please try again later.",
                reply_markup=get_main_keyboard(user_id)
            )
            return MAIN_MENU
    
    except Exception as e:
        await update.message.reply_text(
            f"❌ Error: {str(e)}\n"
            f"Please try again.",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU

# ===== PAYMENT HANDLER =====
async def handle_payment_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    user_id = update.effective_user.id
    
    if text == "🔙 Back":
        await update.message.reply_text(
            "Main menu:",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    try:
        if "Birr" in text:
            amount = int(text.split()[0])
        else:
            amount = int(text)
        
        if amount < MINIMUM_PURCHASE and amount not in [UNLIMITED_WEEK, UNLIMITED_MONTH]:
            await update.message.reply_text(
                f"❌ Minimum purchase is {MINIMUM_PURCHASE} Birr.\n"
                f"Please choose from the options below.",
                reply_markup=ReplyKeyboardMarkup([
                    [f"💳 {MINIMUM_PURCHASE} Birr ({MINIMUM_PURCHASE//PRICE_PER_PDF} PDFs)"],
                    [f"♾️ {UNLIMITED_WEEK} Birr (Unlimited 1 Week)"],
                    [f"♾️ {UNLIMITED_MONTH} Birr (Unlimited 1 Month)"],
                    ["🔙 Back"]
                ], resize_keyboard=True)
            )
            return PAYMENT_AMOUNT
        
        context.user_data['payment_amount'] = amount
        
        await update.message.reply_text(
            f"💰 *Payment Request*\n\n"
            f"Amount: *{amount} Birr*\n\n"
            f"📱 Send payment to:\n"
            f"*{PAYMENT_PHONE}* via TeleBirr\n\n"
            f"📸 After sending, *send the payment screenshot* here.\n\n"
            f"Type /cancel to cancel.",
            parse_mode="Markdown"
        )
        return WAITING_SCREENSHOT
    
    except ValueError:
        await update.message.reply_text(
            "❌ Please enter a valid amount.",
            reply_markup=ReplyKeyboardMarkup([
                [f"💳 {MINIMUM_PURCHASE} Birr ({MINIMUM_PURCHASE//PRICE_PER_PDF} PDFs)"],
                [f"♾️ {UNLIMITED_WEEK} Birr (Unlimited 1 Week)"],
                [f"♾️ {UNLIMITED_MONTH} Birr (Unlimited 1 Month)"],
                ["🔙 Back"]
            ], resize_keyboard=True)
        )
        return PAYMENT_AMOUNT

async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    
    if update.message.text == "/cancel":
        await update.message.reply_text(
            "Payment cancelled.",
            reply_markup=get_main_keyboard(user_id)
        )
        context.user_data.pop('payment_amount', None)
        return MAIN_MENU
    
    if not update.message.photo:
        await update.message.reply_text(
            "📸 Please send a payment screenshot photo.\n"
            "Type /cancel to cancel."
        )
        return WAITING_SCREENSHOT
    
    amount = context.user_data.get('payment_amount')
    if not amount:
        await update.message.reply_text(
            "❌ Payment session expired. Please start again.",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    photo = update.message.photo[-1]
    file = await photo.get_file()
    
    os.makedirs("screenshots", exist_ok=True)
    file_path = f"screenshots/payment_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    await file.download_to_drive(file_path)
    
    db.create_payment_request(user_id, amount, file_path)
    
    await update.message.reply_text(
        f"✅ *Payment request received!*\n\n"
        f"💰 Amount: {amount} Birr\n"
        f"📱 Please wait for admin approval.\n\n"
        f"You will be notified once approved.",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(user_id)
    )
    
    if ADMIN_IDS:
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_photo(
                    admin_id,
                    photo=InputFile(file_path),
                    caption=(
                        f"💰 *New Payment Request*\n\n"
                        f"👤 User: @{update.effective_user.username or 'N/A'}\n"
                        f"🆔 `{user_id}`\n"
                        f"💳 Amount: {amount} Birr\n"
                        f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
                        f"Use Admin Panel to approve/reject."
                    ),
                    parse_mode="Markdown"
                )
            except:
                pass
    
    context.user_data.pop('payment_amount', None)
    return MAIN_MENU

# ===== ADMIN HANDLERS =====
async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Access denied.")
        return MAIN_MENU
    
    stats = db.get_total_stats()
    await update.message.reply_text(
        f"⚙️ *Admin Panel*\n\n"
        f"📊 *Statistics:*\n"
        f"👥 Total Users: {stats['total_users']}\n"
        f"🟢 Active Users: {stats['active_users']}\n"
        f"📥 Total Downloads: {stats['total_downloads']}\n"
        f"💰 Pending Payments: {stats['pending_payments']}\n\n"
        f"Use the buttons below to manage.",
        parse_mode="Markdown",
        reply_markup=get_admin_keyboard()
    )
    return ADMIN_MENU

async def handle_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Access denied.")
        return MAIN_MENU
    
    if text == "📊 Statistics":
        stats = db.get_total_stats()
        users = db.get_all_users()
        total_pdfs = sum(get_user_credit(u) for u in users)
        
        await update.message.reply_text(
            f"📊 *Full Statistics*\n\n"
            f"👥 Total Users: {stats['total_users']}\n"
            f"🟢 Active Users: {stats['active_users']}\n"
            f"🚫 Banned Users: {stats['total_users'] - stats['active_users']}\n"
            f"📥 Total Downloads: {stats['total_downloads']}\n"
            f"📄 Total PDF Credits: {total_pdfs}\n"
            f"💰 Pending Payments: {stats['pending_payments']}\n\n"
            f"📅 Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )
        return ADMIN_MENU
    
    elif text == "💰 Pending Payments":
        payments = db.get_pending_payments()
        if not payments:
            await update.message.reply_text(
                "✅ No pending payments.",
                reply_markup=get_admin_keyboard()
            )
            return ADMIN_MENU
        
        msg = "💰 *Pending Payments*\n\n"
        for p in payments[:10]:
            user = db.get_user(p.user_id)
            name = f"@{user.username}" if user and user.username else f"ID: {p.user_id}"
            msg += f"📌 ID: `{p.id}`\n"
            msg += f"👤 {name}\n"
            msg += f"💳 {p.amount} Birr\n"
            msg += f"📅 {p.created_at}\n"
            msg += "─────────────────\n"
        
        if len(payments) > 10:
            msg += f"\n... and {len(payments) - 10} more"
        
        await update.message.reply_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )
        return ADMIN_MENU
    
    elif text == "✅ Approve Payment":
        await update.message.reply_text(
            "✏️ Enter the payment ID to approve:\n"
            "(Type /cancel to cancel)"
        )
        context.user_data['admin_action'] = 'approve'
        return ADMIN_MENU
    
    elif text == "❌ Reject Payment":
        await update.message.reply_text(
            "✏️ Enter the payment ID to reject:\n"
            "(Type /cancel to cancel)"
        )
        context.user_data['admin_action'] = 'reject'
        return ADMIN_MENU
    
    elif text == "👥 Users":
        users = db.get_all_users()
        msg = "👥 *All Users*\n\n"
        for u in users[:20]:
            status = "🚫" if u.is_banned else "🟢"
            unlimited = "♾️" if u.unlimited_expiry and datetime.fromisoformat(u.unlimited_expiry) > datetime.now() else ""
            msg += f"{status} `{u.user_id}` | @{u.username or 'N/A'} | 📄{get_user_credit(u)} {unlimited}\n"
        
        if len(users) > 20:
            msg += f"\n... and {len(users) - 20} more"
        
        await update.message.reply_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_admin_keyboard()
        )
        return ADMIN_MENU
    
    elif text == "🚫 Ban User":
        await update.message.reply_text(
            "✏️ Enter the user ID to ban:\n"
            "(Type /cancel to cancel)"
        )
        context.user_data['admin_action'] = 'ban'
        return ADMIN_MENU
    
    elif text == "📢 Broadcast":
        await update.message.reply_text(
            "✏️ Enter the message to broadcast to all users:\n"
            "(Type /cancel to cancel)"
        )
        context.user_data['admin_action'] = 'broadcast'
        return ADMIN_MENU
    
    elif text == "📥 Download Logs":
        await update.message.reply_text(
            "📥 Download logs are being generated...",
            reply_markup=get_admin_keyboard()
        )
        
        users = db.get_all_users()
        logs = "📥 DOWNLOAD LOGS\n"
        logs += f"Generated: {datetime.now().isoformat()}\n"
        logs += "="*50 + "\n\n"
        
        for user in users:
            stats = db.get_download_stats(user.user_id)
            logs += f"👤 @{user.username or user.first_name or str(user.user_id)}\n"
            logs += f"   📥 Total: {stats['total']} | ✅ Success: {stats['successful']} | ❌ Failed: {stats['failed']}\n"
            logs += f"   📄 Credits: {get_user_credit(user)}\n\n"
        
        os.makedirs("logs", exist_ok=True)
        log_path = f"logs/downloads_{datetime.now().strftime('%Y%m%d')}.txt"
        with open(log_path, "w") as f:
            f.write(logs)
        
        await update.message.reply_document(
            document=InputFile(log_path, filename=f"download_logs_{datetime.now().strftime('%Y%m%d')}.txt"),
            caption="📥 Download logs",
            reply_markup=get_admin_keyboard()
        )
        return ADMIN_MENU
    
    elif text == "🔙 Back to Main":
        await update.message.reply_text(
            "Main menu:",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    else:
        if context.user_data.get('admin_action'):
            action = context.user_data['admin_action']
            input_text = text
            
            if action == 'approve':
                try:
                    payment_id = int(input_text)
                    db.approve_payment(payment_id)
                    await update.message.reply_text(
                        f"✅ Payment {payment_id} approved successfully!",
                        reply_markup=get_admin_keyboard()
                    )
                    payment = db.get_pending_payments()
                    for p in payment:
                        if p.id == payment_id:
                            try:
                                await context.bot.send_message(
                                    p.user_id,
                                    f"✅ *Payment Approved!*\n\n"
                                    f"💰 Amount: {p.amount} Birr\n"
                                    f"📄 Credits added to your account!\n\n"
                                    f"Use /start to download PDFs.",
                                    parse_mode="Markdown"
                                )
                            except:
                                pass
                            break
                except ValueError:
                    await update.message.reply_text(
                        "❌ Invalid payment ID. Please enter a number.",
                        reply_markup=get_admin_keyboard()
                    )
                context.user_data.pop('admin_action', None)
                return ADMIN_MENU
            
            elif action == 'reject':
                try:
                    payment_id = int(input_text)
                    db.reject_payment(payment_id)
                    await update.message.reply_text(
                        f"❌ Payment {payment_id} rejected.",
                        reply_markup=get_admin_keyboard()
                    )
                    payment = db.get_pending_payments()
                    for p in payment:
                        if p.id == payment_id:
                            try:
                                await context.bot.send_message(
                                    p.user_id,
                                    f"❌ *Payment Rejected*\n\n"
                                    f"💰 Amount: {p.amount} Birr\n"
                                    f"Please contact support for assistance.",
                                    parse_mode="Markdown"
                                )
                            except:
                                pass
                            break
                except ValueError:
                    await update.message.reply_text(
                        "❌ Invalid payment ID. Please enter a number.",
                        reply_markup=get_admin_keyboard()
                    )
                context.user_data.pop('admin_action', None)
                return ADMIN_MENU
            
            elif action == 'ban':
                try:
                    target_id = int(input_text)
                    if target_id == user_id:
                        await update.message.reply_text(
                            "❌ You cannot ban yourself!",
                            reply_markup=get_admin_keyboard()
                        )
                    else:
                        user = db.get_user(target_id)
                        if user:
                            user.is_banned = True
                            db.update_user(user)
                            await update.message.reply_text(
                                f"🚫 User {target_id} banned successfully!",
                                reply_markup=get_admin_keyboard()
                            )
                            try:
                                await context.bot.send_message(
                                    target_id,
                                    f"🚫 You have been banned from using this bot."
                                )
                            except:
                                pass
                        else:
                            await update.message.reply_text(
                                f"❌ User {target_id} not found.",
                                reply_markup=get_admin_keyboard()
                            )
                except ValueError:
                    await update.message.reply_text(
                        "❌ Invalid user ID. Please enter a number.",
                        reply_markup=get_admin_keyboard()
                    )
                context.user_data.pop('admin_action', None)
                return ADMIN_MENU
            
            elif action == 'broadcast':
                users = db.get_all_users()
                success = 0
                failed = 0
                
                await update.message.reply_text(
                    f"📢 Broadcasting message to {len(users)} users...\n"
                    f"This may take a while.",
                    reply_markup=get_admin_keyboard()
                )
                
                for user in users:
                    if user.is_banned:
                        continue
                    try:
                        await context.bot.send_message(
                            user.user_id,
                            f"📢 *Announcement*\n\n{input_text}",
                            parse_mode="Markdown"
                        )
                        success += 1
                    except:
                        failed += 1
                    await asyncio.sleep(0.1)
                
                await update.message.reply_text(
                    f"📢 Broadcast complete!\n"
                    f"✅ Sent: {success}\n"
                    f"❌ Failed: {failed}",
                    reply_markup=get_admin_keyboard()
                )
                context.user_data.pop('admin_action', None)
                return ADMIN_MENU
        
        await update.message.reply_text(
            "Please use the admin buttons.",
            reply_markup=get_admin_keyboard()
        )
        return ADMIN_MENU

# ===== CANCEL =====
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    context.user_data.clear()
    await update.message.reply_text(
        "Operation cancelled.",
        reply_markup=get_main_keyboard(user_id)
    )
    return MAIN_MENU

# ===== MAIN =====
def main():
    # Setup logging
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    
    print("🤖 Starting Fayda Rental Bot...")
    print(f"📱 Bot Token: {TOKEN[:10]}...")
    print(f"🌐 API URL: {API_BASE_URL}")
    print(f"🔑 API Key: {API_KEY[:10]}...")
    print(f"👑 Admin IDs: {ADMIN_IDS}")
    
    # Start health check server in background
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    # Build application
    app = Application.builder().token(TOKEN).build()
    
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu)
            ],
            PAYMENT_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_payment_amount)
            ],
            WAITING_SCREENSHOT: [
                MessageHandler(filters.PHOTO, handle_screenshot),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_screenshot)
            ],
            FAN_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_fan)
            ],
            OTP_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_otp)
            ],
            ADMIN_MENU: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_menu)
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        per_chat=False,
        per_user=True,
        allow_reentry=True
    )
    
    app.add_handler(conv_handler)
    app.add_handler(MessageHandler(filters.Regex(r'^⚙️ Admin Panel$'), show_admin_panel))
    
    print(f"🤖 Bot started! Username: @{app.bot.username}")
    print("✅ Health check available at: /health")
    
    # Start polling
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()