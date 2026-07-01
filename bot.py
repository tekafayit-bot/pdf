import os
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from contextlib import contextmanager
from dataclasses import dataclass

from telegram import Update, ReplyKeyboardMarkup, InputFile
from telegram.ext import (
    Application, CommandHandler, MessageHandler, 
    filters, ConversationHandler, ContextTypes
)
import requests

# ============ CONFIGURATION ============
TOKEN = os.environ.get("BOT_TOKEN")
API_BASE_URL = os.environ.get("API_BASE_URL", "https://faydaapi-production.up.railway.app/api/v1")
API_KEY = os.environ.get("API_KEY")
ADMIN_IDS = [int(id) for id in os.environ.get("ADMIN_IDS", "").split(",") if id]
PAYMENT_PHONE = os.environ.get("PAYMENT_PHONE", "0919545335")

if not TOKEN or not API_KEY:
    raise ValueError("BOT_TOKEN and API_KEY are required!")

# Pricing
PRICE_PER_PDF = 5
MINIMUM_PURCHASE = 150
UNLIMITED_WEEK = 1000
UNLIMITED_MONTH = 5000
TRIAL_PDFS = 1
INVITE_BONUS = 2

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
    invited_by: Optional[int] = None
    invite_count: int = 0

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
                    processed_at TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS download_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    session_id TEXT,
                    timestamp TEXT,
                    status TEXT
                )
            """)
    
    def get_user(self, user_id: int) -> Optional[UserData]:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            return UserData(**dict(row)) if row else None
    
    def create_user(self, user, invited_by: Optional[int] = None) -> UserData:
        now = datetime.now().isoformat()
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id, username, first_name, last_name, created_at, invited_by)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user.id, user.username or "", user.first_name or "", user.last_name or "", now, invited_by))
            if invited_by:
                cur.execute("UPDATE users SET pdf_balance = pdf_balance + ? WHERE user_id = ?", (INVITE_BONUS, invited_by))
            return self.get_user(user.id)
    
    def update_user(self, user_data: UserData):
        with self.get_cursor() as cur:
            cur.execute("""
                UPDATE users SET username=?, first_name=?, last_name=?, balance=?, pdf_balance=?,
                total_downloads=?, is_admin=?, is_banned=?, trial_used=?, unlimited_expiry=?, invite_count=?
                WHERE user_id=?
            """, (user_data.username, user_data.first_name, user_data.last_name, user_data.balance,
                  user_data.pdf_balance, user_data.total_downloads, int(user_data.is_admin),
                  int(user_data.is_banned), int(user_data.trial_used), user_data.unlimited_expiry,
                  user_data.invite_count, user_data.user_id))
    
    def get_all_users(self) -> List[UserData]:
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM users ORDER BY created_at DESC")
            return [UserData(**dict(row)) for row in cur.fetchall()]
    
    def get_pending_payments(self):
        with self.get_cursor() as cur:
            cur.execute("SELECT * FROM payment_requests WHERE status='pending' ORDER BY created_at ASC")
            return [dict(row) for row in cur.fetchall()]
    
    def create_payment_request(self, user_id: int, amount: int, screenshot_path: str) -> int:
        with self.get_cursor() as cur:
            cur.execute("""
                INSERT INTO payment_requests (user_id, amount, screenshot_path, created_at)
                VALUES (?, ?, ?, ?)
            """, (user_id, amount, screenshot_path, datetime.now().isoformat()))
            return cur.lastrowid
    
    def approve_payment(self, payment_id: int):
        with self.get_cursor() as cur:
            cur.execute("UPDATE payment_requests SET status='approved', processed_at=? WHERE id=?", 
                       (datetime.now().isoformat(), payment_id))
            cur.execute("SELECT user_id, amount FROM payment_requests WHERE id=?", (payment_id,))
            payment = cur.fetchone()
            if payment:
                user_id, amount = payment['user_id'], payment['amount']
                if amount >= UNLIMITED_MONTH:
                    cur.execute("UPDATE users SET balance=balance+?, unlimited_expiry=? WHERE user_id=?", 
                               (amount, (datetime.now() + timedelta(days=30)).isoformat(), user_id))
                elif amount >= UNLIMITED_WEEK:
                    cur.execute("UPDATE users SET balance=balance+?, unlimited_expiry=? WHERE user_id=?",
                               (amount, (datetime.now() + timedelta(days=7)).isoformat(), user_id))
                else:
                    cur.execute("UPDATE users SET pdf_balance=pdf_balance+? WHERE user_id=?", 
                               (amount // PRICE_PER_PDF, user_id))
    
    def reject_payment(self, payment_id: int):
        with self.get_cursor() as cur:
            cur.execute("UPDATE payment_requests SET status='rejected', processed_at=? WHERE id=?", 
                       (datetime.now().isoformat(), payment_id))
    
    def log_download(self, user_id: int, session_id: str, status: str):
        with self.get_cursor() as cur:
            cur.execute("INSERT INTO download_logs (user_id, session_id, timestamp, status) VALUES (?, ?, ?, ?)",
                       (user_id, session_id, datetime.now().isoformat(), status))
            cur.execute("UPDATE users SET total_downloads=total_downloads+1 WHERE user_id=?", (user_id,))
    
    def get_total_stats(self) -> Dict:
        with self.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) as total_users FROM users")
            total_users = cur.fetchone()['total_users']
            cur.execute("SELECT COUNT(*) as active_users FROM users WHERE is_banned=0")
            active_users = cur.fetchone()['active_users']
            cur.execute("SELECT SUM(total_downloads) as total_downloads FROM users")
            total_downloads = cur.fetchone()['total_downloads'] or 0
            cur.execute("SELECT COUNT(*) as pending_payments FROM payment_requests WHERE status='pending'")
            pending_payments = cur.fetchone()['pending_payments']
            return {"total_users": total_users, "active_users": active_users, 
                    "total_downloads": total_downloads, "pending_payments": pending_payments}

db = Database()

# ============ BOT HANDLERS ============
MAIN_MENU, PAYMENT_AMOUNT, WAITING_SCREENSHOT, FAN_INPUT, OTP_INPUT, ADMIN_MENU = range(6)

def is_admin(user_id: int) -> bool:
    user = db.get_user(user_id)
    return user and user.is_admin

def is_banned(user_id: int) -> bool:
    user = db.get_user(user_id)
    return user and user.is_banned

def get_user_credit(user: UserData) -> int:
    if user.unlimited_expiry and datetime.fromisoformat(user.unlimited_expiry) > datetime.now():
        return 999999
    return user.pdf_balance

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    existing = db.get_user(user.id)
    if not existing:
        invited_by = int(context.args[0]) if context.args and context.args[0].isdigit() else None
        if invited_by == user.id:
            invited_by = None
        db.create_user(user, invited_by)
        await update.message.reply_text(
            f"🎉 Welcome to Fayda PDF Bot!\n\n"
            f"📌 Join our channel: t.me/faydatech\n\n"
            f"🎁 Free trial: {TRIAL_PDFS} PDF\n"
            f"💰 5 Birr per PDF\n"
            f"💳 Minimum: {MINIMUM_PURCHASE} Birr",
            reply_markup=get_main_keyboard(user.id)
        )
    else:
        await update.message.reply_text("Welcome back! 👋", reply_markup=get_main_keyboard(user.id))
    return MAIN_MENU

async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    user_id = update.effective_user.id
    
    if is_banned(user_id):
        await update.message.reply_text("🚫 You are banned.")
        return MAIN_MENU
    
    if text == "📄 Download PDF":
        user = db.get_user(user_id)
        credit = get_user_credit(user)
        if credit <= 0 and not user.trial_used:
            user.pdf_balance += TRIAL_PDFS
            user.trial_used = True
            db.update_user(user)
            await update.message.reply_text(f"🎁 Free trial activated! {TRIAL_PDFS} PDF.\n\nEnter 16-digit FAN:")
            context.user_data['awaiting_fan'] = True
            return FAN_INPUT
        elif credit <= 0:
            await update.message.reply_text("⚠️ Insufficient balance. Use 'Buy Credits' to purchase more.")
            return MAIN_MENU
        await update.message.reply_text(f"📄 Enter 16-digit FAN (Available: {credit} PDFs):")
        context.user_data['awaiting_fan'] = True
        return FAN_INPUT
    
    elif text == "📊 My Status":
        user = db.get_user(user_id)
        credit = get_user_credit(user)
        unlimited = ""
        if user.unlimited_expiry:
            expiry = datetime.fromisoformat(user.unlimited_expiry)
            if expiry > datetime.now():
                days_left = (expiry - datetime.now()).days
                unlimited = f"\n♾️ Unlimited until: {expiry.strftime('%Y-%m-%d')} ({days_left} days left)"
        await update.message.reply_text(
            f"👤 {user.first_name} {user.last_name or ''}\n"
            f"📄 PDF Credits: {credit}{unlimited}\n"
            f"📥 Downloads: {user.total_downloads}\n"
            f"👥 Invites: {user.invite_count}",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    elif text == "💰 Buy Credits":
        keyboard = [
            [f"💳 {MINIMUM_PURCHASE} Birr ({MINIMUM_PURCHASE//PRICE_PER_PDF} PDFs)"],
            [f"♾️ {UNLIMITED_WEEK} Birr (1 Week Unlimited)"],
            [f"♾️ {UNLIMITED_MONTH} Birr (1 Month Unlimited)"],
            ["🔙 Back"]
        ]
        await update.message.reply_text(
            f"💰 Purchase Credits\n\n"
            f"💳 {MINIMUM_PURCHASE} Birr = {MINIMUM_PURCHASE//PRICE_PER_PDF} PDFs\n"
            f"♾️ {UNLIMITED_WEEK} Birr = Unlimited 1 Week\n"
            f"♾️ {UNLIMITED_MONTH} Birr = Unlimited 1 Month\n\n"
            f"📱 Send payment to: {PAYMENT_PHONE} (TeleBirr)\n"
            f"Then send the screenshot.",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return PAYMENT_AMOUNT
    
    elif text == "🎁 Invite Friends":
        user = db.get_user(user_id)
        await update.message.reply_text(
            f"🎁 Invite Friends & Earn!\n\n"
            f"Get {INVITE_BONUS} free PDFs per invite!\n\n"
            f"🔗 Your invite link:\n"
            f"https://t.me/{context.bot.username}?start={user_id}\n\n"
            f"👥 Invites: {user.invite_count}",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    elif text == "📞 Support":
        await update.message.reply_text(
            f"📞 Support\n\n"
            f"Contact: @dhtechs_admin\n"
            f"Phone: {PAYMENT_PHONE}\n"
            f"Channel: t.me/faydatech",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    elif text == "ℹ️ Help":
        await update.message.reply_text(
            f"ℹ️ Help Guide\n\n"
            f"📄 Download PDF:\n"
            f"1. Enter 16-digit FAN\n"
            f"2. Enter OTP\n"
            f"3. PDF downloads\n\n"
            f"💰 Pricing:\n"
            f"• 5 Birr/PDF\n"
            f"• Min: {MINIMUM_PURCHASE} Birr\n"
            f"• Unlimited: {UNLIMITED_WEEK}/week, {UNLIMITED_MONTH}/month\n\n"
            f"🎁 Free trial: {TRIAL_PDFS} PDF\n"
            f"🎁 Referral: {INVITE_BONUS} PDFs/invite",
            reply_markup=get_main_keyboard(user_id)
        )
        return MAIN_MENU
    
    elif text == "⚙️ Admin Panel" and is_admin(user_id):
        stats = db.get_total_stats()
        await update.message.reply_text(
            f"⚙️ Admin Panel\n\n"
            f"👥 Total Users: {stats['total_users']}\n"
            f"🟢 Active: {stats['active_users']}\n"
            f"📥 Downloads: {stats['total_downloads']}\n"
            f"💰 Pending: {stats['pending_payments']}",
            reply_markup=get_admin_keyboard()
        )
        return ADMIN_MENU
    
    elif text in ["🔙 Back", "🔙 Back to Main"]:
        await update.message.reply_text("Main menu:", reply_markup=get_main_keyboard(user_id))
        return MAIN_MENU
    
    else:
        await update.message.reply_text("Please use the buttons below.", reply_markup=get_main_keyboard(user_id))
        return MAIN_MENU

async def handle_fan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    user_id = update.effective_user.id
    
    if text == "/cancel":
        await update.message.reply_text("Cancelled.", reply_markup=get_main_keyboard(user_id))
        context.user_data.pop('awaiting_fan', None)
        return MAIN_MENU
    
    if not text.isdigit() or len(text) != 16:
        await update.message.reply_text("❌ Invalid FAN. Enter exactly 16 digits:")
        return FAN_INPUT
    
    context.user_data['fan'] = text
    await update.message.reply_text(f"📤 Sending OTP to {text[:4]}****{text[-4:]}...")
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/session/send-otp",
            json={"fan": text, "server": "server3"},
            headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
            timeout=30
        )
        data = response.json()
        
        if data.get("success") and data.get("data", {}).get("sessionId"):
            context.user_data['session_id'] = data['data']['sessionId']
            await update.message.reply_text("✅ OTP sent! Enter the OTP code:")
            return OTP_INPUT
        else:
            await update.message.reply_text(f"❌ Failed: {data.get('message', 'Unknown error')}")
            return MAIN_MENU
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        return MAIN_MENU

async def handle_otp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    user_id = update.effective_user.id
    user = db.get_user(user_id)
    
    if text == "/cancel":
        await update.message.reply_text("Cancelled.", reply_markup=get_main_keyboard(user_id))
        context.user_data.pop('session_id', None)
        context.user_data.pop('fan', None)
        return MAIN_MENU
    
    session_id = context.user_data.get('session_id')
    if not session_id:
        await update.message.reply_text("❌ Session expired. Start again.")
        return MAIN_MENU
    
    await update.message.reply_text("⏳ Verifying OTP and downloading PDF...")
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/session/verify-otp",
            json={"sessionId": session_id, "otp": text, "responseMode": "pdf", "includeScreenshots": False},
            headers={"x-api-key": API_KEY, "Content-Type": "application/json"},
            timeout=60
        )
        
        if response.status_code == 200 and 'application/pdf' in response.headers.get('content-type', ''):
            # Deduct credit
            if get_user_credit(user) > 0:
                user.pdf_balance -= 1
                db.update_user(user)
            db.log_download(user_id, session_id, "success")
            
            await update.message.reply_document(
                document=InputFile(response.content, filename=f"fayda_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"),
                caption=f"✅ Downloaded successfully!\n📄 Remaining: {get_user_credit(user)}",
                reply_markup=get_main_keyboard(user_id)
            )
            context.user_data.pop('session_id', None)
            context.user_data.pop('fan', None)
            return MAIN_MENU
        else:
            await update.message.reply_text("❌ Download failed. Please try again.")
            return MAIN_MENU
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        return MAIN_MENU

async def handle_payment_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    user_id = update.effective_user.id
    
    if text == "🔙 Back":
        await update.message.reply_text("Main menu:", reply_markup=get_main_keyboard(user_id))
        return MAIN_MENU
    
    try:
        amount = int(text.split()[0])
        context.user_data['payment_amount'] = amount
        await update.message.reply_text(
            f"💰 Send {amount} Birr to {PAYMENT_PHONE} via TeleBirr\n\n"
            f"📸 After sending, send the screenshot here.\n"
            f"Type /cancel to cancel."
        )
        return WAITING_SCREENSHOT
    except:
        await update.message.reply_text("Invalid amount. Choose from buttons.")
        return PAYMENT_AMOUNT

async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    
    if update.message.text == "/cancel":
        await update.message.reply_text("Payment cancelled.", reply_markup=get_main_keyboard(user_id))
        context.user_data.pop('payment_amount', None)
        return MAIN_MENU
    
    if not update.message.photo:
        await update.message.reply_text("📸 Please send a photo screenshot.")
        return WAITING_SCREENSHOT
    
    amount = context.user_data.get('payment_amount')
    if not amount:
        await update.message.reply_text("Session expired. Start again.")
        return MAIN_MENU
    
    photo = update.message.photo[-1]
    file = await photo.get_file()
    os.makedirs("screenshots", exist_ok=True)
    file_path = f"screenshots/payment_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    await file.download_to_drive(file_path)
    
    db.create_payment_request(user_id, amount, file_path)
    await update.message.reply_text(
        f"✅ Payment request submitted!\n"
        f"💰 Amount: {amount} Birr\n"
        f"⏳ Waiting for admin approval.",
        reply_markup=get_main_keyboard(user_id)
    )
    
    # Notify admins
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                admin_id,
                photo=InputFile(file_path),
                caption=f"💰 New Payment\n"
                        f"👤 @{update.effective_user.username or user_id}\n"
                        f"🆔 {user_id}\n"
                        f"💳 {amount} Birr"
            )
        except:
            pass
    
    context.user_data.pop('payment_amount', None)
    return MAIN_MENU

async def handle_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Access denied.")
        return MAIN_MENU
    
    if text == "📊 Statistics":
        stats = db.get_total_stats()
        users = db.get_all_users()
        total_credits = sum(get_user_credit(u) for u in users)
        await update.message.reply_text(
            f"📊 Full Statistics\n\n"
            f"👥 Total: {stats['total_users']}\n"
            f"🟢 Active: {stats['active_users']}\n"
            f"📥 Downloads: {stats['total_downloads']}\n"
            f"📄 Total Credits: {total_credits}\n"
            f"💰 Pending: {stats['pending_payments']}",
            reply_markup=get_admin_keyboard()
        )
        return ADMIN_MENU
    
    elif text == "💰 Pending Payments":
        payments = db.get_pending_payments()
        if not payments:
            await update.message.reply_text("✅ No pending payments.", reply_markup=get_admin_keyboard())
            return ADMIN_MENU
        msg = "💰 Pending Payments:\n\n"
        for p in payments[:10]:
            msg += f"ID: {p['id']} | User: {p['user_id']} | {p['amount']} Birr\n"
        if len(payments) > 10:
            msg += f"\n... and {len(payments) - 10} more"
        await update.message.reply_text(msg, reply_markup=get_admin_keyboard())
        return ADMIN_MENU
    
    elif text == "✅ Approve Payment":
        await update.message.reply_text("Enter payment ID to approve:")
        context.user_data['admin_action'] = 'approve'
        return ADMIN_MENU
    
    elif text == "❌ Reject Payment":
        await update.message.reply_text("Enter payment ID to reject:")
        context.user_data['admin_action'] = 'reject'
        return ADMIN_MENU
    
    elif text == "👥 Users":
        users = db.get_all_users()
        msg = "👥 Users List:\n\n"
        for u in users[:20]:
            status = "🚫" if u.is_banned else "🟢"
            unlimited = "♾️" if u.unlimited_expiry and datetime.fromisoformat(u.unlimited_expiry) > datetime.now() else ""
            msg += f"{status} {u.user_id} | @{u.username or 'N/A'} | 📄{get_user_credit(u)} {unlimited}\n"
        if len(users) > 20:
            msg += f"\n... and {len(users) - 20} more"
        await update.message.reply_text(msg, reply_markup=get_admin_keyboard())
        return ADMIN_MENU
    
    elif text == "🚫 Ban User":
        await update.message.reply_text("Enter user ID to ban:")
        context.user_data['admin_action'] = 'ban'
        return ADMIN_MENU
    
    elif text == "📢 Broadcast":
        await update.message.reply_text("Enter broadcast message:")
        context.user_data['admin_action'] = 'broadcast'
        return ADMIN_MENU
    
    elif text == "📥 Download Logs":
        users = db.get_all_users()
        logs = "📥 DOWNLOAD LOGS\n\n"
        for u in users:
            logs += f"{u.user_id}: {u.total_downloads} downloads\n"
        os.makedirs("logs", exist_ok=True)
        log_path = f"logs/downloads_{datetime.now().strftime('%Y%m%d')}.txt"
        with open(log_path, "w") as f:
            f.write(logs)
        await update.message.reply_document(
            document=InputFile(log_path),
            caption="📥 Download Logs",
            reply_markup=get_admin_keyboard()
        )
        return ADMIN_MENU
    
    elif text == "🔙 Back to Main":
        await update.message.reply_text("Main menu:", reply_markup=get_main_keyboard(user_id))
        return MAIN_MENU
    
    else:
        action = context.user_data.get('admin_action')
        if action == 'approve':
            try:
                payment_id = int(text)
                db.approve_payment(payment_id)
                await update.message.reply_text(f"✅ Payment {payment_id} approved!", reply_markup=get_admin_keyboard())
            except:
                await update.message.reply_text("❌ Invalid ID.", reply_markup=get_admin_keyboard())
            context.user_data.pop('admin_action', None)
            return ADMIN_MENU
        
        elif action == 'reject':
            try:
                payment_id = int(text)
                db.reject_payment(payment_id)
                await update.message.reply_text(f"❌ Payment {payment_id} rejected!", reply_markup=get_admin_keyboard())
            except:
                await update.message.reply_text("❌ Invalid ID.", reply_markup=get_admin_keyboard())
            context.user_data.pop('admin_action', None)
            return ADMIN_MENU
        
        elif action == 'ban':
            try:
                target_id = int(text)
                if target_id == user_id:
                    await update.message.reply_text("❌ Cannot ban yourself!", reply_markup=get_admin_keyboard())
                else:
                    user = db.get_user(target_id)
                    if user:
                        user.is_banned = True
                        db.update_user(user)
                        await update.message.reply_text(f"🚫 User {target_id} banned!", reply_markup=get_admin_keyboard())
                        try:
                            await context.bot.send_message(target_id, "🚫 You have been banned.")
                        except:
                            pass
                    else:
                        await update.message.reply_text("❌ User not found.", reply_markup=get_admin_keyboard())
            except:
                await update.message.reply_text("❌ Invalid ID.", reply_markup=get_admin_keyboard())
            context.user_data.pop('admin_action', None)
            return ADMIN_MENU
        
        elif action == 'broadcast':
            users = db.get_all_users()
            sent = 0
            await update.message.reply_text(f"📢 Broadcasting to {len(users)} users...")
            for user in users:
                if not user.is_banned:
                    try:
                        await context.bot.send_message(user.user_id, f"📢 {text}")
                        sent += 1
                    except:
                        pass
            await update.message.reply_text(f"✅ Broadcast sent to {sent} users.", reply_markup=get_admin_keyboard())
            context.user_data.pop('admin_action', None)
            return ADMIN_MENU
        
        await update.message.reply_text("Use admin buttons.", reply_markup=get_admin_keyboard())
        return ADMIN_MENU

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    context.user_data.clear()
    await update.message.reply_text("Cancelled.", reply_markup=get_main_keyboard(user_id))
    return MAIN_MENU

# ============ MAIN ============
def main():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    
    print("🤖 Fayda Rental Bot Starting...")
    print(f"📱 Bot: @{os.environ.get('BOT_USERNAME', 'unknown')}")
    print(f"🌐 API: {API_BASE_URL}")
    print(f"👑 Admins: {ADMIN_IDS}")
    
    app = Application.builder().token(TOKEN).build()
    
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu)],
            PAYMENT_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_payment_amount)],
            WAITING_SCREENSHOT: [MessageHandler(filters.PHOTO, handle_screenshot),
                                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_screenshot)],
            FAN_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_fan)],
            OTP_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_otp)],
            ADMIN_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_menu)]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )
    
    app.add_handler(conv_handler)
    app.add_handler(MessageHandler(filters.Regex(r'^⚙️ Admin Panel$'), handle_menu))
    
    print("✅ Bot is running!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
