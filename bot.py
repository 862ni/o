import os
import random
import string
import threading
import asyncio
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.environ["BOT_TOKEN"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

LETTERS = string.ascii_lowercase
DIGITS = string.digits

# ---------------------------------------------------------------
# كل دالة ترجع: True = متاح ، False = غير متاح ، None = ما قدرت أتحقق
# ---------------------------------------------------------------


async def check_tiktok(client, name):
    try:
        r = await client.get(f"https://www.tiktok.com/@{name}", headers=HEADERS)
    except Exception:
        return None
    if r.status_code == 404:
        return True
    if r.status_code == 200:
        t = r.text
        if '"statusCode":10221' in t or '"statusCode":10202' in t:
            return True
        if '"uniqueId"' in t or '"statusCode":0' in t:
            return False
    return None


async def check_instagram(client, name):
    try:
        r = await client.get(
            f"https://www.instagram.com/{name}/", headers=HEADERS, follow_redirects=False
        )
    except Exception:
        return None
    if r.status_code == 404:
        return True
    if r.status_code == 200 and f"@{name}" in r.text.lower():
        return False
    return None


async def check_snapchat(client, name):
    try:
        r = await client.get(f"https://www.snapchat.com/add/{name}", headers=HEADERS)
    except Exception:
        return None
    if r.status_code == 404:
        return True
    if r.status_code == 200 and name in r.text.lower():
        return False
    return None


async def check_discord(client, name):
    try:
        r = await client.post(
            "https://discord.com/api/v9/unique-username/username-attempt-unauthed",
            json={"username": name},
            headers={"User-Agent": HEADERS["User-Agent"]},
        )
    except Exception:
        return None
    if r.status_code == 200:
        try:
            return not r.json().get("taken", True)
        except Exception:
            return None
    return None


async def check_psn(client, name):
    try:
        r = await client.post(
            "https://accounts.api.playstation.com/api/v1/accounts/onlineIds",
            json={"onlineId": name, "reserveIfAvailable": False},
            headers={"User-Agent": HEADERS["User-Agent"]},
        )
    except Exception:
        return None
    if r.status_code in (200, 201, 204):
        return True
    if r.status_code == 400:
        return False
    return None


# اسم البرنامج: (دالة الفحص، عدد الفحوصات المتزامنة، تأخير صغير بين كل فحص)
# كل برنامج له عماله الخاصين فما أحد يبطّئ الثاني
SERVICES = {
    "TikTok": (check_tiktok, 10, 0.0),
    "Instagram": (check_instagram, 6, 0.0),
    "Snapchat": (check_snapchat, 10, 0.0),
    "Discord": (check_discord, 3, 0.3),
    "PlayStation": (check_psn, 6, 0.0),
}

PAUSE_AFTER_FAILS = 20   # كم فشل متتالي قبل ما نوقف البرنامج مؤقتاً
PAUSE_SECONDS = 90       # مدة الإيقاف المؤقت


def random_name():
    """يوزر من 3 أو 4 خانات يناسب كل البرامج: يبدأ بحرف وينتهي بحرف أو رقم"""
    length = random.choice([3, 4])
    first = random.choice(LETTERS)
    last = random.choice(LETTERS + DIGITS)
    middle = "".join(random.choice(LETTERS + DIGITS + "_") for _ in range(length - 2))
    return first + middle + last


class Session:
    def __init__(self):
        self.started = time.time()
        self.queue = asyncio.Queue()
        self.seen = {s: set() for s in SERVICES}
        self.stats = {
            s: {"checked": 0, "found": 0, "unknown": 0, "streak": 0, "pause_until": 0.0}
            for s in SERVICES
        }

    def next_name(self, svc):
        seen = self.seen[svc]
        if len(seen) > 300000:
            seen.clear()
        for _ in range(30):
            n = random_name()
            if n not in seen:
                seen.add(n)
                return n
        return None


async def worker(sess, svc, client):
    checker, _, delay = SERVICES[svc]
    st = sess.stats[svc]
    while True:
        now = time.time()
        if st["pause_until"] > now:
            await asyncio.sleep(min(5, st["pause_until"] - now))
            continue

        name = sess.next_name(svc)
        if name is None:
            await asyncio.sleep(0.05)
            continue

        res = await checker(client, name)
        st["checked"] += 1

        if res is None:
            st["unknown"] += 1
            st["streak"] += 1
            if st["streak"] >= PAUSE_AFTER_FAILS:
                st["pause_until"] = time.time() + PAUSE_SECONDS
                st["streak"] = 0
        else:
            st["streak"] = 0
            if res is True:
                # تأكيد ثاني قبل الإرسال عشان ما يطلع لك يوزر خطأ
                await asyncio.sleep(0.7)
                res2 = await checker(client, name)
                if res2 is True:
                    st["found"] += 1
                    await sess.queue.put((name, svc))

        if delay:
            await asyncio.sleep(delay)


async def sender(sess, bot, chat_id):
    while True:
        first = await sess.queue.get()
        items = [first]
        await asyncio.sleep(0.3)
        while not sess.queue.empty():
            items.append(sess.queue.get_nowait())
        grouped = {}
        for n, s in items:
            grouped.setdefault(n, []).append(s)
        text = "\n".join(f"✅ {n} — {' ، '.join(ss)}" for n, ss in grouped.items())
        try:
            await bot.send_message(chat_id, text)
        except Exception:
            await asyncio.sleep(2)
        await asyncio.sleep(0.5)


async def run_session(sess, bot, chat_id):
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=40)
    async with httpx.AsyncClient(timeout=8, follow_redirects=True, limits=limits) as client:
        coros = [sender(sess, bot, chat_id)]
        for svc, (_, workers, _) in SERVICES.items():
            for _ in range(workers):
                coros.append(worker(sess, svc, client))
        await asyncio.gather(*coros)


sessions = {}  # chat_id -> (Session, Task)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in sessions and not sessions[chat_id][1].done():
        await update.message.reply_text("البحث شغال أصلاً 🔍\n/status للتفاصيل\n/stop للإيقاف")
        return
    sess = Session()
    task = asyncio.create_task(run_session(sess, context.bot, chat_id))
    sessions[chat_id] = (sess, task)
    await update.message.reply_text(
        "بديت البحث السريع عن يوزرات من 3 و4 خانات 🔍\n"
        "البرامج: " + "، ".join(SERVICES) + "\n"
        "كل يوزر أرسله لك تم فحصه مرتين للتأكد.\n\n"
        "/status لمعرفة وين وصل البحث\n"
        "/stop للإيقاف"
    )


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    item = sessions.get(chat_id)
    if item and not item[1].done():
        item[1].cancel()
        await update.message.reply_text("وقفت البحث ⏹")
    else:
        await update.message.reply_text("ما في بحث شغال.")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    item = sessions.get(chat_id)
    if not item or item[1].done():
        await update.message.reply_text("ما في بحث شغال. أرسل /start")
        return
    sess = item[0]
    mins = max((time.time() - sess.started) / 60, 0.01)
    now = time.time()
    lines = [f"⏱ شغال من {mins:.1f} دقيقة\n"]
    total = 0
    for svc, st in sess.stats.items():
        total += st["checked"]
        pause = " ⏸ موقوف مؤقتاً" if st["pause_until"] > now else ""
        lines.append(
            f"{svc}: فحص {st['checked']} | متاح {st['found']} | ⚠️ فشل {st['unknown']}{pause}"
        )
    lines.append(f"\nالسرعة: {total / (mins * 60):.1f} فحص/ثانية")
    await update.message.reply_text("\n".join(lines))


class Ping(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


def run_ping():
    port = int(os.environ.get("PORT", 10000))
    HTTPServer(("0.0.0.0", port), Ping).serve_forever()


async def main():
    threading.Thread(target=run_ping, daemon=True).start()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("status", status))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
