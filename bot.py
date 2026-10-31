import websocket
import msgpack
import json
import time
import threading
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import ssl
import math
import sys
import os
import logging

sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("fishbot")

# ==========================================
# TOKEN FROM ENV
# ==========================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not TELEGRAM_BOT_TOKEN:
    log.error("❌ TELEGRAM_BOT_TOKEN env variable is missing.")
    sys.exit(1)

# ==========================================
# GLOBALS & CONFIG
# ==========================================
last_tg_ok = time.time()
last_ws_ok = time.time()
TG_TIMEOUT = 120
WS_TIMEOUT = 180

WS_URL = "wss://api-fishmcloud.ugame.vn:2083"
CONFIG_FILE = "bot_config.json"
WS_HEADERS = [
    "User-Agent: Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36",
    "Origin: https://fishmya.ugame.vn",
    "Accept-Language: my-MM,my;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control: no-cache"
]

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

config_data = {
    "owner_id": None,
    "tokens": [],
    "selected_index": 0,
}

def load_config():
    global config_data
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                loaded = json.load(f)
                config_data.update(loaded)
                if "tokens" not in config_data:
                    config_data["tokens"] = []
                if "selected_index" not in config_data:
                    config_data["selected_index"] = 0
        except Exception:
            log.exception("load_config failed")

def save_config():
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config_data, f, indent=2)
    except Exception:
        log.exception("save_config failed")

load_config()

# ==========================================
# BOT INSTANCE
# ==========================================
class BotInstance:
    def __init__(self, token, owner_id):
        self.token = token
        self.owner_id = owner_id
        self.is_running = False
        self.ws_conn = None
        self.ws_lock = threading.Lock()
        self.error_count = 0
        self.max_errors = 1
        self.last_error_msg = "None"
        self.is_restarting = False
        
        self.heartbeat_alive = False
        self.shoot_alive = False
        
        # အမြဲတမ်း 4x Mode အသုံးပြုမည်
        self.use_4x_alive = False   
        
        self.in_game = False
        self.login_handled = False
        self.play_handled = False
        self.game_creds = {"username": "", "password": ""}
        self.fish_list = {}
        self.fish_lock = threading.Lock()
        self.cycle_duration = 120
        self.cycle_pause = 5
        self.stats = {
            "requests_sent": 0,
            "coins_spent": 0,
            "coins_gained": 0,
            "fish_killed": 0,
            "start_balance": 0,
            "current_balance": 0,
        }
        self.stats_lock = threading.Lock()
        self.current_target_id = None

        self.login_start_ts = 0.0
        self.last_coin_change_ts = 0.0
        self.last_balance_value = 0
        self.login_timeout = 25
        self.coin_stale_timeout = 180
        
        # msgId Auto-increment & Response Tracking
        self.msg_id_counter = 10
        self.pending_item_requests = set()
        self.speed_confirmed_notified = False

    def reset_stats(self):
        with self.stats_lock:
            for k in self.stats:
                self.stats[k] = 0

    def log_stats(self):
        with self.stats_lock:
            profit = self.stats["coins_gained"] - self.stats["coins_spent"]
            msg = (
                f"📊 STATS [{self.token[:10]}...]\n"
                f"Requests: {self.stats['requests_sent']}\n"
                f"Spent: {self.stats['coins_spent']:,}\n"
                f"Gained: {self.stats['coins_gained']:,}\n"
                f"Profit: {profit:,}\n"
                f"Fish: {self.stats['fish_killed']}\n"
                f"Balance: {self.stats['current_balance']:,}\n"
                f"Mode: 🔥 4x Burst (Always ON)"
            )
        if self.owner_id:
            try:
                bot.send_message(self.owner_id, msg)
            except Exception as e:
                pass

    def send_ws(self, payload_dict):
        if self.ws_conn and self.ws_conn.connected:
            try:
                self.ws_conn.send(
                    msgpack.packb(payload_dict, use_bin_type=True),
                    opcode=websocket.ABNF.OPCODE_BINARY,
                )
                with self.stats_lock:
                    self.stats["requests_sent"] += 1
                    if payload_dict.get("route") == "shoot":
                        self.stats["coins_spent"] += 24 # 4x Mode Expense
                return True
            except Exception as e:
                if not self.is_restarting:
                    self.error_count += 1
                    self.last_error_msg = str(e)
        return False

    def stop_all_threads(self):
        self.heartbeat_alive = False
        self.shoot_alive = False
        self.use_4x_alive = False
        self.login_handled = False
        self.play_handled = False
        self.in_game = False
        self.current_target_id = None
        self.login_start_ts = 0.0
        self.last_coin_change_ts = 0.0
        self.last_balance_value = 0
        self.pending_item_requests.clear()
        with self.fish_lock:
            self.fish_list.clear()

    def start_ws_connection(self):
        current_time = int(time.time())
        url = f"{WS_URL}?access_token={self.token}&log=true&cp=myid&lang=my&wss=true&t={current_time}"
        
        try:
            conn = websocket.create_connection(
                url,
                header=WS_HEADERS,
                sslopt={"cert_reqs": ssl.CERT_NONE},
                timeout=30,
            )
            with self.ws_lock:
                self.ws_conn = conn
            log.info(f"[{self.token[:10]}...] WS Connected")

            self.login_start_ts = time.time()
            self.last_coin_change_ts = time.time()
            self.last_balance_value = 0
            self.pending_item_requests.clear()
            self.speed_confirmed_notified = False

            self.send_ws(
                {
                    "route": "mytelLogin",
                    "data": {"accessToken": self.token, "language": "my", "cp": "myid"},
                    "msgId": 1,
                }
            )
            threading.Thread(target=self.ws_recv_loop, args=(conn,), daemon=True).start()
        except Exception as e:
            time.sleep(2)

    def ws_recv_loop(self, ws):
        while ws.connected and not self.is_restarting:
            try:
                data = ws.recv()
                if not data:
                    break
                self.handle_message(data, ws)
            except Exception:
                break

    def handle_message(self, data, ws):
        global last_ws_ok
        try:
            last_ws_ok = time.time()
            decoded = msgpack.unpackb(data, raw=False)
            if not isinstance(decoded, dict):
                return
            route = decoded.get("route", "")
            msg_id = decoded.get("msgId", -1)
            inner = decoded.get("data", decoded)
            if not isinstance(inner, dict):
                inner = {}

            # Item အသုံးပြုခြင်း Response ကို စစ်ဆေးသည့်အပိုင်း
            if msg_id in self.pending_item_requests:
                self.pending_item_requests.discard(msg_id)
                if inner.get("ok") is False:
                    err_msg = inner.get("error", "") or inner.get("msg", "") or "Error"
                    log.warning(f"[{self.token[:10]}] Item Use Failed: {err_msg}")
                    if self.owner_id and not self.speed_confirmed_notified:
                        try:
                            bot.send_message(self.owner_id, f"⚠️ 4x Speed အသုံးပြုရန် မအောင်မြင်ပါ: {err_msg}")
                        except Exception:
                            pass
                elif "remain" in inner or "endTime" in inner:
                    log.info(f"[{self.token[:10]}] ⚡ Speed Item Active (msgId: {msg_id})")
                    if not self.speed_confirmed_notified and self.owner_id:
                        self.speed_confirmed_notified = True
                        try:
                            bot.send_message(self.owner_id, "✅ ဂိမ်း Server မှ 4x Speed အသုံးပြုမှုကို အတည်ပြုလိုက်ပါပြီ။ သေနတ်အမြန်ပစ်ပါမည်။")
                        except Exception:
                            pass
                return

            if isinstance(inner, dict) and inner.get("ok") is False:
                err_msg = inner.get("error", "") or inner.get("msg", "") or inner.get("message", "Error")
                if any(k in str(err_msg).lower() for k in ["token", "expire", "auth", "invalid"]):
                    self.is_running = False
                    if self.owner_id:
                        try:
                            bot.send_message(self.owner_id, f"❌ Token သက်တမ်းကုန်သွားပါပြီ။\nအကြောင်းရင်း: {err_msg}")
                        except Exception:
                            pass
                    return

            if route == "OnUpdateObjects":
                objects = inner.get("objects", [])
                dead_fish = inner.get("deadFish", [])
                with self.fish_lock:
                    for obj in objects:
                        f_id = obj.get("id")
                        if f_id:
                            self.fish_list[f_id] = obj
                    for df in dead_fish:
                        f_id = df.get("id")
                        if f_id in self.fish_list:
                            if f_id == self.current_target_id:
                                self.current_target_id = None
                            del self.fish_list[f_id]
            elif route == "OnUpdateObject":
                f_id = inner.get("id")
                if f_id:
                    with self.fish_lock:
                        self.fish_list[f_id] = inner
            elif route == "OnObjectDie":
                f_id = inner.get("id")
                with self.fish_lock:
                    if f_id in self.fish_list:
                        if f_id == self.current_target_id:
                            self.current_target_id = None
                        del self.fish_list[f_id]
                if inner.get("playerId") == self.game_creds.get("username"):
                    with self.stats_lock:
                        self.stats["fish_killed"] += 1
                        gained = inner.get("cash", 0)
                        self.stats["coins_gained"] += gained
                        if gained > 0:
                            self.last_coin_change_ts = time.time()
            elif route == "OnUpdateCash":
                if inner.get("playerId") == self.game_creds.get("username"):
                    new_cash = inner.get("cash", 0)
                    with self.stats_lock:
                        if new_cash != self.stats["current_balance"]:
                            self.last_coin_change_ts = time.time()
                            self.last_balance_value = new_cash
                        self.stats["current_balance"] = new_cash

            if msg_id == 1:
                if inner.get("ok"):
                    self.login_handled = True
                    self.game_creds["username"] = inner.get("username", "")
                    self.game_creds["password"] = inner.get("password", "")
                    with self.stats_lock:
                        self.stats["start_balance"] = inner.get("cash", 0)
                        self.stats["current_balance"] = inner.get("cash", 0)
                    self.last_coin_change_ts = time.time()
                    if not self.heartbeat_alive:
                        threading.Thread(target=self.heartbeat_loop, args=(ws,), daemon=True).start()
                    time.sleep(0.5)
                    self.send_ws({
                        "route": "play",
                        "data": {"playerId": self.game_creds["username"], "password": self.game_creds["password"], "index": 0},
                        "msgId": 2,
                    })
            elif msg_id == 2:
                if inner.get("ok"):
                    self.play_handled = True
                    self.last_coin_change_ts = time.time()
                    self.start_game_actions(ws)
        except Exception:
            pass

    def heartbeat_loop(self, ws):
        self.heartbeat_alive = True
        while self.is_running and self.heartbeat_alive and ws.connected and not self.is_restarting:
            self.send_ws({"route": "ping", "data": {}, "msgId": 0})
            time.sleep(2)
        self.heartbeat_alive = False

    def use_4x_loop(self, ws):
        self.use_4x_alive = True
        while self.is_running and self.use_4x_alive and ws.connected and not self.is_restarting:
            try:
                self.msg_id_counter += 1
                current_msg_id = self.msg_id_counter
                self.pending_item_requests.add(current_msg_id)
                
                # Speed Item (Type 6) ကို အသုံးပြုရန် Request ပို့ခြင်း
                self.send_ws({
                    "route": "useItem", 
                    "data": {"type": 6}, 
                    "msgId": current_msg_id
                })
                
                # ၁၀ စက္ကန့်ခြား၍ ထပ်သုံးရန် စောင့်မည် (ကြားထဲ Connection ပြတ်ပါက ချက်ချင်းထွက်မည်)
                for _ in range(100):
                    if not self.is_running or not self.use_4x_alive or self.is_restarting or not ws.connected:
                        break
                    time.sleep(0.1)
            except Exception:
                time.sleep(1)
                
        self.use_4x_alive = False

    def auto_shoot_loop(self, ws):
        self.shoot_alive = True
        while self.is_running and self.shoot_alive and ws.connected and not self.is_restarting:
            try:
                with self.fish_lock:
                    fish_list_items = list(self.fish_list.values())

                if not fish_list_items:
                    self.current_target_id = None
                    time.sleep(0.05)
                    continue

                target_fish = None
                if self.current_target_id:
                    for fish in fish_list_items:
                        if fish.get("id") == self.current_target_id:
                            target_fish = fish
                            break

                if not target_fish and fish_list_items:
                    target_fish = fish_list_items[0]
                    self.current_target_id = target_fish.get("id")

                if not target_fish:
                    time.sleep(0.05)
                    continue

                fish_id = target_fish.get("id")
                if fish_id:
                    # ငါးတည်နေရာ (x, y) ပါလာပါက တွက်ချက်မည်၊ မပါပါက တည့်တည့် (Angle 0) ဖြင့်ပစ်မည်
                    fx = target_fish.get("x")
                    fy = target_fish.get("y")
                    if fx is not None and fy is not None:
                        angle_rad = math.atan2(fy, fx)
                    else:
                        angle_rad = 0.0
                    
                    # အမြဲတမ်း 4x Mode အတိုင်း (၃) ချက်ဆက်တိုက် (Burst) ပစ်ပါမည်
                    for _ in range(3):
                        if not ws.connected or self.is_restarting:
                            break
                            
                        self.send_ws({
                            "route": "shoot",
                            "data": {
                                "rad": angle_rad,
                                "type": 4, 
                                "target": fish_id,
                                "rapidFire": True,
                                "auto": True,
                                "bulletSpeed": 2000,
                            },
                            "msgId": 0,
                        })

                        self.send_ws({
                            "route": "clientHitFish",
                            "data": {
                                "btype": 4,
                                "skillType": 0,
                                "fIds": [fish_id],
                                "bulletSpeed": 2000,
                            },
                            "msgId": 0,
                        })
                        
                        # ကျည်တစ်ချက်နှင့် တစ်ချက်ကြား ၀.၀၀၂ စက္ကန့် ခြားပေးမည်
                        time.sleep(0.002)

                    # Burst (၃) ချက်ပြီးတိုင်း Server အား ဖိအားမများစေရန် ၀.၀၁၅ စက္ကန့် ခြားပေးမည်
                    time.sleep(0.015)

            except Exception as e:
                # Thread မရပ်သွားစေရန်
                time.sleep(0.1)
                
        self.shoot_alive = False

    def start_game_actions(self, ws):
        if not self.is_running or self.is_restarting:
            return
        self.in_game = True
        self.current_target_id = None
        
        self.send_ws({"route": "useItem", "data": {"type": 4}, "msgId": 0})
        self.send_ws({
            "route": "clientActiveGun",
            "data": {
                "btype": 4,
                "gun": "gun1",
                "skillType": "none",
                "locationX": 0,
                "locationY": 0,
                "bulletSpeed": 2000,
            },
            "msgId": 0,
        })
        
        # Shoot loop နှင့် 4x Use Item Loop ကို အမြဲတမ်း စတင်ပါမည်
        if not self.shoot_alive:
            threading.Thread(target=self.auto_shoot_loop, args=(ws,), daemon=True).start()
            
        if not self.use_4x_alive:
            threading.Thread(target=self.use_4x_loop, args=(ws,), daemon=True).start()

    def run_cycle(self):
        log.info(f"[{self.token[:10]}...] Starting cycle")
        self.is_running = True
        self.reset_stats()
        self.error_count = 0
        self.is_restarting = False

        while self.is_running:
            self.start_ws_connection()
            start_time = time.time()
            while self.is_running and not self.is_restarting:
                elapsed = time.time() - start_time
                now = time.time()

                if elapsed >= self.cycle_duration:
                    self.log_stats()
                    break

                if self.error_count >= self.max_errors:
                    self.log_stats()
                    break

                # Error ကာကွယ်မှု - Connection ပြတ်သွားလျှင် သို့မဟုတ် Thread ရပ်သွားလျှင် ချက်ချင်း Auto-Restart လုပ်မည်
                if self.in_game:
                    if not self.ws_conn or not self.ws_conn.connected:
                        break
                    if not self.shoot_alive or not self.use_4x_alive:
                        break
                        
                    stale = now - self.last_coin_change_ts
                    if stale > self.coin_stale_timeout:
                        self.log_stats()
                        break

                time.sleep(1)

            self.is_restarting = True
            with self.ws_lock:
                if self.ws_conn:
                    try:
                        self.ws_conn.close()
                    except Exception:
                        pass
                self.ws_conn = None
            self.stop_all_threads()
            self.error_count = 0
            if self.is_running:
                time.sleep(self.cycle_pause)
            self.is_restarting = False

# ==========================================
# GLOBAL BOT CONTROLLER
# ==========================================
active_bot = None
active_thread = None
bot_lock = threading.Lock()

def get_selected_token():
    if not config_data["tokens"]:
        return None
    idx = config_data.get("selected_index", 0)
    if idx >= len(config_data["tokens"]):
        idx = 0
        config_data["selected_index"] = 0
        save_config()
    return config_data["tokens"][idx]

def start_bot():
    global active_bot, active_thread
    with bot_lock:
        if active_bot and active_bot.is_running:
            return "စက်လည်ပတ်နေပြီးသားဖြစ်ပါသည်။"
        token = get_selected_token()
        if not token:
            return "Token မရှိသေးပါ။ Token အရင်ပေးပို့ပါ။"
        active_bot = BotInstance(token, config_data["owner_id"])
        t = threading.Thread(target=active_bot.run_cycle, daemon=True)
        t.start()
        active_thread = t
        return f"✅ ဂိမ်းစတင်ကစားနေပါပြီ (Token: {token[:10]}...)"

def stop_bot():
    global active_bot
    with bot_lock:
        if not active_bot or not active_bot.is_running:
            return "Bot က ရပ်တန့်ထားပြီးသားဖြစ်ပါသည်။"
        active_bot.is_running = False
        active_bot = None
        return "🛑 ဂိမ်းကစားခြင်းကို ရပ်တန့်လိုက်ပါပြီ။"

# ==========================================
# TELEGRAM COMMANDS
# ==========================================
def get_main_markup():
    # Toggle ခလုတ်ကို ဖြုတ်လိုက်ပါပြီ
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("▶️ Start", callback_data="cmd_start"),
        InlineKeyboardButton("🛑 Stop", callback_data="cmd_stop"),
        InlineKeyboardButton("📋 List Tokens", callback_data="cmd_list"),
    )
    return markup

@bot.message_handler(commands=["start"])
def handle_start_cmd(message):
    user_id = message.chat.id
    if config_data["owner_id"] is None:
        config_data["owner_id"] = user_id
        save_config()
        bot.send_message(user_id, "👑 သင်သည် Bot ၏ Owner ဖြစ်သွားပါပြီ။")
    elif config_data["owner_id"] != user_id:
        return
    bot.send_message(user_id, "🤖 MyID Fish Bot Manager\n\n📌 Token အသစ်ထည့်ရန် Token စာသားကို တိုက်ရိုက်ပေးပို့ပါ။", reply_markup=get_main_markup())

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    user_id = call.message.chat.id
    if config_data["owner_id"] != user_id:
        bot.answer_callback_query(call.id, "Unauthorized")
        return
    cmd = call.data
    
    if cmd == "cmd_start":
        msg = start_bot()
        bot.answer_callback_query(call.id, msg)
        bot.send_message(user_id, msg)
    elif cmd == "cmd_stop":
        msg = stop_bot()
        bot.answer_callback_query(call.id, msg)
        bot.send_message(user_id, msg)
    elif cmd == "cmd_list":
        if not config_data["tokens"]:
            bot.send_message(user_id, "Tokens မရှိသေးပါ။")
        else:
            text = "📋 သင့်၏ Tokens များ:\n"
            for i, t in enumerate(config_data["tokens"]):
                selected = "✅ " if i == config_data.get("selected_index", 0) else "   "
                text += f"{selected} {i + 1}. {t[:15]}...\n"
            bot.send_message(user_id, text)

@bot.message_handler(func=lambda message: message.text and message.text.startswith("eyJ") and len(message.text.split(".")) >= 3)
def handle_direct_token(message):
    user_id = message.chat.id
    if config_data["owner_id"] is None:
        config_data["owner_id"] = user_id
        save_config()
    elif config_data["owner_id"] != user_id:
        return

    token = message.text.strip()
    if token not in config_data["tokens"]:
        config_data["tokens"].append(token)
    
    idx = config_data["tokens"].index(token)
    config_data["selected_index"] = idx
    save_config()
    
    bot.reply_to(message, "✅ Token အသစ်ကို လက်ခံရရှိပါပြီ။\nအလိုအလျောက် ဂိမ်းစတင်ရန် ပြင်ဆင်နေပါပြီ...")
    
    global active_bot
    if active_bot and active_bot.is_running:
        stop_bot()
        time.sleep(2)
        
    msg = start_bot()
    bot.send_message(user_id, msg)

# ==========================================
# SUPERVISOR
# ==========================================
def polling_loop():
    global last_tg_ok
    offset = None
    try:
        bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass

    while True:
        try:
            updates = bot.get_updates(offset=offset, timeout=25, long_polling_timeout=20)
            last_tg_ok = time.time()
            if updates:
                for u in updates:
                    offset = u.update_id + 1
                    try:
                        bot.process_new_updates([u])
                    except Exception:
                        pass
        except Exception:
            time.sleep(5)

def watchdog_loop():
    global last_tg_ok, last_ws_ok
    while True:
        time.sleep(20)
        now = time.time()
        if (now - last_tg_ok) > TG_TIMEOUT:
            os._exit(1)
        if active_bot and active_bot.is_running and active_bot.in_game:
            if (now - last_ws_ok) > WS_TIMEOUT:
                os._exit(1)

if __name__ == "__main__":
    threading.Thread(target=watchdog_loop, daemon=True).start()
    polling_loop()
