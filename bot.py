import logging
import asyncio
import random
import time
import os
import json
import re
import io
import difflib
import requests
import httpx  
import aiohttp
import arabic_reshaper
from aiogram import types
from aiogram.dispatcher.filters import Text # بديل لـ F في الإصدار الثاني
from pilmoji import Pilmoji 
from PIL import Image, ImageDraw, ImageFont, ImageOps
from bidi.algorithm import get_display
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from supabase import create_client, Client 

# إعداد السجلات
logging.basicConfig(level=logging.INFO)
# --- [ 1. إعدادات الهوية والاتصال ] ---
ADMIN_ID = 7988144062
OWNER_USERNAME = "@Ya_79k"

# سحب التوكينات من Render (لن يعمل البوت بدونها في الإعدادات)
API_TOKEN = os.getenv('BOT_TOKEN')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# --- [ استدعاء القلوب الثلاثة - تشفير خارجي ] ---
# هنا الكود يطلب المفاتيح من المتغيرات فقط، ولا توجد أي قيمة مسجلة هنا
GROQ_KEYS = [
    os.getenv('G_KEY_1'),
    os.getenv('G_KEY_2'),
    os.getenv('G_KEY_3')
]

# تصفية المصفوفة لضمان عدم وجود قيم فارغة
GROQ_KEYS = [k for k in GROQ_KEYS if k]
current_key_index = 0  # مؤشر تدوير القلوب

# التحقق من وجود المتغيرات الأساسية لضمان عدم حدوث Crash
if not API_TOKEN or not GROQ_KEYS:
    logging.error("❌ خطأ: المتغيرات المشفرة مفقودة في إعدادات Render!")

# تعريف المحركات
bot = Bot(token=API_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


active_quizzes = {}
cancelled_groups = set() # لحفظ المجموعات التي ضغطت إلغاء مؤقتاً
# في أعلى الملف تماماً (Global Variable)
answered_users_global = {}
async def cleanup_leftover_data():
    """
    تنظيف مخلفات الجلسات السابقة لضمان بداية نظيفة للقاعدة
    """
    try:
        # حذف أي مسابقة كانت "نشطة" عند إغلاق البوت
        # الـ CASCADE سيتكفل بحذف المشاركين واللوج المرتبط بها تلقائياً
        res = supabase.table("active_quizzes").delete().eq("is_active", True).execute()
        
        count = len(res.data) if res.data else 0
        if count > 0:
            logging.info(f"♻️ مكنسة الطوارئ: تم تنظيف {count} مسابقة عالقة بنجاح.")
        else:
            logging.info("✨ مكنسة الطوارئ: قاعدة البيانات نظيفة بالفعل.")
            
    except Exception as e:
        logging.error(f"⚠️ فشل مكنسة الطوارئ في التنظيف: {e}")

# استدعاء الدالة عند تشغيل البوت (Aiogram مثال)
async def on_startup(dp):
    await cleanup_leftover_data()
   
# ==========================================
# 4. محركات العرض والقوالب (Display Engines) - النسخة المصلحة
# ==========================================

# [3] دالة قالب السؤال (المصلحة)
async def send_quiz_question(chat_id, q_data, current_num, total_num, settings):
    is_pub = settings.get('is_public', False) 
    q_scope = "إذاعة عامة 🌐" if is_pub else "مسابقة داخلية 📍"
    q_mode = settings.get('mode', 'السرعة ⚡')
    is_hint_on = settings.get('smart_hint', False) # الزر المفعل قبل الحفظ
    
    # استخراج التلميح العادي (البنيوي)
    normal_hint = settings.get('normal_hint', "")

    if q_data.get('bot_category_id'):
        real_source = "أسئلة البوت 🤖"
    elif q_data.get('user_id') or 'answer_text' in q_data:
        real_source = "أسئلة الأعضاء 👥"
    else:
        real_source = "أقسام خاصة 🔒"

    q_text = q_data.get('question_content') or q_data.get('question_text') or "⚠️ نص السؤال مفقود!"
    
    text = (
        f"🎓 **الـمنـظـم:** {settings['owner_name']} ☁️\n"
        f"  ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
        f"📌 **السؤال:** « {current_num} » من « {total_num} »\n"
        f"📂 **القسم:** `{settings['cat_name']}`\n"
        f"🛠 **المصدر:** `{real_source}`\n"
        f"📡 **النطاق:** **{q_scope}**\n"
        f"🔖 **النظام:** {q_mode}\n"
        f"⏳ **المهلة:** {settings['time_limit']} ثانية\n"
        f"  ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n\n"
        f"❓ **السؤال:**\n**{q_text}**\n"
    )
    
    # عرض التلميح العادي فقط إذا كان الزر مفعلاً
    if is_hint_on and normal_hint:
        text += f"\n💡 **تلميح الإجابة:** {normal_hint}"

    try:
        return await bot.send_message(chat_id, text, parse_mode='Markdown')
    except Exception as e:
        clean_text = text.replace("*", "").replace("`", "").replace("_", "")
        return await bot.send_message(chat_id, clean_text)
# ==========================================
# --- [ 2. بداية الدوال المساعدة قالب الاجابات  ] ---
# ==========================================
async def send_creative_results(chat_id, correct_ans, winners, group_scores, is_public=False, mode="السرعة ⚡", group_names=None):
    """
    🎁 نسخة الهدية - قالب ياسر الملكي (التشطيب النهائي 2026)
    تتميز بحساب ألقاب السرعة وجمالية التنسيق العالمي.
    """
    mode_icon = "⚡" if "سرعة" in mode else "⏰"
    is_time_mode = "الوقت" in mode or "وقت" in mode

    msg = f"🏆 <b>تـفـاصـيـل الـجـولـة الـمـلـكـيـة</b> {mode_icon}\n"
    msg += "  ━━━━━━━━━━━━━━━━━━\n"
    msg += f"🎯 الإجابة: <b>「 {correct_ans} 」</b>\n"
    msg += "  ━━━━━━━━━━━━━━━━━━\n\n"

    # --- [ 1. عرض الأبطال مع ألقاب السرعة للهدية ] ---
    if winners:
        msg += "🌟 <b>نجم الجولة الحالية:</b>\n"
        
        # في نظام السرعة نعرض الأول فقط بلقب مميز
        winners_to_show = winners if is_time_mode else [winners[0]]
        
        for idx, w in enumerate(winners_to_show):
            # تنسيق الميداليات
            medal = "🥇" if idx == 0 else "🥈" if idx == 1 else "🥉" if idx == 2 else "✨"
            
            # 🎁 [إضافة الهدية]: لقب السرعة
            speed_title = ""
            if not is_time_mode and 'time' in w:
                t = float(w['time'])
                if t < 1.0: speed_title = "⚡ (خارق الصمت)"
                elif t < 3.0: speed_title = "🚀 (القناص السريع)"
                elif t < 5.0: speed_title = "🏹 (المتمكن)"
                else: speed_title = "🧠 (الذكي)"

            time_info = f" ⏱ <code>{w['time']}s</code>" if 'time' in w else ""
            msg += f"{medal} ⇠ <b>{w['name']}</b> {time_info} {speed_title}\n"
    else:
        msg += "💤 <b>انتهى الوقت دون حسم!</b>\n"
    
    msg += "  ━━━━━━━━━━━━━━━━━━\n\n"

    # --- [ 2. الترتيب العالمي (مدمج بدون تكرار) ] ---
    msg += "📊 <b>الـنـقـاط الـتـراكمـيـة (TOP):</b>\n"
    combined_players = {}
    for gid, players in group_scores.items():
        for uid, pdata in players.items():
            if uid not in combined_players:
                combined_players[uid] = {"name": pdata['name'], "points": 0}
            combined_players[uid]['points'] += pdata['points']
    
    sorted_players = sorted(combined_players.values(), key=lambda x: x['points'], reverse=True)
    # عرض التوب 5 فقط لجمالية القالب
    for i, p in enumerate(sorted_players[:5]):
        m = "👑" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "👤"
        msg += f"{m} <b>{p['name']}</b> ⇠ <code>{p['points']}</code> ن\n"
    
    msg += "  ━━━━━━━━━━━━━━━━━━\n"

    # --- [ 3. إحصائيات المجموعات (نظام الفرسان) ] ---
    if is_public:
        msg += "\n👥 <b>تـنـافـس الـمـجـمـوعـات :</b>\n"
        group_ranking = []
        for gid, players in group_scores.items():
            if players:
                total_group_pts = sum(p['points'] for p in players.values())
                local_top = sorted(players.values(), key=lambda x: x['points'], reverse=True)
                group_ranking.append({'id': gid, 'points': total_group_pts, 'players': local_top})
        
        sorted_groups = sorted(group_ranking, key=lambda x: x['points'], reverse=True)
        for i, g in enumerate(sorted_groups):
            g_name = group_names.get(str(g['id']), f"جروب {g['id']}") if group_names else f"جروب {g['id']}"
            # إضافة وسام لأول مجموعة
            g_medal = "⭐" if i == 0 else "▫️"
            msg += f"{g_medal} <b>{g_name}</b> ⇠ (<code>{g['points']}</code>ن)\n"
            # عرض فارس المجموعة الأول فقط لتقليل طول الرسالة
            if g['players']:
                msg += f"    أبطال المجموعه: 👤 <b>{g['players'][0]['name']}</b>\n"
            msg += "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n"

    msg += "\n🔥 <i>استعد.. السؤال التالي في الطريق!</i>"

    # الإرسال مع return (ضروري جداً لمحرك الحذف)
    try:
        return await bot.send_message(chat_id, msg, parse_mode="HTML")
    except Exception as e:
        import logging
        logging.error(f"⚠️ HTML Parsing Error: {e}")
        # في حال فشل الـ HTML، يتم تنظيف النص وإرساله كنص عادي لضمان الحذف لاحقاً
        clean_text = msg.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "").replace("<i>", "").replace("</i>", "")
        return await bot.send_message(chat_id, clean_text)
        

async def send_broadcast_final_results(chat_id, scores, total_q, group_names=None):
    try:
        msg = "🌍 <b>تـم اخـتـتـام المسابقة الـعـالـمـيـة</b> 🌍\n"
        msg += ". ━━━━━━━━━━━━━━━━━━\n"
        msg += "🏆 <b>: { كـشـف نـتـائـج الـمـجـموعـات }</b>\n"
        msg += "  ━━━━━━━━━━━━━━━━━━\n\n"

        all_global_players = {}
        group_summary = []
        max_possible_pts = total_q * 10 
        found_any_score = False

        # --- [ 1. معالجة البيانات ] ---
        for gid, players in scores.items():
            if not players: continue
            
            group_players_list = []
            group_total_pts = 0

            # ترتيب لاعبي المجموعة داخلياً
            sorted_p = sorted(players.items(), key=lambda x: x[1].get('points', 0) if isinstance(x[1], dict) else 0, reverse=True)

            for uid, p_data in sorted_p:
                found_any_score = True
                pts = p_data.get('points', 0) if isinstance(p_data, dict) else 0
                name = p_data.get('name', 'لاعب')
                
                group_total_pts += pts
                p_link = f'<a href="tg://user?id={uid}">{name}</a>'
                # ترتيب سطر اللاعب من اليمين
                group_players_list.append(f"👤 : {p_link} [ <b>{pts}</b> ن ]")

                u_id_str = str(uid)
                if u_id_str not in all_global_players:
                    all_global_players[u_id_str] = {"name": name, "points": 0}
                all_global_players[u_id_str]['points'] += pts

            g_name = group_names.get(str(gid), f"جروب {gid}") if group_names else f"جروب {gid}"
            group_summary.append({
                'name': g_name,
                'total': group_total_pts,
                'players_text': "\n".join(group_players_list)
            })

        # --- [ 2. عرض المجموعات مع ترتيب الرموز ] ---
        if group_summary:
            sorted_groups = sorted(group_summary, key=lambda x: x['total'], reverse=True)
            
            for i, g in enumerate(sorted_groups, 1):
                is_winner = (i == 1)
                medal = "🥇 :" if i == 1 else "🥈 :" if i == 2 else "🥉 :" if i == 3 else "🔹 :"
                
                win_status = " ✨ [+1 🏆 : فوز]" if is_winner else ""
                
                msg += f"{medal} <b>{g['name']}</b> {win_status}\n"
                msg += f"📊 : إجمالي النقاط ( <code>{g['total']}</code> ن )\n"
                msg += f"{g['players_text']}\n"
                msg += "  ┅┅┅┅┅┅┅┅┅┅┅┅┅┅┅┅\n"

        # --- [ 3. ملوك الإذاعة (الترتيب الفردي) ] ---
        if all_global_players:
            msg += "\n👑 <b>: تـرتـيـب مـلـوك الـعـالـم :</b>\n"
            sorted_global = sorted(all_global_players.items(), key=lambda x: x[1]['points'], reverse=True)
            
            for i, (uid, p) in enumerate(sorted_global[:5], 1):
                icon = "🥇 :" if i == 1 else "🥈 :" if i == 2 else "🥉 :" if i == 3 else "👤 :"
                iq = min(int((p['points'] / max_possible_pts) * 100) + 40, 100) if max_possible_pts > 0 else 40
                msg += f"{icon} {p['name']} ⇠ <b>{p['points']}</b> ن (🧠 {iq}% IQ)\n"

        if not found_any_score:
            msg = "🌍 <b>: انتهت الإذاعة !</b>\n\nلم يتم تسجيل أي نقاط ."
        else:
            msg += "\n  ━━━━━━━━━━━━━━━━━━\n"
            msg += f"📋 : إجمالي الأسئلة ( <b>{total_q}</b> )\n"
            msg += "✅ : تم ترحيل الفوز للأبطال بنجاح !"

        return await bot.send_message(chat_id, msg, parse_mode="HTML")

    except Exception as e:
        import logging
        logging.error(f"❌ : خطأ في الإذاعة : {e}")
        
# ==========================================
# ==========================================
async def send_creative_results2(chat_id, correct_ans, winners, overall_scores):
    """تصميم ياسر المطور: دمج الفائزين والترتيب في رسالة واحدة"""
    msg =  "  ━━━━━━━━━━━━━━━━━━━\n"
    msg += f"✅ الإجابة الصحيحة: <b>{correct_ans}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━\n\n"
    
    if winners:
        msg += "━ المتفوقون ✅ ━\n"
        for i, w in enumerate(winners, 1):
            msg += f"{i}- {w['name']} (كسبت 1 نقطة)\n"
    else:
        msg += "❌ لم ينجح أحد في الإجابة على هذا السؤال\n"
    
    leaderboard = sorted(overall_scores.values(), key=lambda x: x['points'], reverse=True)
    msg += "\n━ 🏆 الترتيب  ━\n"
    medals = ["🥇", "🥈", "🥉"]
    for i, player in enumerate(leaderboard[:3]):
        medal = medals[i] if i < 3 else "👤"
        msg += f"{medal} {player['name']} — {player['points']}\n"
    
    # --- [ نهاية قالب الإجابة - المحرك الخاص ] ---
    try:
        # كلمة return هنا هي المحرك الأساسي لعملية الحذف لاحقاً
        return await bot.send_message(chat_id, msg, parse_mode="HTML")
    
    except Exception as e:
        import logging
        logging.error(f"⚠️ HTML Error in Private Results: {e}")
        # تنظيف النص من التاغات في حال وجود خطأ في التنسيق لضمان الإرسال
        clean_msg = msg.replace("<b>","").replace("</b>","").replace("<code>","").replace("</code>","")
        return await bot.send_message(chat_id, clean_msg)
        
async def send_final_results2(chat_id, overall_scores, total_q):
    """
    🥇 تصميم ياسر الملكي - نسخة المسابقات الخاصة V3
    ضبط المحاذاة اليمينية باستخدام الفواصل النقطية :
    """
    try:
        # 🎨 رأس القالب
        msg =  "  ━━━━━━━━━━━━━━━━━━━\n"
        msg += "🏁 <b>: انـتـهـت الـمـسـابـقـة الـخـاصـة</b>\n"
        msg += "🔥 <b>: حـصـاد الـعـمـالـقـة والأبـطـال</b>\n"
        msg += "  ━━━━━━━━━━━━━━━━━━━\n\n"
        
        msg += "🏆 <b>: { لـوحـة الـشـرف والـتـتـويـج }</b>\n\n"

        # ترتيب اللاعبين حسب النقاط
        sorted_players = sorted(overall_scores.values(), key=lambda x: x['points'], reverse=True)
        max_possible_pts = total_q * 10 
        
        # الأيقونات مع الفواصل لضبط اليمين
        medals = ["🥇 :", "🥈 :", "🥉 :", "👤 :", "👤 :"]

        for i, player in enumerate(sorted_players[:10]):  # عرض توب 10
            # اختيار الأيقونة المناسبة
            icon = medals[i] if i < 5 else "👤 :"
            
            # حساب IQ الجولة
            round_iq = min(int((player['points'] / max_possible_pts) * 100) + 40, 100) if max_possible_pts > 0 else 40
            
            # السطر الذهبي (محاذاة من اليمين)
            msg += f"{icon} <b>{player['name']}</b>\n"
            msg += f"🏅 <b>:</b> المركز ( {i+1} ) ⇠ <b>{player['points']}</b> ن\n"
            msg += f"🧠 <b>:</b> ذكاء الجولة ⇠ <code>{round_iq}% IQ</code>\n"
            
            # تمييز بطل المسابقة الخاصة
            if i == 0:
                msg += "✨ <b>: [+1 🔥 فـوز خـاص مـسـجـل]</b>\n"
                
            msg += "  ┅┅┅┅┅┅┅┅┅┅┅┅┅┅┅┅\n"

        # 📊 ذيل القالب
        msg += "\n📊 <b>: إحـصـائـيـات الـتـفـاعـل</b>\n"
        msg += f"📋 <b>:</b> إجمالي الأسئلة ⇠ ( <b>{total_q}</b> )\n"
        msg += f"👥 <b>:</b> عدد المشاركين ⇠ ( <b>{len(overall_scores)}</b> )\n"
        msg += "  ━━━━━━━━━━━━━━━━━━━\n"
        msg += "❤️ <b>: تهانينا للفائزين وحظاً أوفر للبقية</b>\n"
        msg += "✅ <b>: تم ترحيل الألقاب والجوائز بنجاح</b>"

        await bot.send_message(chat_id, msg, parse_mode="HTML")
        
    except Exception as e:
        import logging
        logging.error(f"❌ خطأ في العملية السابقة: {e}")

# ============================================================
# 1. دوال النظام الذكي (الرتب، التخصصات، الحسابات)
# ============================================================
def generate_14_digit_bank():
    """توليد رقم حساب بنكي احترافي مكون من 14 رقم"""
    return "".join([str(random.randint(0, 9)) for _ in range(14)])

# =========================================
# تأكد أن الدالة تبدأ من بداية السطر تماماً (بدون مسافات جهة اليسار)
# ==========================================
async def sync_points_to_global_db(group_scores, winners_list=None, cat_name="عام", is_special=False):
    """
    👑 محرك ياسر الملكي المتكامل 2026
    - المسابقة الخاصة: النقطة = إجابة كاملة | الرفع في special_wins
    - المسابقة العامة: 10 نقاط = إجابة واحدة | الرفع في total_wins
    - تحديث تلقائي للألقاب، التخصصات، والرتب التعليمية.
    """
    
    # 1. تحديد أبطال الجولة
    winning_groups = winners_list if winners_list else []
    if not winning_groups:
        group_totals = {gid: sum(p.get('points', 0) for p in players.values()) 
                        for gid, players in group_scores.items()}
        if group_totals:
            top_group_id = max(group_totals, key=group_totals.get)
            winning_groups = [top_group_id]

    # 2. تجميع حصاد اللاعبين من جميع المجموعات
    final_tallies = {}
    for cid, players in group_scores.items():
        is_the_champion_group = (cid in winning_groups)
        
        for uid, p_data in players.items():
            u_id = int(uid)
            if u_id not in final_tallies:
                final_tallies[u_id] = {
                    "name": p_data.get('name', 'لاعب مجهول'), 
                    "pts": 0, "ans_count": 0, "won_round": 0
                }
            
            pts = p_data.get('points', 0)
            final_tallies[u_id]["pts"] += pts
            
            # ✅ قاعدة الحساب: الخاصة (1:1) | العامة (10:1)
            final_tallies[u_id]["ans_count"] += pts if is_special else (pts // 10)
            
            if is_the_champion_group:
                final_tallies[u_id]["won_round"] = 1

    # 3. المزامنة مع Supabase (التحديث الذكي)
    for uid, data in final_tallies.items():
        try:
            res = supabase.table("users_global_profile").select("*").eq("user_id", uid).execute()
            
            # --- [ دوال النظام الذكي ] ---
            def calculate_rank(total_ans):
                if total_ans <= 50: return "🌱 عضو جديد"
                elif total_ans <= 150: return "📚 طالب مجتهد"
                elif total_ans <= 300: return "🎓 خريج متميز"
                elif total_ans <= 600: return "📑 باحث علمي"
                elif total_ans <= 1200: return "🔬 عالم فذ"
                elif total_ans <= 2500: return "🏛️ بروفيسور"
                elif total_ans <= 5000: return "👑 أسطورة زدني"
                else: return "✨ سلطان المعرفة"

            
            def calculate_specialty(stats):
                if not stats: return "هاوي"
                top_cat = max(stats, key=stats.get)
                score = stats[top_cat]
                if score > 1000: return f"🏅 أسطورة {top_cat}"
                elif score > 500: return f"👨‍🔬 عالم {top_cat}"
                elif score > 100: return f"📜 خبير  {top_cat}"
                else: return f"🔍 محب لـ {top_cat}"

            if res.data:
                current = res.data[0]
            
                # تحديث إحصائيات الأقسام
                current_stats = current.get('category_stats') or {}
                current_stats[cat_name] = current_stats.get(cat_name, 0) + data['ans_count']
                
                total_ans = (current.get('correct_answers_count') or 0) + data['ans_count']
                titles = current.get('titles', [])
                
                # لمسة "نجم المسابقات" للفائز في الخاصة 🔥
                if is_special and data['won_round'] > 0:
                    if "🔥 : نجم المسابقات" not in titles:
                        titles.append("🔥 : نجم المسابقات")

                # تجهيز حمولة البيانات
                upd_payload = {
                    "user_name": data['name'],
                    "total_points": (current.get('total_points') or 0) + data['pts'],
                    "wallet": (current.get('wallet') or 0) + data['pts'],
                    "correct_answers_count": total_ans,
                    "iq_score": min(150, (current.get('iq_score') or 50) + (data['ans_count'] // 5)),
                    "educational_rank": calculate_rank(total_ans),
                    "category_stats": current_stats,
                    "specialty_title": calculate_specialty(current_stats),
                    "titles": titles,
                    "last_update": "now()"
                }

                # ✅ الرفع في العمود الصحيح بناءً على نوع المسابقة
                if is_special:
                    upd_payload["special_wins"] = (current.get('special_wins') or 0) + data['won_round']
                else:
                    upd_payload["total_wins"] = (current.get('total_wins') or 0) + data['won_round']
                
                supabase.table("users_global_profile").update(upd_payload).eq("user_id", uid).execute()
                logging.info(f"✅ تم تحديث بروفايل: {data['name']}")

            else:
                # 🆕 إنشاء لاعب جديد (الهوية الملكية)
                new_payload = {
                    "user_id": uid, "user_name": data['name'],
                    "bank_account": generate_14_digit_bank(),
                    "total_points": data['pts'], "wallet": data['pts'],
                    "correct_answers_count": data['ans_count'],
                    "total_wins": data['won_round'] if not is_special else 0,
                    "special_wins": data['won_round'] if is_special else 0,
                    "iq_score": 60,
                    "category_stats": {cat_name: data['ans_count']},
                    "educational_rank": calculate_rank(data['ans_count']),
                    "specialty_title": calculate_specialty({cat_name: data['ans_count']}),
                    "titles": ["🌱 : عضو جديد"], "inventory": [],
                    "cards_inventory": {"time": 1, "full": 1, "shield": 1, "reveal": 1, "double": 1, "letter": 1}
                }
                supabase.table("users_global_profile").insert(new_payload).execute()
                logging.info(f"🆕 تسجيل عضو عالمي جديد: {data['name']}")

        except Exception as e:
            logging.error(f"❌ فشل ترحيل بيانات {uid}: {e}")
            
            
# ============================================================
# دالة تحديث سجلات المجموعة (الذكاء الجماعي)
# الوظيفة: حفظ نقاط المجموعة وتحديد العضو الأبرز فيها
# ============================================================
async def update_group_stats(group_id: int, group_name: str, user_id: int, user_name: str, points: int):
    try:
        # 1. محاولة جلب بيانات المجموعة
        res = supabase.table("groups_global_stats").select("*").eq("group_id", group_id).execute()
        
        if not res.data:
            # إذا كانت المجموعة جديدة، ننشئ لها سجلاً
            supabase.table("groups_global_stats").insert({
                "group_id": group_id,
                "group_name": group_name,
                "total_points": points,
                "top_member_name": user_name,
                "top_member_id": user_id,
                "members_count": 1
            }).execute()
        else:
            group = res.data[0]
            new_total_points = group['total_points'] + points
            
            # 2. منطق تحديد "الأكثر ذكاءً": (يمكنك ربطها بنقاط العضو الحالية)
            # هنا سنفترض أن العضو الذي جلب نقاطاً للمجموعة الآن يُقارن بالعضو المسجل سابقاً
            # ملاحظة: يمكنك تطويرها لتقارن مع الـ IQ_score من جدول المستخدمين
            
            update_data = {
                "total_points": new_total_points,
                "group_name": group_name, # لتحديث الاسم إذا تغير
                "last_activity": "now()"
            }
            
            # تحديث العضو المتصدر إذا حقق نقاطاً عالية (مثال)
            if points > 50: # إذا أضاف أكثر من 50 نقطة في مرة واحدة
                update_data["top_member_name"] = user_name
                update_data["top_member_id"] = user_id

            supabase.table("groups_global_stats").update(update_data).eq("group_id", group_id).execute()

    except Exception as e:
        print(f"Error updating group stats: {e}")
                    
# --- إصلاح اتجاه النصوص (حلك الذكي) ---
def fix_arabic(text):
    return "\u200F" + str(text) if text else ""

def fix_number(text):
    return "\u200E" + str(text) if text else ""

# --- دالة جلب صورة البروفايل ومعالجتها (الحل السريع لـ Aiogram) ---
async def get_profile_img(bot, user_id):
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file_id = photos.photos[0][-1].file_id
            file = await bot.get_file(file_id)
            
            photo_bytes = io.BytesIO()
            await bot.download_file(file.file_path, destination=photo_bytes)
            photo_bytes.seek(0)

            p_raw = Image.open(photo_bytes).convert("RGBA")
            size = (220, 220)
            p_raw = p_raw.resize(size, Image.LANCZOS)
            
            mask = Image.new("L", size, 0)
            ImageDraw.Draw(mask).ellipse((0, 0) + size, fill=255)
            
            output = Image.new("RGBA", size, (0, 0, 0, 0))
            output.paste(p_raw, (0, 0), mask)
            return output
        return None
    except Exception as e:
        logging.warning(f"⚠️ فشل جلب صورة المستخدم {user_id}: {e}")
        return None

# --- الدالة الرئيسية لتوليد البطاقة (نسخة الأعلام والهوية الموحدة) ---
async def generate_zidni_card(user_id: int, bot, supabase):
    base_path = "assets/fonts/"
    paths = {
        "font": os.path.join(base_path, "font.ttf"),
        "emoji": os.path.join(base_path, "emoji.ttf"),
        "card": "assets/images/zidni_card.png"
    }

    try:
        # 1. جلب البيانات من Supabase
        res = supabase.table("users_global_profile").select("*").eq("user_id", int(user_id)).execute()
        if not res.data:
            return None, None
        user_db = res.data[0]

        # 2. فتح القالب والخطوط
        template = Image.open(paths["card"]).convert("RGBA")
        font_main = ImageFont.truetype(paths["font"], 35)
        font_info = ImageFont.truetype(paths["font"], 30)

        # 3. جلب ووضع صورة البروفايل
        profile_circle = await get_profile_img(bot, user_id)
        if profile_circle:
            template.paste(profile_circle, (83, 62), profile_circle)

        # 4. تجهيز البيانات النصية (تم حذف specialty_title وإضافة flag)
        name = str(user_db.get("user_name", "غير معروف"))[:20]
        # الرتبة الآن تعرض المستوى التعليمي فقط
        rank = f"{user_db.get('educational_rank', 'طالب')}"
        wallet = user_db.get("wallet", 0)
        acc_num = user_db.get("bank_account", "0000")
        # جلب العلم من القاعدة (الافتراضي علم اليمن إذا لم يوجد)
        flag = user_db.get("country_flag", "")

        # 5. الرسم على البطاقة (بإحداثياتك المعتمدة)
        with Pilmoji(template) as pilmoji:
            white, gold = (255, 255, 255), (212, 175, 55)
            # نمرر مسار خط الإيموجي لضمان ظهور علم اليمن والرموز
            emoji_path = paths["emoji"] if os.path.exists(paths["emoji"]) else None

            # الاسم
            pilmoji.text((795, 210), fix_arabic(name), font=font_main, fill=white, anchor="ra", emoji_fontpath=emoji_path)
            # الدولة
            pilmoji.text((795, 280), fix_arabic("اليمن"), font=font_info, fill=gold, anchor="ra", emoji_fontpath=emoji_path)
            # الرتبة
            pilmoji.text((795, 345), fix_arabic(rank), font=font_info, fill=white, anchor="ra", emoji_fontpath=emoji_path)
            # الرصيد
            pilmoji.text((795, 415), fix_arabic(f"{wallet:,} ن"), font=font_info, fill=gold, anchor="ra", emoji_fontpath=emoji_path)
            # رقم الحساب
            pilmoji.text((585, 550), fix_number(f"ZD-{acc_num}"), font=font_info, fill=white, anchor="mm")
        # 6. إخراج الصورة والبيانات (للكابشن)
        output = io.BytesIO()
        template.save(output, format="PNG")
        output.seek(0)
        
        return output, user_db

    except Exception as e:
        logging.error(f"❌ خطأ في generate_zidni_card: {e}")
        return None, None
        
# ============================================================
# دالة تنسيق بطاقة المجموعة (Top Groups)
# ============================================================
def format_group_card(group_data: dict):
    g = group_data
    card = f"<b>🏰 : إحـصـائـيـات الـمـجـمـوعـة 🏰</b>\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += f"👥 <b>: المجموعة ⇠</b> <code>{g.get('group_name')}</code>\n"
    card += f"💰 <b>: رصيد النقاط ⇠</b> <code>{g.get('total_points', 0)}</code> ن\n"
    card += f"📊 <b>: عدد المشتركين ⇠</b> <code>{g.get('members_count', 0)}</code>\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += f"🧠 <b>: العضو الأبرز ⇠</b> {g.get('top_member_name', 'لا يوجد')}\n"
    card += f"🏆 <b>: مـركـز الـمـجـمـوعـة ⇠</b> [ عـالـمـي ]\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += "✨ <i>تعاونوا لتصبح مجموعتكم هي الأكثر ذكاءً!</i>"
    
    return card
def update_system_setting(setting_name, new_value):
    """
    تحديث إعدادات النظام في جدول system_settings
    """
    try:
        # نقوم بتحديث القيمة حيث اسم الإعداد هو ACTIVE_GROQ_KEY
        res = supabase.table("system_settings").update({"key_value": new_value}).eq("key_name", setting_name).execute()
        
        # إذا تمت العملية بنجاح نرجع True
        if res.data:
            return True
        return False
    except Exception as e:
        logging.error(f"Error updating system setting: {e}")
        return False
# ==========================================
# 1. كيبوردات التحكم الرئيسية (Main Keyboards)
# ==========================================
def get_main_control_kb(user_id):
    """توليد كيبورد لوحة التحكم الرئيسية مشفرة بآيدي المستخدم"""
    kb = InlineKeyboardMarkup(row_width=2).add(
        InlineKeyboardButton("📝 إضافة خاصة", callback_data=f"custom_add_{user_id}"),
        InlineKeyboardButton("📅 جلسة سابقة", callback_data=f"dev_session_{user_id}"),
        InlineKeyboardButton("🛒 المتجر العالمي", callback_data=f"open_shop_{user_id}"),
        InlineKeyboardButton("🏆 تجهيز مسابقة", callback_data=f"setup_quiz_{user_id}"),
        InlineKeyboardButton("📊 لوحة الصدارة", callback_data=f"dev_leaderboard_{user_id}"),
        InlineKeyboardButton("🛑 إغلاق", callback_data=f"close_bot_{user_id}")
    )
    return kb


# 3️⃣ [ دالة عرض القائمة الرئيسية للأقسام ]
async def custom_add_menu(c, owner_id, state):
    if state:
        await state.finish()
        
    kb = get_categories_kb(owner_id) 
    await c.message.edit_text(
        "⚙️ **لوحة إعدادات أقسامك الخاصة:**\n\nأهلاً بك! اختر من الخيارات أدناه لإدارة بنك أسئلتك وإضافة أقسام جديدة:",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await c.answer()
# ==========================================
# ---الدالة التي طلبتها (تأكد أنها موجودة بهذا الاسم) ---
# ==========================================
def get_categories_kb(user_id):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("➕ إضافة قسم جديد", callback_data=f"add_new_cat_{user_id}"))
    kb.add(InlineKeyboardButton("📋 قائمة الأقسام", callback_data=f"list_cats_{user_id}"))
    kb.add(InlineKeyboardButton("🔙 الرجوع لصفحة التحكم", callback_data=f"back_to_main_{user_id}"))
    
    return kb

# ==========================================
# 2. دوال عرض الواجهات الموحدة (UI Controllers)
# ==========================================
async def show_category_settings_ui(message: types.Message, cat_id, owner_id, is_edit=True):
    """الدالة الموحدة لعرض إعدادات القسم بضغطة واحدة"""
    # جلب البيانات من سوبابيس
    cat_res = supabase.table("categories").select("name").eq("id", cat_id).single().execute()
    q_res = supabase.table("questions").select("*", count="exact").eq("category_id", cat_id).execute()
    
    cat_name = cat_res.data['name']
    q_count = q_res.count if q_res.count else 0
    
    txt = (f"⚙️ إعدادات القسم: {cat_name}\n\n"
           f"📊 عدد الأسئلة المضافة: {q_count}\n"
           f"ماذا تريد أن تفعل الآن؟")

    # بناء الأزرار وتشفيرها بالآيدي المزدوج (cat_id + owner_id)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ إضافة سؤال", callback_data=f"add_q_{cat_id}_{owner_id}"),
        InlineKeyboardButton("📝 تعديل الاسم", callback_data=f"edit_cat_{cat_id}_{owner_id}")
    )
    kb.add(
        InlineKeyboardButton("🔍 عرض الأسئلة", callback_data=f"view_qs_{cat_id}_{owner_id}"),
        InlineKeyboardButton("🗑️ حذف الأسئلة", callback_data=f"del_qs_menu_{cat_id}_{owner_id}")
    )
    kb.add(InlineKeyboardButton("❌ حذف القسم", callback_data=f"confirm_del_cat_{cat_id}_{owner_id}"))
    kb.add(
        InlineKeyboardButton("🔙 رجوع", callback_data=f"list_cats_{owner_id}"),
        InlineKeyboardButton("🏠 الرئيسية", callback_data=f"back_to_control_{owner_id}")
    )
    
    if is_edit:
        await message.edit_text(txt, reply_markup=kb, parse_mode="Markdown")
    else:
        # تستخدم هذه بعد الـ message_handler (save_cat) لأن الرسالة السابقة قد حذفت
        await message.answer(txt, reply_markup=kb, parse_mode="Markdown")
# ==========================================
# ==========================================
def get_setup_quiz_kb(user_id):
    """كيبورد تهيئة المسابقة مشفر بآيدي المستخدم"""
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("👥 أقسام الأعضاء (إسئلة الاعضاء)", callback_data=f"members_setup_step1_{user_id}"),
        InlineKeyboardButton("👤 أقسامك الخاصة (مكتبتي)", callback_data=f"my_setup_step1_{user_id}"),
        InlineKeyboardButton("🤖 أقسام البوت (الرسمية)", callback_data=f"bot_setup_step1_{user_id}"),
        InlineKeyboardButton("🔙 رجوع للقائمة الرئيسية", callback_data=f"back_to_control_{user_id}")
    )
    return kb

# ============================================================
# 1. دوال الأزرار (Keyboards)
# ============================================================
def get_leaderboard_keyboard():
    """لوحة التحكم الرئيسية"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("💰 : قـائـمـة أغـنـيـاء الـعـرب", callback_data="top_wealth"),
        InlineKeyboardButton("🧠 : سـجـل أذكـيـاء الـمـجـرات", callback_data="top_iq"),
        InlineKeyboardButton("🏰 : تـرتـيـب أقـوى الـمـجـمـوعـات", callback_data="top_groups"),
        InlineKeyboardButton("❌ : إغـلاق الـسـجـل", callback_data="close_card")
    )
    return keyboard

def get_back_keyboard():
    """أزرار الرجوع والإغلاق (تظهر أسفل كل القوالب بلا استثناء)"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔙 : رجوع", callback_data="back_to_leaderboard"),
        InlineKeyboardButton("❌ : إغلاق", callback_data="close_card")
    )
    return keyboard

# ============================================================
# 2. رسالة الترحيب الرئيسية
# ============================================================
def get_leaderboard_main_message():
    # 1. القالب البصري للرسالة
    text = (
        "<b>🏆 | مـنـصـة الـشـرف الـعـالـمـيـة</b>\n"
        "<b>— — — — — — — — — — — — — —</b>\n\n"
        "مرحباً بك في سجلات <b>بنك زدني</b> الخالدة، "
        "هنا تُكتب أسماء العمالقة الذين تربعوا على العروش بالذكاء والمال والتحالفات.\n\n"
        "<b>📌 | اختر القائمة التي تود استكشافها:</b>\n\n"
        "💰 <b>: الأغنياء ⇠</b> لعرض أصحاب المليارات.\n"
        "🧠 <b>: الأذكياء ⇠</b> لعرض دهاه العقل وعباقرة الـ IQ.\n"
        "🏰 <b>: المجموعات ⇠</b> لعرض أقوى التحالفات الجماعية.\n\n"
        "<b>— — — — — — — — — — — — — —</b>\n"
        "✨ <i>المنافسة مشتعلة.. هل اسمك موجود بينهم؟</i>"
    )
    return text, get_leaderboard_keyboard()
# ============================================================
# 3. قوالب التنسيق الفخمة (Templates)
# ============================================================

def format_top_iq_list(data: list):
    header = "<b>🧠 : سـجـل أذكـيـاء الـمـجـرات</b>\n"
    header += "<b>— — — — — — — — — — — —</b>\n\n"
    body = ""
    medals = ["🥇 :", "🥈 :", "🥉 :", "🏅 :", "🏅 :", "🏅 :", "🏅 :", "🏅 :", "🏅 :", "🏅 :"]
    for i, user in enumerate(data):
        rank_icon = medals[i]
        name = user.get('user_name', 'مجهول')
        iq = user.get('iq_score', 0); ans = user.get('correct_answers_count', 0)
        rank_n = user.get('educational_rank', 'طالب'); flag = user.get('country_flag', '🌐')
        body += f"{rank_icon} <b>{name}</b> {flag}\n"
        body += f"    🔸 <b>: الرتبة :</b> <code>{rank_n}</code>\n"
        body += f"    🔹 <b>: الذكاء :</b> <code>{iq} IQ</code> | <b>الاجابات :</b> <code>{ans}</code>\n\n"
    return header + body + "<b>— — — — — — — — — — — —</b>"

def format_top_wealth_list(data: list):
    header = "<b>💰 : قـائـمـة أغـنـيـاء الـعـرب</b>\n"
    header += "<b>— — — — — — — — — — — —</b>\n\n"
    body = ""
    medals = ["👑 :", "💎 :", "💰 :", "💵 :", "💵 :", "💵 :", "💵 :", "💵 :", "💵 :", "💵 :"]
    for i, user in enumerate(data):
        rank_icon = medals[i]
        name = user.get('user_name', 'مجهول'); money = user.get('wallet', 0)
        inv = user.get('inventory', []); items = len(inv) if isinstance(inv, list) else 0
        gifts = user.get('special_wins', 0); flag = user.get('country_flag', '🌐')
        body += f"{rank_icon} <b>{name}</b> {flag}\n"
        body += f"    🔸 <b>: الرصيد :</b> <code>{money:,}</code> ن\n"
        body += f"    🔹 <b>: المقتنيات :</b> <code>{items}</code> | <b>الهدايا :</b> <code>{gifts}</code>\n\n"
    return header + body + "<b>— — — — — — — — — — — —</b>"

def format_top_groups_list(data: list):
    header = "<b>🏰 : تـرتـيـب أقـوى الـمـجـمـوعـات</b>\n"
    header += "<b>— — — — — — — — — — — —</b>\n\n"
    body = ""
    medals = ["🏛️ :", "🏟️ :", "🏤 :", "🏰 :", "🏰 :", "🏰 :", "🏰 :", "🏰 :", "🏰 :", "🏰 :"]
    for i, group in enumerate(data):
        rank_icon = medals[i]; g_name = group.get('group_name', 'مجموعة مجهولة')
        pts = group.get('total_points', 0); top_m = group.get('top_member_name', 'لا يوجد')
        m_count = group.get('members_count', 0)
        body += f"{rank_icon} <b>{g_name}</b>\n"
        body += f"    🔸 <b>: إجمالي النقاط :</b> <code>{pts:,}</code> ن\n"
        body += f"    🔹 <b>: العضو الأبرز :</b> <code>{top_m}</code>\n"
        body += f"    👥 <b>: عدد الأعضاء :</b> <code>{m_count}</code> عضواً\n\n"
    return header + body + "<b>— — — — — — — — — — — —</b>"

# ============================================================
# 4. معالج العمليات (Callback Handler)
# ============================================================

@dp.callback_query_handler(lambda c: c.data in ['top_wealth', 'top_iq', 'top_groups', 'back_to_leaderboard', 'close_card'])
async def process_board_navigation(c: types.CallbackQuery):
    action = c.data

    if action == "close_card":
        try: await c.message.delete()
        except: pass
        return await c.answer("تم إغلاق السجل")

    if action == "back_to_leaderboard":
        text, kb = get_leaderboard_main_message()
        return await c.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

    await c.answer("⏳ جاري جلب البيانات...")

    try:
        if action == "top_wealth":
            res = supabase.table("users_global_profile").select("*").order("wallet", desc=True).limit(10).execute()
            text = format_top_wealth_list(res.data)
        elif action == "top_iq":
            res = supabase.table("users_global_profile").select("*").order("iq_score", desc=True).limit(10).execute()
            text = format_top_iq_list(res.data)
        elif action == "top_groups":
            res = supabase.table("groups_global_stats").select("*").order("total_points", desc=True).limit(10).execute()
            text = format_top_groups_list(res.data)

        # تحديث الرسالة مع ضمان وجود أزرار الرجوع والإغلاق في كل القوالب
        await c.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode="HTML")

    except Exception as e:
        print(f"Leaderboard Error: {e}")
        await c.answer("❌ فشل الاتصال بقاعدة البيانات.", show_alert=True)

# ============================================================
# 5. استدعاء الكلمات والحذف التلقائي (النسخة الصاروخية)
# ============================================================

@dp.message_handler(lambda message: message.text in ['توب', 'التوب', 'الترتيب', 'لوحة الصدارة'] or message.text.startswith('/top'))
async def cmd_show_leaderboard(message: types.Message):
    """استدعاء اللوحة مع ميزة الحذف التلقائي بعد 60 ثانية"""
    text, reply_markup = get_leaderboard_main_message()
    
    # إرسال الرسالة وحفظ الكائن الخاص بها
    sent_msg = await message.reply(text=text, reply_markup=reply_markup, parse_mode="HTML")
    
    # مهمة الحذف التلقائي بعد 60 ثانية
    await asyncio.sleep(60)
    try:
        await sent_msg.delete()
        # اختياري: حذف رسالة المستخدم أيضاً ليبقى الشات نظيفاً
        await message.delete()
    except:
        pass # الرسالة قد تم حذفها يدوياً بالفعل
# ==========================================
# الدوال المساعدة المحدثة (حماية + أسماء حقيقية)
# ==========================================
async def render_members_list(message, eligible_list, selected_list, owner_id):
    """
    eligible_list: قائمة تحتوي على ديكشنري [{id: ..., name: ...}]
    """
    kb = InlineKeyboardMarkup(row_width=2)
    for member in eligible_list:
        m_id = str(member['id'])
        # نستخدم الاسم الحقيقي اللي جلبناه من جدول users
        status = "✅ " if m_id in selected_list else ""
        # الحماية: نمرر owner_id في نهاية الكولباك
        kb.insert(InlineKeyboardButton(
            f"{status}{member['name']}", 
            callback_data=f"toggle_mem_{m_id}_{owner_id}"
        ))
    
    if selected_list:
        # زر محمي تماماً لا ينتقل إلا بآيدي صاحب الجلسة
        kb.add(InlineKeyboardButton(
            f"➡️ تم اختيار ({len(selected_list)}) .. عرض أقسامهم", 
            callback_data=f"go_to_cats_step_{owner_id}"
        ))
    
    kb.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"setup_quiz_{owner_id}"))
    await message.edit_text("👥 <b>أقسام الأعضاء المبدعين:</b>\nاختر المبدعين لعرض أقسامهم:", reply_markup=kb, parse_mode="HTML")

# 2. دالة عرض المجلدات (نظام البوت الرسمي الجديد)
async def render_folders_list(message, eligible_folders, selected_folders, owner_id):
    kb = InlineKeyboardMarkup(row_width=2)
    for folder in eligible_folders:
        f_id = str(folder['id'])
        status = "✅ " if f_id in selected_folders else ""
        kb.insert(InlineKeyboardButton(
            f"{status}{folder['name']}", 
            callback_data=f"toggle_folder_{f_id}_{owner_id}"
        ))
    
    if selected_folders:
        kb.add(InlineKeyboardButton(
            f"➡️ تم اختيار ({len(selected_folders)}) .. عرض الأقسام", 
            callback_data=f"confirm_folders_{owner_id}"
        ))
    
    kb.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"setup_quiz_{owner_id}"))
    await message.edit_text("🗂️ <b>مجلدات البوت الرسمية:</b>\nاختر المجلدات المطلوبة:", reply_markup=kb, parse_mode="HTML")

# 3. دالة عرض الأقسام (محمية من المبعسسين)
async def render_categories_list(message, eligible_cats, selected_cats, owner_id):
    kb = InlineKeyboardMarkup(row_width=2)
    for cat in eligible_cats:
        cat_id_str = str(cat['id'])
        status = "✅ " if cat_id_str in selected_cats else ""
        kb.insert(InlineKeyboardButton(
            f"{status}{cat['name']}", 
            callback_data=f"toggle_cat_{cat_id_str}_{owner_id}"
        ))
    
    if selected_cats:
        # زر محمي: يمنع المبعسس من الانتقال لواجهة الإعدادات النهائية
        kb.add(InlineKeyboardButton(
            f"➡️ تم اختيار ({len(selected_cats)}) .. الإعدادات", 
            callback_data=f"final_quiz_settings_{owner_id}"
        ))
    
    kb.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"setup_quiz_{owner_id}"))
    await message.edit_text("📂 <b>اختر الأقسام المطلوبة:</b>", reply_markup=kb, parse_mode="HTML")

# ==========================================
async def render_final_settings_panel(message, data, owner_id):
    """الدالة الموحدة لعرض لوحة الإعدادات النهائية مشفرة بآيدي المالك"""
    q_time = data.get('quiz_time', 15)
    q_count = data.get('quiz_count', 10)
    q_mode = data.get('quiz_mode', 'السرعة ⚡')
    is_hint = data.get('quiz_hint_bool', False)
    is_broadcast = data.get('is_broadcast', False)
    
    q_hint_text = "مفعل ✅" if is_hint else "معطل ❌"
    q_scope_text = "إذاعة عامة 🌐" if is_broadcast else "مسابقة داخلية 📍"
    
    text = (
       f"⚙️ لوحة إعدادات المسابقة\n"
       f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
       f"📊 عدد الأسئلة: {q_count}\n"
       f"📡 النطاق: {q_scope_text}\n"
       f"🔖 النظام: {q_mode}\n"
       f"⏳ المهلة: {q_time} ثانية\n"
       f"💡 التلميح: {q_hint_text}\n"
       f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
       f"⚠️ *هذه الإعدادات خاصة بـ {data.get('owner_name', 'المنظم')} فقط*"
    )

    kb = InlineKeyboardMarkup(row_width=5)
    
    # 1. أزرار الأعداد
    kb.row(InlineKeyboardButton("📊 اختر عدد الأسئلة:", callback_data="ignore"))
    counts = [10, 15, 25, 32, 45]
    btn_counts = [InlineKeyboardButton(f"{'✅' if q_count==n else ''}{n}", callback_data=f"set_cnt_{n}_{owner_id}") for n in counts]
    kb.add(*btn_counts)

    # 2. أزرار التحكم (مشفره بالـ owner_id)
    kb.row(InlineKeyboardButton(f"⏱️ المهلة: {q_time} ثانية", callback_data=f"cyc_time_{owner_id}"))
    kb.row(
        InlineKeyboardButton(f"🔖 {q_mode}", callback_data=f"cyc_mode_{owner_id}"),
        InlineKeyboardButton(f"💡 التلميح: {q_hint_text}", callback_data=f"cyc_hint_{owner_id}")
    )
    kb.row(InlineKeyboardButton(f"📡 النطاق: {q_scope_text}", callback_data=f"tog_broad_{owner_id}"))
    
    kb.row(InlineKeyboardButton("🚀 حفظ وبدء المسابقة 🚀", callback_data=f"start_quiz_{owner_id}"))
    kb.row(InlineKeyboardButton("❌ إلغاء", callback_data=f"setup_quiz_{owner_id}"))
    
    await message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
# ==========================================
# 3. دوال الفحص الأمني والمحركات (Security Helpers & Engines)
# ==========================================
async def get_group_status(chat_id):
    """فحص حالة تفعيل المجموعة في الجدول الموحد الجديد groups_hub"""
    try:
        res = supabase.table("groups_hub").select("status").eq("group_id", chat_id).execute()
        return res.data[0]['status'] if res.data else "not_found"
    except Exception as e:
        logging.error(f"Error checking group status: {e}")
        return "error"
# ==========================================
async def run_visual_countdown(group_msgs, kb, base_info):
    """دالة العد التنازلي البصري - آخر 10 ثوانٍ 🔥"""
    timer_emojis = ["🔟", "9️⃣", "8️⃣", "7️⃣", "6️⃣", "5️⃣", "4️⃣", "3️⃣", "2️⃣", "1️⃣"]
    
    for emoji in timer_emojis:
        # نص الإعلان مع تحديث التوقيت فقط
        text = f"{base_info}\n\n⏳ **ستبدأ المسابقة بعد:** {emoji}\n👈 إن كنت لا تريد المشاركة اضغط إلغاء أدناه."
        
        edit_tasks = []
        for cid, mid in group_msgs.items():
            if cid not in cancelled_groups:
                edit_tasks.append(bot.edit_message_text(text, cid, mid, reply_markup=kb, parse_mode="Markdown"))
        
        await asyncio.gather(*edit_tasks, return_exceptions=True)
        await asyncio.sleep(1)

async def start_broadcast_process(c: types.CallbackQuery, quiz_id: int, owner_id: int):
    try:
        # 1. جلب بيانات المسابقة والمجموعات
        res_q = supabase.table("saved_quizzes").select("*").eq("id", quiz_id).single().execute()
        q = res_q.data
        if not q: return await c.answer("❌ تعذر جلب بيانات المسابقة")

        groups_res = supabase.table("groups_hub").select("group_id").eq("status", "active").execute()
        if not groups_res.data: return await c.answer("⚠️ لا توجد مجموعات نشطة!")

        all_chats = [g['group_id'] for g in groups_res.data]
        cancelled_groups.clear() 

        # 2. تجهيز نص الإعلان الثابت
        quiz_name = q.get('quiz_name', 'تحدي جديد')
        q_count = q.get('questions_count', 10)
        q_mode = q.get('mode', 'السرعة ⚡')
        cat_info = q.get('category_name', 'عام') 
        
        base_info = (
            f"**إعلان: مسابقة عامة منطلقة !** ™️\n"
            f"━━━━━━━━━━━━━━\n"
            f"🏆 المسابقة: **{quiz_name}**\n"
            f"📂 القسم: **{cat_info}**\n"
            f"🔢 عدد الأسئلة: **{q_count}**\n"
            f"⚙️ النوع: **{q_mode}**\n"
            f"👤 المنظم: **{c.from_user.first_name}**\n"
            f"━━━━━━━━━━━━━━"
        )

        # 3. إرسال رسائل التحضير الأولية لكل المجموعات
        group_msgs = {}
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("🚫 إلغاء المسابقة في مجموعتنا", callback_data=f"cancel_quiz_{quiz_id}"))

        for cid in all_chats:
            try:
                msg = await bot.send_message(cid, f"{base_info}\n\n🛰️ **جاري تحضير الإذاعة العالمية...**", parse_mode="Markdown", reply_markup=kb)
                group_msgs[cid] = msg.message_id
            except: continue

        # 4. المرحلة الأولى: انتظار هادئ (5 ثوانٍ)
        await asyncio.sleep(5)

        # 5. المرحلة الثانية: العد التنازلي البصري (10 ثوانٍ)
        await run_visual_countdown(group_msgs, kb, base_info)

        # 🚀 [ الخطوة الجوهرية 6: التصفية وتسجيل البيانات ] 🚀
        final_groups = [cid for cid in group_msgs if cid not in cancelled_groups]
        
        if final_groups:
            # تحديث الحالة البصرية قبل المحرك مباشرة
            launch_tasks = [bot.edit_message_text(f"{base_info}\n\n🚀 **تـم الانـطـلاق الآن! استعدوا..**", cid, mid, parse_mode="Markdown") for cid, mid in group_msgs.items() if cid in final_groups]
            await asyncio.gather(*launch_tasks, return_exceptions=True)

            try:
                # أ. إنشاء السجل الرقمي في سوبابيس (إلزامي للترتيب العالمي)
                active_res = supabase.table("active_quizzes").insert({
                    "quiz_name": quiz_name,
                    "created_by": owner_id, 
                    "is_global": True,
                    "is_active": True,
                    "total_questions": q_count,
                    "participants_ids": final_groups 
                }).execute()
                
                if not active_res.data:
                    raise Exception("فشل إنشاء سجل المسابقة")

                new_quiz_db_id = active_res.data[0]['id']

                # ب. تسجيل المشاركين (الحبل السري)
                participant_data = [{"quiz_id": new_quiz_db_id, "chat_id": cid} for cid in final_groups]
                supabase.table("quiz_participants").insert(participant_data).execute()

                # ج. استدعاء المحرك العالمي لبدء بث الأسئلة
                await engine_global_broadcast(final_groups, q, "الإذاعة العالمية 🌐", new_quiz_db_id)

            except Exception as db_err:
                logging.error(f"❌ خطأ قاعدة البيانات: {db_err}")
                await bot.send_message(owner_id, f"🚨 حدث خطأ أثناء التسجيل الرقمي: {db_err}")
        
        # 7. التنظيف النهائي لرسائل الإعلان
        for cid, mid in group_msgs.items():
            try: await bot.delete_message(cid, mid)
            except: pass

    except Exception as e:
        logging.error(f"🚨 General Broadcast Error: {e}")

# --- [ 1. الدوال الخدمية - الربط مع سوبابيس ] ---

async def get_user_full_data(user_id: int):
    """جلب بيانات اللاعب من جدول users_global_profile"""
    try:
        # التأكد من تحويل الـ ID لرقم صحيح ليتوافق مع BigInt
        res = supabase.table("users_global_profile").select("*").eq("user_id", int(user_id)).execute()
        return res.data[0] if res.data else None
    except Exception as e:
        logging.error(f"خطأ في جلب بيانات الجدول users_global_profile: {e}")
        return None

async def format_profile_card(user_data: dict, user_id: int):
    """
    تنسيق البطاقة الفخمة - نسخة ياسر المطورة 2026
    تم إضافة نظام الدول وتحديث الأزرار التفاعلية.
    """
    p = user_data
    ans_count = p.get('correct_answers_count', 0)
    
    # --- [ 1. منطق الرتب والتقدم ] ---
    ranks_map = [
        ("طالب مبتدئ", 100), ("طالب ثانوية", 250), ("طالب جامعي", 500),
        ("بروفيسور", 1000), ("عالم عبقري", 2000), ("أسطورة المعرفة", 5000)
    ]
    
    current_rank, next_rank_name, target_pts, prev_pts = "طالب مبتدئ", "القمة", 5000, 0
    for i, (name, limit) in enumerate(ranks_map):
        if ans_count <= limit:
            current_rank = name
            next_rank_name = ranks_map[i+1][0] if i+1 < len(ranks_map) else "القمة"
            target_pts, prev_pts = limit, (ranks_map[i-1][1] if i > 0 else 0)
            break

    percentage = min(100, max(0, ((ans_count - prev_pts) / (target_pts - prev_pts)) * 100))
    progress_bar = "🟢" * int(percentage // 10) + "⚪" * (10 - int(percentage // 10))

    # --- [ 2. معالجة البيانات المعقدة (JSON) ] ---
    def parse_json(data):
        if isinstance(data, str):
            import json
            try: return json.loads(data)
            except: return {}
        return data or {}

    stats = parse_json(p.get('category_stats'))
    cards = parse_json(p.get('cards_inventory'))
    titles = p.get('titles', []) 
    inventory = p.get('inventory', []) 

    # --- [ 3. بناء نص البطاقة النهائي ] ---
    card = f"<b>    👤 : بـروفـايـل الـمـتـمـيـز 👤</b>\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += f"🆔 <b>:</b> الاسم ⇠ <a href='tg://user?id={user_id}'>{p.get('user_name', 'مشارك جديد')}</a>\n"
    
    # التعديل الجديد: سطر الدولة والعلم
    country = p.get('country_name', 'غير محدد')
    flag = p.get('country_flag', '🌐')
    card += f"🌍 <b>:</b> الدولة ⇠ <b>{country} {flag}</b>\n"
    
    card += f"💳 <b>:</b> الحساب ⇠ <code>{p.get('bank_account', '----')}</code>\n"
    card += f"🎓 <b>:</b> الرتبة ⇠ <b>{current_rank}</b>\n"
    card += f"🎖 <b>:</b> التخصص ⇠ <b>{p.get('specialty_title', 'هاوي')}</b>\n"
    
    # --- [ بقية أقسام البطاقة كما هي ] ---
    if titles:
        card += "<b>— — — — — — — — — — — —</b>\n"
        card += "<b>👑 : الألـقـاب الـمـلـكـيـة :</b>\n"
        for title in titles:
            card += f"  ⇠ <code>{title}</code>\n"
    
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += f"📈 <b>: التقدم لـ ({next_rank_name}) :</b>\n"
    card += f"{progress_bar} <code>{int(percentage)}%</code>\n"
    card += f"🎯 <b>:</b> المتبقي ⇠ <code>{max(0, target_pts - ans_count)}</code> إجابة\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    
    card += f"💰 <b>:</b> المحفظة ⇠ <code>{p.get('wallet', 0)}</code> ن\n"
    card += f"🧠 <b>:</b> الذكاء ⇠ <code>{p.get('iq_score', 0)}% IQ</code>\n"
    card += f"🏆 <b>:</b> الفوز العام ⇠ <code>{p.get('total_wins', 0)}</code>\n"
    card += f"🔥 <b>:</b> فوز خاص ⇠ <code>{p.get('special_wins', 0)}</code>\n"
    card += f"✅ <b>:</b> الإجمالي ⇠ <code>{ans_count}</code> إجابة\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    
    card += "<b>🃏 : مـخـزن الـكـروت الـمـلـكـي :</b>\n"
    card += f"🔍 <b>:</b> كرت إظهار حرف ⇠ [ <code>{cards.get('letter', 0)}</code> ]\n"
    card += f"💡 <b>:</b> كرت التلميح الكامل ⇠ [ <code>{cards.get('full', 0)}</code> ]\n"
    card += f"⏱️ <b>:</b> كرت زيادة الوقت ⇠ [ <code>{cards.get('time', 0)}</code> ]\n"
    card += f"🎯 <b>:</b> كرت كشف الإجابة ⇠ [ <code>{cards.get('reveal', 0)}</code> ]\n"
    card += f"💰 <b>:</b> كرت المضاعفة x2 ⇠ [ <code>{cards.get('double', 0)}</code> ]\n"
    card += f"🛡️ <b>:</b> كرت حماية الدرع ⇠ [ <code>{cards.get('shield', 0)}</code> ]\n"
    card += "<b>— — — — — — — — — — — —</b>\n"

    if inventory:
        card += "<b>📦 : الـمـقـتـنـيـات والـنـوادر :</b>\n"
        for item in inventory:
            card += f"  ⇠ <code>{item}</code>\n"
        card += "<b>— — — — — — — — — — — —</b>\n"

    return card

# 1. لوحة البروفايل (الزر الرئيسي)
def get_profile_keyboard(user_id):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
       # نستخدم set_country لفتح قائمة الدول
       InlineKeyboardButton("🚩 : إضافة دولتي", callback_data=f"set_country_{user_id}"),
       InlineKeyboardButton("❌ : إغلاق", callback_data="close_card")
    )
    return keyboard

# 2. لوحة اختيار الدول
def get_countries_keyboard(user_id: int):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup(row_width=3) 
    
    countries = [
        ("اليمن", "🇾🇪"), ("السعودية", "🇸🇦"), ("مصر", "🇪🇬"), 
        ("الإمارات", "🇦🇪"), ("الكويت", "🇰🇼"), ("قطر", "🇶🇦"), 
        ("عمان", "🇴🇲"), ("البحرين", "🇧🇭"), ("العراق", "🇮🇶"), 
        ("الأردن", "🇯🇴"), ("فلسطين", "🇵🇸"), ("سوريا", "🇸🇾"), 
        ("لبنان", "🇱🇧"), ("المغرب", "🇲🇦"), ("تونس", "🇹🇳"), 
        ("الجزائر", "🇩🇿"), ("ليبيا", "🇱🇾"), ("السودان", "🇸🇩"), 
        ("الصومال", "🇸🇴"), ("موريتانيا", "🇲🇷"), ("جيبوتي", "🇩🇯"), 
        ("جزر القمر", "🇰🇲")
    ]
    
    buttons = []
    for name, flag in countries:
        # sv_c اختصار لـ save_country لتقليل حجم البيانات في الـ Callback
        callback_str = f"sv_c_{name}_{flag}_{user_id}"
        buttons.append(InlineKeyboardButton(text=f"{name} {flag}", callback_data=callback_str))
    
    keyboard.add(*buttons)
    keyboard.row(InlineKeyboardButton(text="⬅️ رجوع للبروفايل", callback_data=f"back_to_profile_{user_id}"))
    return keyboard
# ============================================================
# دالة معالجة التحويل البنكي - نسخة ياسر 2026
# ============================================================
async def process_bank_transfer(sender_id, amount, receiver_id=None, receiver_acc=None):
    try:
        # 1. جلب بيانات الراسل
        sender_res = supabase.table("users_global_profile").select("*").eq("user_id", sender_id).single().execute()
        if not sender_res.data: return "❌ ليس لديك حساب بنكي مسجل."
        
        sender = sender_res.data
        if sender['wallet'] < amount:
            return f"❌ رصيدك غير كافٍ. رصيدك الحالي: {sender['wallet']} ن"

        # 2. تحديد المستلم (إما عن طريق ID أو رقم الحساب)
        query = supabase.table("users_global_profile").select("*")
        if receiver_id:
            query = query.eq("user_id", receiver_id)
        else:
            query = query.eq("bank_account", receiver_acc)
        
        receiver_res = query.single().execute()
        if not receiver_res.data: return "❌ تعذر العثور على حساب المستلم."
        
        receiver = receiver_res.data
        if receiver['user_id'] == sender_id: return "❌ لا يمكنك التحويل لنفسك!"

        # 3. تنفيذ العملية (خصم وإضافة)
        fee = int(amount * 0.02)  # عمولة 5%
        net_amount = amount - fee

        # تحديث الراسل
        supabase.table("users_global_profile").update({"wallet": sender['wallet'] - amount}).eq("user_id", sender_id).execute()
        # تحديث المستلم
        supabase.table("users_global_profile").update({"wallet": receiver['wallet'] + net_amount}).eq("user_id", receiver['user_id']).execute()

        # 4. تنسيق قالب النجاح الفخم
        msg = f"<b>🏧 تمـت عـمـلـيـة الـتـحـويـل بـنـجـاح</b>\n"
        msg += f"<b>— — — — — — — — — — — —</b>\n"
        msg += f"👤 <b>الـمـسـتـلـم :</b> {receiver.get('user_name', 'غير معروف')}\n"
        msg += f"💳 <b>الـحـسـاب :</b> <code>#{receiver['bank_account']}</code>\n"
        msg += f"💰 <b>الـمـبـلـغ الـمـرسـل :</b> <code>{amount}</code> ن\n"
        msg += f"📉 <b>الـعـمـولـة (2%) :</b> <code>{fee}</code> ن\n"
        msg += f"✅ <b>الـصـافـي للـمـسـتـلـم :</b> <code>{net_amount}</code> ن\n"
        msg += f"<b>— — — — — — — — — — — —</b>\n"
        msg += f"✨ <i>شكراً لاستخدامك خدمات بنك زدني</i>"
        return msg

    except Exception as e:
        print(f"Transfer Error: {e}")
        return "⚠️ حدث خطأ فني أثناء التحويل."
        
# --- [ 1. قاعدة بيانات الأصناف ] ---
ITEMS_DB = {
    # --- [ 👑 الألقاب الملكية - ألقاب الهيبة ] ---
    "royal": {
        "r1": {"name": "👑 الملك", "price": 50000},
        "r2": {"name": "🎩 الإمبراطور", "price": 100000},
        "r3": {"name": "💎 الأسطورة", "price": 200000},
        "r4": {"name": "🌟 الجنرال", "price": 300000},
        "r5": {"name": "⚔️ الفارس", "price": 10000},
        "r6": {"name": "🛡️ الحارس", "price": 5000},
        "r7": {"name": "⚜️ النبيل", "price": 150000},
        "r8": {"name": "🐲 التنين", "price": 500000},
        "r9": {"name": "⚡ البرق", "price": 40000},
        "r10": {"name": "🦅 الصقر الملكي", "price": 25000},
        "r11": {"name": "🔥 البركان", "price": 18000},
        "r12": {"name": "🔱 القائد الأعلى", "price": 60000},
        "r13": {"name": "🌑 سيد الظلام", "price": 350000},
        "r14": {"name": "🦁 أسد الفلوجة", "price": 120000},
        "r15": {"name": "🐅 النمر", "price": 90000},
        "r16": {"name": "⚔️ السيف الصارم", "price": 7500},
        "r17": {"name": "🏹 القناص", "price": 6500},
        "r18": {"name": "🌌 حامي المجرة", "price": 10000},
        "r19": {"name": "🌪️ الإعصار", "price": 2200},
        "r20": {"name": "🗿 العملاق", "price": 4500},
        # (يمكنك إضافة المزيد هنا بنفس النمط حتى تصل لـ 100)
    },

    # --- [ 🌸 الألقاب البناتية - الرقة والجمال ] ---
    "girls": {
        "g1": {"name": "🎀 الأميرة", "price": 50000},
        "g2": {"name": "👑 الملكة", "price": 100000},
        "g3": {"name": "🦋 الفراشة", "price": 5000},
        "g4": {"name": "🌸 الوردة", "price": 3000},
        "g5": {"name": "🌙 قمر الزمان", "price": 150000},
        "g6": {"name": "💎 الجوهرة", "price": 250000},
        "g7": {"name": "✨ النجمة", "price": 10000},
        "g8": {"name": "🍭 السكرة", "price": 2000},
        "g9": {"name": "🎵 القيثارة", "price": 80000},
        "g10": {"name": "🌬️ النسمة", "price": 15000},
        "g11": {"name": "🧸 الدلوعة", "price": 7000},
        "g12": {"name": "🌹 الياسمينة", "price": 12000},
        "g13": {"name": "🎻 عازفة الأمل", "price": 95000},
        "g14": {"name": "🌊 حورية البحر", "price": 300000},
        "g15": {"name": "💎 الألماسة", "price": 500000},
        "g16": {"name": "🏹 الصيادة", "price": 40000},
        "g17": {"name": "❄️ ملكة الثلج", "price": 200000},
        "g18": {"name": "🍓 التوتة", "price": 5000},
        "g19": {"name": "🕊️ الحمامة", "price": 30000},
        "g20": {"name": "🧚 الجنية", "price": 180000},
    },

    # --- [ 🌹 هدايا وورود ] ---
    "gifts": {
        "rosered": {"name": "🌹 باقة ورد أحمر", "price": 1000},
        "tulip": {"name": "🌷 زهرة التوليب", "price": 1200},
        "bouquet": {"name": "💐 الباقة الملكية", "price": 5000},
        "sunflower": {"name": "🌻 إشراقة أمل", "price": 1500},
        "jasmine": {"name": "⚪ ياسمين الشام", "price": 1100},
        "choc": {"name": "🍫 صندوق شوكولا", "price": 2000},
        "giftb": {"name": "🎁 صندوق المفاجآت", "price": 3000},
        "ring": {"name": "💍 خاتم الألماس", "price": 20000},
        "perfume": {"name": "🧴 عطر فرنسي", "price": 8000},
        "watch": {"name": "⌚ ساعة رولكس", "price": 45000},
        "goldr": {"name": "💍 خاتم ذهب عيار 21", "price": 15000},
        "teddy": {"name": "🧸 دبدوب كبير", "price": 4000},
        "cake": {"name": "🎂 كيكة الاحتفال", "price": 6000},
        "iphone": {"name": "📱 آيفون 15 بروماكس", "price": 50000},
        "laptop": {"name": "💻 لابتوب قيمنق", "price": 70000},
    },

    # --- [ 🥂 رفاهية المليارديرات ] ---
    "rare": {
        "goldbar": {"name": "🧱 سبيكة ذهب", "price": 100000},
        "luxurycar": {"name": "🏎️ فيراري", "price": 500000},
        "privatejet": {"name": "🛩️ طائرة خاصة", "price": 2000000},
        "yacht": {"name": "🛥️ يخت ملكي", "price": 5000000},
        "island": {"name": "🏝️ جزيرة خاصة", "price": 10000000},
        "crowndiamond": {"name": "👑 التاج الماسي", "price": 50000000},
        "satellite": {"name": "🛰️ قمر صناعي", "price": 100000000},
        "spaceship": {"name": "🚀 سفينة فضاء", "price": 500000000},
        "pyramid": {"name": "📐 هرم خاص", "price": 150000000},
        "oilwell": {"name": "🛢️ بئر نفط", "price": 80000000},
        "footballclub": {"name": "⚽ نادي رياضي", "price": 120000000},
        "moonland": {"name": "🌑 قطعة أرض على القمر", "price": 1000000000},
        "marsbase": {"name": "🚀 قاعدة على المريخ", "price": 5000000000},
        "flat": {"name": "🏢 شقة فاخرة", "price": 30000},
        "villa": {"name": "🏡 فيلا بمسبح", "price": 150000},
        "palace": {"name": "🏰 قصر منيف", "price": 1000000},
        "tower": {"name": "🏙️ ناطحة سحاب", "price": 2000000},
        "hotel": {"name": "🏨 فندق 7 نجوم", "price": 4000000},
        "city": {"name": "🌆 مدينة كاملة", "price": 100000000},
        "stadium": {"name": "🏟️ ملعب رياضي", "price": 60000000},
        "bank": {"name": "🏦 بنك مركزي", "price": 900000000},
        "mall": {"name": "🛍️ مول تجاري", "price": 25000000},
        "museum": {"name": "🏛️ متحف تاريخي", "price": 12000000},
        "factory": {"name": "🏭 مصنع ضخم", "price": 18000000},
        "village": {"name": "🏘️ قرية سياحية", "price": 50000000},
    },
    
    # --- [ 🃏 كروت اللعب ] ---
    "cards": {
        "letter": {"name": "🔍 كرت إظهار حرف", "price": 1500},
        "full": {"name": "💡 كرت التلميح", "price": 3000},
        "time": {"name": "⏱️ كرت زيادة الوقت", "price": 2500},
        "reveal": {"name": "🎯 كرت كشف الإجابة", "price": 10000},
        "double": {"name": "💰 كرت مضاعفة المبلغ x2", "price": 7000},
        "shield": {"name": "🛡️ كرت الحماية", "price": 5000}
    },
}
# --- [ 2. دالة تنسيق واجهة المتجر ] ---
async def format_shop_bazaar_card(user_wallet: int):
    """تجهيز القالب النصي الفخم للمتجر"""
    msg =  "<b>   🛒 : الـمـتـجـر الـعـالـمـي الـكـبـيـر 🛒</b>\n"
    msg += "<b>━━━━━━━━━━━━━━━━━━</b>\n"
    msg += f"💰: <b>: رصيدك الحالي ⇠ <code>{user_wallet}</code> نقطة</b>\n"
    msg += "<b>━━━━━━━━━━━━━━━━━━</b>\n\n"
    msg += "<b>🔹 : تصفح الأقسام عبر الأزرار :</b>\n"
    msg += "👑 ⇠ ألقاب ملكية | 🌸 ⇠ ألقاب بناتي\n"
    msg += "💐 ⇠ هدايا وورود | ⚔️ ⇠ مقتنيات نادرة\n"
    msg += "🃏 ⇠ كروت مساعدة\n\n"
    msg += "<b>━━━━━━━━━━━━━━━━━━</b>\n"
    msg += "✅ : اختر القسم الذي ترغب بتصفحه بالأسفل"
    return msg
# --- [ 3. دوال الأزرار (Keyboards) المنسقة ] ---
def get_shop_main_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    # استخدمنا open_cat_ لكي يقرأها المعالج مباشرة
    # وأضفنا _ID في النهاية لحماية "البعسسة"
    keyboard.add(
        InlineKeyboardButton("👑 : الألقاب الملكية", callback_data=f"open_cat_royal_{user_id}"),
        InlineKeyboardButton("🌸 : الألقاب البناتية", callback_data=f"open_cat_girls_{user_id}")
    )
    keyboard.add(
        InlineKeyboardButton("💐 : الورود والهدايا", callback_data=f"open_cat_gifts_{user_id}"),
        InlineKeyboardButton("⚔️ : مقتنيات نادرة", callback_data=f"open_cat_rare_{user_id}")
    )
    keyboard.add(
        InlineKeyboardButton("🃏 : كروت اللعب", callback_data=f"open_cat_cards_{user_id}"),
        InlineKeyboardButton("❌ : إغلاق المتجر", callback_data=f"close_card_{user_id}")
    )
    
    return keyboard

# --- [ 3.5 دالة توليد أزرار المنتجات داخل الأقسام ] ---
def get_products_keyboard(category, user_id):
    """توليد أزرار المنتجات (الأسماء فقط) مع حماية الآيدي"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    # جلب قائمة المنتجات الخاصة بالقسم المختار
    products = ITEMS_DB.get(category, {})
    
    for p_id, p_info in products.items():
        # تعديلك المطلوب: عرض اسم السلعة فقط بدون السعر
        btn_text = f"{p_info['name']}"
        
        # داتا الزر المشفرة (تأكد أن p_id هو المفتاح البرمجي وليس الاسم الطويل)
        btn_data = f"buy_{p_id}_{category}_{user_id}"
        
        keyboard.insert(InlineKeyboardButton(btn_text, callback_data=btn_data))
    
    # زر العودة
    keyboard.add(InlineKeyboardButton("🔙 : الـعـودة لـلـقـائمة", callback_data=f"back_to_shop_{user_id}"))
    
    return keyboard
# ============================================================
# دالة إنشاء لوحة اختيار الدول العربية (22 دولة)
# الوظيفة: توليد أزرار بأسماء وأعلام الدول لتحديث بيانات البروفايل
# ============================================================
def get_countries_keyboard(user_id: int):
    keyboard = InlineKeyboardMarkup(row_width=3) 
    
    countries = [
        ("اليمن", "🇾🇪"), ("السعودية", "🇸🇦"), ("مصر", "🇪🇬"), 
        ("الإمارات", "🇦🇪"), ("الكويت", "🇰🇼"), ("قطر", "🇶🇦"), 
        ("عمان", "🇴🇲"), ("البحرين", "🇧🇭"), ("العراق", "🇮🇶"), 
        ("الأردن", "🇯🇴"), ("فلسطين", "🇵🇸"), ("سوريا", "🇸🇾"), 
        ("لبنان", "🇱🇧"), ("المغرب", "🇲🇦"), ("تونس", "🇹🇳"), 
        ("الجزائر", "🇩🇿"), ("ليبيا", "🇱🇾"), ("السودان", "🇸🇩"), 
        ("الصومال", "🇸🇴"), ("موريتانيا", "🇲🇷"), ("جيبوتي", "🇩🇯"), 
        ("جزر القمر", "🇰🇲")
    ]
    
    buttons = []
    for name, flag in countries:
        # sv_c اختصار لـ save_country لتقليل حجم البيانات
        callback_str = f"sv_c_{name}_{flag}_{user_id}"
        buttons.append(InlineKeyboardButton(text=f"{name} {flag}", callback_data=callback_str))
    
    keyboard.add(*buttons)
    
    # زر الرجوع للبروفايل
    keyboard.row(
        InlineKeyboardButton(text="⬅️ رجوع للبروفايل", callback_data=f"back_to_profile_{user_id}")
    )
    
    return keyboard
# ============================================================
# دالة عرض الألقاب والمقتنيات (الخزنة الملكية)
# الوظيفة: تنسيق قائمة الألقاب والنوادر بشكل فخم ومنظم
# ============================================================
def format_vault_display(user_name: str, titles: list, inventory: list):
    """
    تنسيق عرض المقتنيات والألقاب بشكل فخم.
    يتم عرض كل عنصر في سطر مستقل مع أيقونة مميزة.
    """
    
    # --- [ 1. ترويسة الخزنة ] ---
    display = f"<b>🏛️ : خـزنـة الـمـتـمـيـز : {user_name}</b>\n"
    display += "<b>— — — — — — — — — — — —</b>\n\n"

    # --- [ 2. قسم الألقاب الملكية ] ---
    display += "<b>👑 : الألـقـاب والـرتـب الـشـرفـيـة :</b>\n"
    if titles and len(titles) > 0:
        for title in titles:
            # استخدام كود مونو لتبريز اللقب
            display += f"  ⇠ <code>{title}</code>\n"
    else:
        display += "  ⇠ <i>لا توجد ألقاب مسجلة حالياً</i>\n"
    
    display += "\n<b>— — — — — — — — — — — —</b>\n\n"

    # --- [ 3. قسم المقتنيات والنوادر ] ---
    display += "<b>📦 : الـمـقـتـنـيـات والـمـوارد الـنـادرة :</b>\n"
    if inventory and len(inventory) > 0:
        for item in inventory:
            # إضافة أيقونة الصندوق لكل مقتنى
            display += f"  🎁 ⇠ <code>{item}</code>\n"
    else:
        display += "  ⇠ <i>المخزن فارغ تماماً</i>\n"

    # --- [ 4. التذييل ] ---
    display += "\n<b>— — — — — — — — — — — —</b>\n"
    display += "✨ <i>استمر في التميز لزيادة مقتنياتك</i>"

    return display
    
# ==========================================
# 4. حالات النظام (FSM States)
# ==========================================
class Form(StatesGroup):
    waiting_for_cat_name = State()
    waiting_for_question = State()
    waiting_for_ans1 = State()
    waiting_for_ans2 = State()
    waiting_for_new_cat_name = State()
    waiting_for_quiz_name = State()
# ==========================================
# حالات نظام التحويل البنكي (Bank FSM)
# ==========================================
class BankTransfer(StatesGroup):
    waiting_for_account = State()  # حالة انتظار رقم الحساب البنكي
    waiting_for_amount = State()   # حالة انتظار إرسال المبلغ

# ==========================================
@dp.message_handler(lambda message: message.text and (message.text.startswith('حسابي') or message.text.startswith('حسابه')))
async def get_user_bank_card(message: types.Message):
    # 1. تحديد المستهدف
    target_user = message.reply_to_message.from_user if message.text.startswith('حسابه') and message.reply_to_message else message.from_user
    
    if message.text.startswith('حسابه') and not message.reply_to_message:
        return await message.reply("⚠️ رد على رسالته أولاً!")

    status_msg = await message.reply("⏳ **جاري مراجعة سجلات بنك زدني...**", parse_mode="Markdown")

    try:
        # 2. استدعاء الدالة (تأكد أنها تعيد القيمة والبيانات الآن)
        result = await generate_zidni_card(target_user.id, bot, supabase)

        if result:
            card_image, user_db = result # تفكيك النتيجة (الصورة + البيانات)
            
            # 3. تجهيز الكابشن الفخم باستخدام بيانات القاعدة
            name = user_db.get('user_name', target_user.full_name)
            acc_num = user_db.get('bank_account', '0000')
            wallet = user_db.get('wallet', 0)
            rank = user_db.get('educational_rank', 'عضو')

            caption = (
                f"🏦 **بطاقة بنك زدني الرسمية**\n"
                f"━━━━━━━━━━━━━━\n"
                f"👤 **الاسم:** {name}\n"
                f"🎖️ **الرتبة:** {rank}\n"
                f"💰 **الرصيد:** {wallet:,} ن\n"
                f"💳 **رقم الحساب ZD:** `{acc_num}`\n"
                f"━━━━━━━━━━━━━━\n"
                f"✨ **حالة الحساب:** نشط ✅"
            )

            await status_msg.delete()
            await message.answer_photo(
                photo=card_image,
                caption=caption,
                parse_mode="Markdown"
            )
        else:
            await status_msg.edit_text("❌ هذا المستخدم غير مسجل في سجلاتنا.")

    except Exception as e:
        print(f"❌ Error: {e}")
        await status_msg.edit_text("⚠️ عذراً، حدث خطأ فني غير متوقع.")
            

# ==========================================
# 2️⃣ المعالج الرئيسي للأوامر (عني، رتبتي، إلخ)
# ==========================================
@dp.message_handler(lambda m: m.text in ["عني", "رتبتي", "نقاطي", "محفظتي", "بروفايلي"])
@dp.message_handler(lambda m: m.reply_to_message and m.text in ["عنه", "رتبته", "نقاطه", "محفظته", "بروفايله"])
async def cmd_show_profile_global(message: types.Message):
    # 1. تحديد الهدف (أنا أو الشخص الذي تم الرد عليه)
    target = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid = target.id

    # رسالة مؤقتة
    status = await message.reply("⏳ <b>جاري سحب البيانات من السجل العالمي...</b>", parse_mode="HTML")

    # 2. جلب البيانات من السجل (تأكد أن الدالة get_user_full_data تجلب الحقول الجديدة)
    user_data = await get_user_full_data(uid)
    
    if not user_data:
        await status.delete()
        msg = "❌ هذا المستخدم غير مسجل عالمياً." if message.reply_to_message else "❌ ليس لديك سجل عالمي بعد!"
        return await message.reply(msg)

    # 3. تنسيق البطاقة (التي عدلناها لتكون الألقاب والمقتنيات في أسطر)
    profile_text = await format_profile_card(user_data, uid)
    
    # 4. جلب الكيبورد (تم تمرير uid ليعمل المتجر لصاحب البروفايل)
    keyboard = get_profile_keyboard(uid) 
    
    # 5. محاولة جلب صورة البروفايل
    photo_id = None
    try:
        photos = await bot.get_user_profile_photos(uid, limit=1)
        if photos.total_count > 0:
            photo_id = photos.photos[0][-1].file_id
    except: 
        pass

    await status.delete() # حذف رسالة "جاري السحب"
    
    # 6. إرسال البروفايل (صورة أو نص)
    final_msg = None
    if photo_id:
        final_msg = await message.answer_photo(
            photo_id, 
            caption=profile_text, 
            parse_mode="HTML", 
            reply_markup=keyboard
        )
    else:
        final_msg = await message.answer(
            profile_text, 
            parse_mode="HTML", 
            reply_markup=keyboard
        )

    # 7. 🔥 نظام التدمير الذاتي (الحذف بعد دقيقة)
    await asyncio.sleep(60) # الانتظار لمدة 60 ثانية
    try:
        await final_msg.delete() # حذف البروفايل
        await message.delete()   # حذف أمر المستخدم لتنظيف المجموعة
    except:
        pass # في حال تم حذفها يدوياً من قبل مشرف
# ==========================================
# 6. معالج أمر البدء المطور في الخاص /start
# ==========================================
@dp.message_handler(commands=['start'], chat_type=types.ChatType.PRIVATE)
async def private_start_handler(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    
    # لوحة الأزرار التفاعلية
    kb_start = InlineKeyboardMarkup(row_width=2)
    kb_start.add(
        InlineKeyboardButton("👤 ملفي الشخصي", callback_data=f"my_profile_{user_id}"),
        InlineKeyboardButton("🛒 المتجر الملكي", callback_data=f"open_shop_{user_id}")
    )
    kb_start.add(
        InlineKeyboardButton("👨‍💻 المطور: ياسر", url="https://t.me/Ya_79k"),
        InlineKeyboardButton("📢 قناة البوت", url="https://t.me/YourChannel")
    )
    kb_start.add(
        InlineKeyboardButton("➕ أضفني لمجموعتك الآن", url=f"https://t.me/{(await bot.get_me()).username}?startgroup=true")
    )

    welcome_msg = (
        f"👋 <b>أهلاً بك يا {first_name} في عالم التحدي!</b>\n\n"
        f"أنا بوت المسابقات الأكثر ذكاءً، استعد لاختبار معلوماتك.\n"
        f"    ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
        f"📜 <b>أوامر المستخدمين (في المجموعات او دردشة البوت):</b>\n"
        f"🔹 <code>الاوامر </code> : لعرض الأوامر و اعدادات البوت.\n"
        f"🔹 <code>حسابي</code> : لعرض بطاقة حسابك البنكي بعد لعب مسابقة.\n"
        f"🔸 <code>تحكم</code> : لطلب تشغيل البوت في المجموعة.\n"
        f"🔹 <code>مسابقة</code> : لعرض لوحة مسابقاتك وتشغيلها.\n"
        f"🔸 <code>انسحاب</code> : لايقاف المسابقة.\n"
        f"🔹 <code>عني</code> : لعرض بطاقتك الشخصية ومقتنياتك.\n"
        f"🔹 <code>ممتلكاتي</code> : لعرض ألقابك و مقتنياتك كلها.\n"
        f"🔹 <code>توب</code> : لعرض قائمة توب الأغنياء والعلماء.\n\n"
        f"    ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
        f"🛠️ <b>أوامر الإدارة (للمشرفين فقط):</b>\n"
        f"🔸 <code>تفعيل</code> : لطلب تشغيل البوت في المجموعة.\n"
        f"🔸 <code>تحكم</code> : لضبط إعدادات المسابقة والوقت.\n"
        f"🔸 <code>مسابقة</code> : لبدء تشغيل المسابقات في المجموعة.\n"
        f"🔸 <code>انسحاب</code> : لايقاف المسابقة العامة.\n"
        f"🔸 <code>كتم</code> : لطرد المشاغبين من المجموعه.\n"
        f"🔸 <code>تعطيل او قفل الايدي</code> : لإغلاق الملف الشخصي.\n"
        f"🔸 <code>تفعيل او قفل الحماية</code> : لحماية المجموعه من المبعسسين .\n"
        f"🔸 <code>رفع</code> : لرفع رتب الاعضاء لاستخدام البوت.\n"
        f"    ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
        f"💎 <b>ميزة خاصة:</b> يمكنك جمع النقاط واستبدالها بجوائز حقيقية داخل المتجر!\n\n"
        f"💬 <b>للتواصل المباشر مع المطور (ياسر):</b>\n"
        f"اضغط هنا: @Ya_79k"
    )

    try:
        # Photo ID الخاص بصورة الترحيب (يفضل صورة فخمة للبوت)
        bot_photo = "AgACAgQAAxkBAA..." 
        await message.answer_photo(
            photo=bot_photo,
            caption=welcome_msg,
            reply_markup=kb_start,
            parse_mode="HTML"
        )
    except:
        await message.answer(welcome_msg, reply_markup=kb_start, parse_mode="HTML")
        
# ==========================================
# 5. الترحيب التلقائي بصورة البوت
# ==========================================
@dp.message_handler(content_types=types.ContentTypes.NEW_CHAT_MEMBERS)
async def welcome_bot_to_group(message: types.Message):
    for member in message.new_chat_members:
        if member.id == (await bot.get_me()).id:
            group_name = message.chat.title
            
            kb_welcome = InlineKeyboardMarkup(row_width=1)
            kb_welcome.add(
                InlineKeyboardButton("👑 مبرمج البوت (ياسر)", url="https://t.me/Ya_79k")
            )

            welcome_text = (
                f"👋 <b>أهلاً بكم في عالم المسابقات!</b>\n"
                f"تمت إضافتي بنجاح في: <b>{group_name}</b>\n"
                f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
                f"🤖 <b>أنا بوت المسابقات الذكي (Questions Bot).</b>\n\n"
                f"🛠️ <b>كيفية البدء:</b>\n"
                f"يجب على المشرف كتابة أمر (تفعيل) لإرسال طلب للمطور.\n\n"
                f"📜 <b>الأوامر الأساسية:</b>\n"
                f"🔹 <b>تفعيل :</b> لطلب تشغيل البوت.\n"
                f"🔹 <b>تحكم :</b> لوحة الإعدادات (للمشرفين).\n"
                f"🔹 <b>مسابقة :</b> لبدء جولة أسئلة.\n"
                f"🔹 <b>عني :</b> لعرض ملفك الشخصي ونقاطك.\n"
                f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
                f"📢 <i>اكتب (تفعيل) الآن لنبدأ الرحلة!</i>"
            )

            try:
                # ضع الـ File ID الذي حصلت عليه من @FileIdBot هنا
                bot_photo_id = "AgACAgQAAxkBAA..." # استبدل هذا بالكود الذي سيعطيك إياه البوت
                await message.answer_photo(
                    photo=bot_photo_id, 
                    caption=welcome_text, 
                    reply_markup=kb_welcome, 
                    parse_mode="HTML"
                )
            except:
                # في حال لم تضع الآيدي بعد أو حدث خطأ، يرسل نصاً فقط
                await message.answer(welcome_text, reply_markup=kb_welcome, parse_mode="HTML")

# ============================================================
# هاندلر عرض المقتنيات والنوادر (شخصي + للآخرين)
# الأوامر: مقتنياتي، ممتلكاتي، مقتنياته، ممتلكاته
# ============================================================
@dp.message_handler(lambda message: message.text and any(word in message.text for word in ["مقتنياتي", "ممتلكاتي", "مقتنياته", "ممتلكاته", "مقتنياتة", "ممتلكاتة"]))
async def show_user_assets(message: types.Message):
    # 1. تحديد الشخص المستهدف (أنا أم الشخص الذي رددت عليه؟)
    if any(word in message.text for word in ["مقتنياته", "ممتلكاته", "مقتنياتة", "ممتلكاتة"]):
        if message.reply_to_message:
            target_user = message.reply_to_message.from_user
        else:
            return await message.reply("⚠️ يجب أن ترد على رسالة العضو لعرض مقتنياته!")
    else:
        target_user = message.from_user

    status_msg = await message.reply("⏳ **جاري فتح الخزنة الملكية...**")

    try:
        # 2. جلب البيانات من الجدول الخاص بك
        res = supabase.table("users_global_profile").select("*").eq("user_id", target_user.id).execute()
        
        if not res.data:
            await status_msg.edit_text("❌ هذا العضو غير مسجل في سجلاتنا البنكية.")
            return

        p = res.data[0]
        
        # 3. معالجة بيانات JSON (titles و inventory)
        # بما أن الأعمدة في جدولك هي jsonb، سنتعامل معها كقوائم
        titles = p.get('titles') or []
        inventory = p.get('inventory') or []
        user_name = p.get('user_name', target_user.full_name)

        # 4. استخدام قالب التنسيق الفخم
        vault_text = format_vault_display(user_name, titles, inventory)

        # 5. عرض النتيجة مع زر إغلاق
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("❌ إغلاق الخزنة", callback_data="close_card"))

        await status_msg.delete()
        await message.answer(vault_text, reply_markup=keyboard, parse_mode="HTML")

    except Exception as e:
        print(f"Error in Vault Handler: {e}")
        await status_msg.edit_text("⚠️ عذراً، حدث خطأ أثناء فحص المقتنيات.")

# ============================================================
# نظام التحويل البنكي المتطور - بنك زدني 2026
# الوظيفة: إدارة عمليات التحويل عبر الرد (Reply) أو رقم الحساب
# ============================================================

# 1. حالة التحويل بالرد (الطريقة السريعة)
@dp.message_handler(lambda message: message.text == "تحويل" and message.reply_to_message)
async def transfer_by_reply(message: types.Message, state: FSMContext):
    receiver = message.reply_to_message.from_user
    
    # حفظ بيانات المستلم في الذاكرة المؤقتة
    await state.update_data(target_id=receiver.id, target_name=receiver.full_name)
    
    # الانتقال لخطوة طلب المبلغ مباشرة
    await BankTransfer.waiting_for_amount.set()
    await message.reply(
        f"👤 <b>الـمـسـتـلـم:</b> {receiver.full_name}\n"
        f"💰 <b>أرسل الآن المبلغ المراد تحويله:</b>",
        parse_mode="HTML"
    )

# ------------------------------------------------------------
# 2. حالة التحويل عبر الأمر (طلب رقم الحساب)
@dp.message_handler(lambda message: message.text == "تحويل" and not message.reply_to_message)
async def transfer_by_acc(message: types.Message):
    # الانتقال لحالة انتظار رقم الحساب
    await BankTransfer.waiting_for_account.set()
    await message.reply(
        "🏦 <b>نظام التحويل البنكي</b>\n"
        "يرجى إرسال <b>رقم الحساب البنكي</b> للشخص المراد التحويل له:",
        parse_mode="HTML"
    )

# ------------------------------------------------------------

# 3. استقبال رقم الحساب والتحقق منه
@dp.message_handler(state=BankTransfer.waiting_for_account)
async def get_acc_num(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.reply("⚠️ يرجى إرسال رقم حساب صحيح (أرقام فقط).")
    
    # حفظ رقم الحساب والانتقال لطلب المبلغ
    await state.update_data(target_acc=int(message.text))
    await BankTransfer.waiting_for_amount.set()
    await message.reply("💰 <b>رقم الحساب صحيح.</b>\nالآن أرسل <b>المبلغ</b> المراد تحويله:")

# ------------------------------------------------------------

# 4. المرحلة النهائية: استقبال المبلغ وتنفيذ التحويل
@dp.message_handler(state=BankTransfer.waiting_for_amount)
async def finalize_transfer(message: types.Message, state: FSMContext):
    # التأكد أن المدخل رقم
    if not message.text.isdigit():
        return await message.reply("⚠️ يرجى إرسال مبلغ صحيح (أرقام فقط).")
    
    amount = int(message.text)
    if amount < 10: 
        return await message.reply("⚠️ الحد الأدنى للتحويل هو 10 ن.")
    
    # جلب البيانات المخزنة (سواء كان المستلم بالرد أو برقم الحساب)
    data = await state.get_data()
    sender_id = message.from_user.id
    
    # استدعاء دالة المعالجة التي تتواصل مع Supabase
    result_msg = await process_bank_transfer(
        sender_id=sender_id,
        amount=amount,
        receiver_id=data.get('target_id'),
        receiver_acc=data.get('target_acc')
    )
    
    # إرسال نتيجة العملية وإنهاء الحالة
    await message.answer(result_msg, parse_mode="HTML")
    await state.finish()

# ============================================================
# هاندلر فتح قائمة الدول عند الضغط على زر "إضافة دولتي"
# الوظيفة: استبدال لوحة البروفايل بلوحة اختيار الدول
# ============================================================
@dp.callback_query_handler(lambda c: c.data.startswith('set_country_'))
async def show_countries_list(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split("_")[-1])
    
    # التأكد أن الشخص الذي ضغط هو صاحب البروفايل
    if callback_query.from_user.id != user_id:
        return await callback_query.answer("⚠️ هذه اللوحة ليست لك!", show_alert=True)

    await callback_query.message.edit_text(
        "🌍 **اختر دولتك من القائمة أدناه:**",
        reply_markup=get_countries_keyboard(user_id),
        parse_mode="Markdown"
    )
    await callback_query.answer()

# معالج زر "الرجوع للبروفايل"
@dp.callback_query_handler(lambda c: c.data.startswith('back_to_profile_'))
async def back_to_profile_handler(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split("_")[-1])
    
    # هنا تعيد استدعاء دالة عرض البروفايل الأصلية الخاصة بك
    # سأفترض أن لديك دالة تجلب نص البروفايل
    profile_text = "🪪 **لوحة التحكم الخاصة بك**" 
    
    await callback_query.message.edit_text(
        profile_text,
        reply_markup=get_profile_keyboard(user_id),
        parse_mode="Markdown"
    )
    await callback_query.answer()
    
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('sv_c_'))
async def process_save_country(callback_query: types.CallbackQuery):
    # استخدام split مع تحديد الأقسام لضمان الدقة
    data = callback_query.data.split('_')
    
    # التقسيم الصحيح بناءً على هيكلة sv_c_{name}_{flag}_{user_id}
    country_name = data[2]
    country_flag = data[3]
    user_id = int(data[4])

    # 1. التحديث في Supabase
    try:
        supabase.table("users_global_profile").update({
            "country_name": country_name,
            "country_flag": country_flag
        }).eq("user_id", user_id).execute()
        
        # 2. إشعار النجاح
        await callback_query.answer(f"تم تحديث دولتك إلى: {country_name} {country_flag} ✅", show_alert=False)
        
        # 3. تحديث رسالة البروفايل فوراً ليرى المستخدم التغيير
        res = supabase.table("users_global_profile").select("*").eq("user_id", user_id).single().execute()
        if res.data:
            from bot_file import format_profile_card, get_profile_keyboard # استيراد دوالك
            
            new_text = await format_profile_card(res.data, user_id)
            new_kb = get_profile_keyboard(user_id)
            
            await callback_query.message.edit_text(
                text=new_text,
                reply_markup=new_kb,
                parse_mode="HTML"
            )
            
    except Exception as e:
        print(f"Error: {e}")
        await callback_query.answer("⚠️ حدث خطأ أثناء الحفظ، حاول مجدداً.")
        
# ==========================================
# --- [ 4. محرك التنقل المنسق والمحمي ] ---
@dp.callback_query_handler(lambda c: c.data.startswith(('open_cat_', 'back_to_shop_', 'close_card_')), state="*")
async def shop_navigation_handler(call: types.CallbackQuery):
    data = call.data
    user_id = call.from_user.id
    
    # تقسيم البيانات بدقة
    # إذا كانت: open_cat_royal_123456
    # فالتقسيم سيكون: ['open', 'cat', 'royal', '123456']
    parts = data.split('_')
    owner_id = int(parts[-1]) # الأخير دائماً هو الآيدي

    # 🛡️ حارس البعسسة
    if user_id != owner_id:
        return await call.answer("🚫 : المتجر ليس لك!", show_alert=True)

    try:
        # 1. إغلاق المتجر
        if "close_card" in data:
            await call.message.delete()

        # 2. العودة للقائمة الرئيسية للمتجر
        elif "back_to_shop" in data:
            await call.message.edit_reply_markup(reply_markup=get_shop_main_keyboard(owner_id))
            await call.answer("🔙 : القائمة الرئيسية")

        # 3. فتح قسم (الملكية، البنات، إلخ)
        elif "open_cat_" in data:
            # نأخذ العضو الثالث في المصفوفة وهو اسم القسم
            category = parts[2] 
            
            # استدعاء دالة المنتجات (تأكد أنها تقبل متغيرين: القسم والآيدي)
            kb = get_products_keyboard(category, owner_id)
            await call.message.edit_reply_markup(reply_markup=kb)
            await call.answer(f"📂 : قسم {category}")

    except Exception as e:
        import logging
        logging.error(f"Shop Error: {e}")
        # إذا حصل خطأ، سنطبع السبب الحقيقي في الكونسول لنعرفه
        await call.answer(f"❌ : خطأ برمي: {e}")
# ==========================================
# ................. الشراء....................
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith('buy_'), state="*")
async def handle_purchase_confirmation(call: types.CallbackQuery):
    user_id = call.from_user.id
    parts = call.data.split('_')
    item_id, category, owner_id = parts[1], parts[2], int(parts[3])

    if user_id != owner_id:
        return await call.answer("🚫 : المتجر ليس لك!", show_alert=True)

    product = ITEMS_DB.get(category, {}).get(item_id)
    if not product: return await call.answer("⚠️ : المنتج غير متوفر!")

    item_name = product['name']
    price = product['price']

    # كيبورد التأكيد (نعم و تراجع فقط)
    confirm_kb = InlineKeyboardMarkup(row_width=2)
    confirm_kb.add(
        InlineKeyboardButton("✅ نعم، شراء", callback_data=f"confbuy_{item_id}_{category}_{user_id}"),
        InlineKeyboardButton("🔙 تراجع", callback_data=f"open_cat_{category}_{user_id}")
    )

    confirm_text = (
        f"<b>🛒 تأكيد عملية الشراء</b>\n"
        f"  — — — — — — — — — —\n"
        f"📦 السلعة: <b>{item_name}</b>\n"
        f"💰 الثمن: <code>{price}</code> ن\n"
        f"  — — — — — — — — — —\n"
        f"⚠️ هل أنت متأكد من رغبتك في الشراء؟"
    )

    await call.message.edit_text(confirm_text, reply_markup=confirm_kb, parse_mode="HTML")

# --- [ معالج التنفيذ الفعلي بعد الضغط على نعم ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('confbuy_'), state="*")
async def execute_actual_purchase(call: types.CallbackQuery):
    user_id = call.from_user.id
    parts = call.data.split('_')
    item_id, category, owner_id = parts[1], parts[2], int(parts[3])

    product = ITEMS_DB.get(category, {}).get(item_id)
    price = product['price']
    item_name = product['name']

    # جلب بيانات المستخدم
    res = supabase.table("users_global_profile").select("*").eq("user_id", user_id).execute()
    user_data = res.data[0]
    wallet = user_data.get('wallet', 0)

    if wallet < price:
        return await call.answer(f"❌ رصيدك {wallet}ن لا يكفي!", show_alert=True)

    # تجهيز المخازن
    current_titles = user_data.get('titles') or []
    current_inventory = user_data.get('inventory') or []
    current_cards = user_data.get('cards_inventory') or {}

    update_payload = {"wallet": wallet - price}

    # --- [ منطق التوزيع الذكي ] ---
    if category == "cards":
        # إضافة الكروت لـ cards_inventory بالعدد
        current_cards[item_id] = current_cards.get(item_id, 0) + 1
        update_payload["cards_inventory"] = current_cards
    
    elif category in ["gifts", "rare", "estates"]:
        # إضافة المقتنيات لـ inventory
        if item_name in current_inventory:
            return await call.answer(f"📦 تملك {item_name} مسبقاً!", show_alert=True)
        current_inventory.append(item_name)
        update_payload["inventory"] = current_inventory
        
    else: # الألقاب (royal, girls)
        # إضافة الألقاب لـ titles
        if item_name in current_titles:
            return await call.answer(f"👑 تملك لقب {item_name} مسبقاً!", show_alert=True)
        current_titles.append(item_name)
        update_payload["titles"] = current_titles

    # حفظ في سوبابيس
    supabase.table("users_global_profile").update(update_payload).eq("user_id", user_id).execute()

    # رسالة طائرة في نصف الشاشة
    await call.answer(f"🎉 مبروك! اشتريت {item_name}\nرصيدك المتبقي: {wallet-price}ن", show_alert=True)

    # العودة للمتجر الرئيسي
    new_text = f"✨ <b>تمت العملية بنجاح!</b>\nمحفظتك الآن: <code>{wallet-price}</code> ن"
    await call.message.edit_text(new_text, reply_markup=get_shop_main_keyboard(user_id), parse_mode="HTML")
    
# ============================================================
# هاندلر استدعاء لوحة المطور (لوحتي، المطور، غرفة العمليات)
# ============================================================
@dp.message_handler(lambda message: message.text in ['لوحتي', 'المطور', 'غرفتي', 'غرفة العمليات', 'الإدارة'], chat_type=types.ChatType.PRIVATE)
async def admin_dashboard_trigger(message: types.Message):
    """
    استدعاء لوحة التحكم الخاصة بالمطور مع التحقق من الهوية (مثل نظام التفعيل)
    """
    user_id = message.from_user.id

    # التحقق الصارم: هل المستخدم هو المطور؟
    if user_id != ADMIN_ID:
        return await message.reply("⚠️ <b>عذراً، هذه اللوحة مخصصة لمطور النظام فقط.</b>", parse_mode="HTML")

    try:
        # 1. جلب البيانات الحية من سوبابيس (إحصائيات المجموعات)
        res = supabase.table("groups_hub").select("*").execute()
        
        # تصنيف البيانات لجعل اللوحة "حية"
        active = len([g for g in res.data if g['status'] == 'active'])
        blocked = len([g for g in res.data if g['status'] == 'blocked'])
        pending = len([g for g in res.data if g['status'] == 'pending'])
        total_points = sum([g.get('total_group_score', 0) for g in res.data])

        # 2. تصميم النص الفخم لغرفة العمليات (نفس أسلوب لوحة التحكم)
        txt = (
            "👑 <b>غرفة العمليات الرئيسية</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"✅ المجموعات النشطة : <b>{active}</b>\n"
            f"🚫 المجموعات المحظورة : <b>{blocked}</b>\n"
            f"⏳ طلبات معلقة : <b>{pending}</b>\n"
            f"🏆 إجمالي نقاط الهب : <b>{total_points:,}</b>\n"
            "━━━━━━━━━━━━━━\n"
            "👇 <b>أهلاً بك يا مطور، اختر قسماً لإدارته :</b>"
        )
        
        # 3. إرسال اللوحة مع الكيبورد الرئيسي الخاص بك
        await message.answer(
            txt, 
            reply_markup=get_main_admin_kb(), 
            parse_mode="HTML"
        )

    except Exception as e:
        logging.error(f"Admin Dashboard Error: {e}")
        await message.answer("❌ <b>حدث خطأ أثناء الاتصال بقاعدة البيانات الموحدة.</b>", parse_mode="HTML")

# =========================================
# 6. أمر التفعيل (Request Activation)
# =========================================
@dp.message_handler(lambda m: m.text == "تفعيل", chat_type=[types.ChatType.GROUP, types.ChatType.SUPERGROUP])
async def activate_group_hub(message: types.Message):
    user_id = message.from_user.id
    chat_member = await message.chat.get_member(user_id)
    
    if not (chat_member.is_chat_admin() or user_id == ADMIN_ID):
        return await message.reply("⚠️ هذا الأمر مخصص لمشرفي القروب فقط.")

    group_id = message.chat.id
    group_name = message.chat.title

    try:
        res = supabase.table("groups_hub").select("*").eq("group_id", group_id).execute()
        
        if res.data:
            status = res.data[0]['status']
            if status == 'active':
                return await message.reply("🛡️ القروب مفعل مسبقاً وجاهز للعمل!", parse_mode="HTML")
            elif status == 'pending':
                return await message.reply("⏳ طلبكم قيد المراجعة، انتظر موافقة المطور.", parse_mode="HTML")
            elif status == 'blocked':
                return await message.reply("🚫 هذا القروب محظور من قبل المطور.", parse_mode="HTML")
        
        # إدخال القروب في pending
        supabase.table("groups_hub").insert({
            "group_id": group_id,
            "group_name": group_name,
            "status": "pending",
            "total_group_score": 0
        }).execute()

        # إشعار المطور
        kb_fast_action = InlineKeyboardMarkup(row_width=2).add(
            InlineKeyboardButton("✅ موافقة", callback_data=f"auth_approve_{group_id}"),
            InlineKeyboardButton("🚫 رفض وحظر", callback_data=f"auth_block_{group_id}")
        )
        await bot.send_message(ADMIN_ID, 
            f"🔔 طلب تفعيل جديد!\n"
            f"👥 القروب: {group_name}\n"
            f"🆔 {group_id}\n"
            f"اتخذ قرارك الآن:", 
            reply_markup=kb_fast_action, 
            parse_mode="HTML")

        # إشعار القروب
        await message.reply("✅ تم إرسال طلب التفعيل، انتظر موافقة المطور.", parse_mode="HTML")

    except Exception as e:
        logging.error(f"Activation Error: {e}")
        await message.reply("❌ حدث خطأ تقني في قاعدة البيانات.")

# ==========================================
# 2. تعديل أمر "تحكم" لضمان عدم العمل إلا بعد التفعيل
# ==========================================
@dp.message_handler(lambda m: m.text == "تحكم")
async def control_panel(message: types.Message):
    user_id = message.from_user.id
    group_id = message.chat.id

    # في المجموعات، نتحقق من حالة التفعيل
    if message.chat.type != 'private':
        # إذا لم يكن المطور، نتحقق من حالة القروب
        if user_id != ADMIN_ID:
            status = await get_group_status(group_id)
            if status != "active":
                return await message.reply("⚠️ <b>هذا القروب غير مفعل.</b>\nيجب أن يوافق المطور على طلب التفعيل أولاً.", parse_mode="HTML")
            
            # فحص هل المستخدم مشرف
            member = await bot.get_chat_member(group_id, user_id)
            if not (member.is_chat_admin() or member.is_chat_creator()):
                return await message.reply("⚠️ لوحة التحكم مخصصة للمشرفين فقط.")

    # إذا كان المطور أو قروب مفعل، تظهر اللوحة
    txt = (f"👋 أهلاً بك في لوحة الإعدادات\n"
           f"👑 المطور: <b>{OWNER_USERNAME}</b>")
    
    await message.answer(txt, reply_markup=get_main_control_kb(user_id), parse_mode="HTML")

# ============================================================
# هاندلر استدعاء المتجر (المتجر، متجر، أوامر المتجر)
# ============================================================
@dp.message_handler(lambda message: message.text in ['المتجر', 'متجر'] or message.text.startswith('/shop'))
async def cmd_open_shop_bazaar(message: types.Message):
    """
    استدعاء متجر البوت مع عرض الرصيد والحذف التلقائي
    """
    user_id = message.from_user.id
    
    try:
        # 1. جلب بيانات المستخدم (المحفظة) من سوبابيس
        res = supabase.table("users_global_profile").select("wallet").eq("user_id", user_id).execute()
        
        if not res.data:
            return await message.reply("⚠️ يجب أن يكون لديك حساب مسجل لفتح المتجر.")
        
        wallet = res.data[0].get('wallet', 0)
        
        # 2. تجهيز النص الفخم (استدعاء دالة التنسيق الخاصة بك)
        shop_text = await format_shop_bazaar_card(wallet)
        
        # 3. إرسال المتجر مع كيبورد الأقسام وحماية صاحب الطلب (owner_id)
        sent_shop = await message.reply(
            shop_text,
            reply_markup=get_shop_main_keyboard(user_id), # تمرير الـ ID للحماية
            parse_mode="HTML"
        )
        
        # 4. ميزة التطهير التلقائي (حذف المتجر بعد 60 ثانية لتقليل الزحام)
        await asyncio.sleep(60)
        try:
            await sent_shop.delete()
            await message.delete() # حذف كلمة "متجر" التي أرسلها المستخدم
        except:
            pass

    except Exception as e:
        logging.error(f"Shop Error: {e}")
        await message.reply("❌ المتجر مغلق حالياً للصيانة، حاول لاحقاً.")

# ============================================================
# معالج العودة للمتجر (Callback)
# ============================================================
@dp.callback_query_handler(lambda c: c.data.startswith("back_to_shop_"), state="*")
async def back_to_shop_handler(c: types.CallbackQuery):
    owner_id = int(c.data.split("_")[-1])
    
    # حماية: المشتري فقط من يمكنه التحكم
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ هذا المتجر ليس لك! اطلب متجرك الخاص بكتابة 'متجر'.", show_alert=True)

    # تحديث المحفظة قبل العودة
    res = supabase.table("users_global_profile").select("wallet").eq("user_id", owner_id).execute()
    wallet = res.data[0].get('wallet', 0)
    
    shop_text = await format_shop_bazaar_card(wallet)
    
    await c.message.edit_text(
        shop_text,
        reply_markup=get_shop_main_keyboard(owner_id),
        parse_mode="HTML"
    )
    await c.answer("🔙 عدنا لساحة ")
    
# التعديل في السطر 330 (أضفنا close_bot_)
@dp.callback_query_handler(lambda c: c.data.startswith(('custom_add_', 'dev_', 'setup_quiz_', 'dev_leaderboard_', 'close_bot_', 'back_', 'open_shop_')), state="*")
async def handle_control_buttons(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    action = data_parts[0] 
    owner_id = int(data_parts[-1])

    # 🛑 [ الأمان ]
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تلمس أزرار غيرك! 😂", show_alert=True)

    # 1️⃣ [ زر الإغلاق ] - فحص الكلمة بالكامل أو أول جزء
    if action == "close":
        await c.answer("تم إغلاق اللوحة ✅")
        return await c.message.delete()

    # 2️⃣ [ زر الرجوع ] - النسخة المصلحة (التعديل بدل الإرسال)
    elif action == "back":
        await state.finish()
        await c.answer("🔙 جاري العودة...")
        # بدلاً من استدعاء control_panel التي ترسل رسالة جديدة، نعدل الرسالة الحالية
        return await c.message.edit_text(
            f"👋 **أهلاً بك في لوحة التحكم الرئيسية**\n\nاختر من الأسفل ما تود القيام به:",
            reply_markup=get_main_control_kb(owner_id), # تأكد من وضع دالة الكيبورد الرئيسي هنا
            parse_mode="Markdown"
        )

    # 3️⃣ [ زر إضافة خاصة ]
    elif action == "custom":
        await c.answer()
        # التعديل هنا: يجب أن يكون السطر القادم تحت elif مباشرة (4 مسافات)
        return await custom_add_menu(c, state=state)

    # 4️⃣ [ زر تجهيز المسابقة ]
    elif action == "setup":
        await c.answer()
        keyboard = get_setup_quiz_kb(owner_id)
        return await c.message.edit_text(
            "🏆 **مرحباً بك في معمل تجهيز المسابقات!**\n\nمن أين تريد جلب الأسئلة لمسابقتك؟",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        # 5️⃣ [ محرك فتح المتجر العالمي ] 🛒
    elif action == "open" and "shop" in data_parts:
        await c.answer("💰 جاري فتح المتجر الملكي...")
        
        # 1. جلب رصيد المستخدم من سوبابيس (أو وضعه 0 كاحتياط)
        try:
            res = supabase.table("users_global_profile").select("wallet").eq("user_id", owner_id).execute()
            wallet = res.data[0]['wallet'] if res.data and len(res.data) > 0 else 0
        except Exception as e:
            print(f"Error fetching wallet: {e}")
            wallet = 0 
            
        # 2. تجهيز النص الفخم (تأكد من وجود دالة format_shop_bazaar_card)
        shop_text = await format_shop_bazaar_card(wallet)
        
        # 3. تحديث الكيبورد واستدعاء دالة الأقسام
        # أضفنا owner_id لكي تمر الحماية للأزرار التالية
        return await c.message.edit_text(
            shop_text,
            reply_markup=get_shop_main_keyboard(owner_id), 
            parse_mode="HTML"
        )
        # 6️⃣ [ محرك فتح لوحة الصدارة العالمية ] 🏆
    elif action == "dev" and "leaderboard" in data_parts:
        await c.answer("🏆 جاري فتح سجلات الشرف...")
        
        try:
            # 1. استدعاء الدالة التي تجهز النص الفخم والكيبورد
            # (تأكد من وجود الدالة التي صممناها سابقاً في ملفك)
            leaderboard_text, leaderboard_kb = get_leaderboard_main_message()
            
            # 2. تحديث الرسالة الحالية بلوحة الصدارة
            await c.message.edit_text(
                text=leaderboard_text,
                reply_markup=leaderboard_kb,
                parse_mode="HTML"
            )
            
        except Exception as e:
            print(f"Error opening leaderboard: {e}")
            await c.answer("⚠️ عذراً، تعذر فتح لوحة الصدارة حالياً.", show_alert=True)
            
# --- [ 4. محرك التنقل بين أقسام المتجر ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('open_cat_') or c.data in ['back_to_shop', 'close_card'])
async def shop_navigation_handler(call: types.CallbackQuery):
    user_id = call.from_user.id
    data = call.data

    # 🛡️ حارس البعسسة: التأكد أن الضاغط هو صاحب الطلب
    if call.message.reply_to_message and call.message.reply_to_message.from_user.id != user_id:
        return await call.answer("🚫 : المتجر ليس لك يا شريك! اطلب /متجر خاص بك.", show_alert=True)

    try:
        # أ. إغلاق المتجر
        if data == "close_card":
            await call.message.delete()
            await call.answer("✅ : تم إغلاق المتجر")

        # ب. العودة للقائمة الرئيسية
        elif data == "back_to_shop":
            await call.message.edit_reply_markup(reply_markup=get_shop_main_keyboard())
            await call.answer("🔙 : العودة للقائمة الرئيسية")

        # ج. فتح قسم محدد (الملكية، البنات، إلخ)
        elif data.startswith("open_cat_"):
            category = data.replace("open_cat_", "")
            
            # فحص إذا كان القسم موجوداً في مصفوفتنا ITEMS_DB
            if category in ITEMS_DB:
                await call.message.edit_reply_markup(reply_markup=get_products_keyboard(category))
                await call.answer(f"📂 : تم فتح قسم {category}")
            elif category == "cards":
                # قسم الكروت سنبرمجه لاحقاً كخطوة مستقلة
                await call.answer("🃏 : قسم الكروت قيد التجهيز في الخطوة القادمة!", show_alert=True)
            else:
                await call.answer("⚠️ : هذا القسم غير متوفر حالياً")

    except Exception as e:
        import logging
        logging.error(f"Error in Shop Navigation: {e}")
        await call.answer("❌ : حدث خطأ أثناء التنقل!")
        
# --- معالج أزرار التفعيل (الإصدار الآمن والمضمون) ---
@dp.callback_query_handler(lambda c: c.data.startswith(('auth_approve_', 'auth_block_')), user_id=ADMIN_ID)
async def process_auth_callback(c: types.CallbackQuery):
    action = c.data.split('_')[1]
    target_id = int(c.data.split('_')[2])
    
    if action == "approve":
        supabase.table("groups_hub").update({"status": "active"}).eq("group_id", target_id).execute()
        await c.answer("تم التفعيل ✅", show_alert=True)
        await bot.send_message(target_id, "🎉 مبارك! القروب مفعل. أرسل كلمة (مسابقة) للبدء.")
        
    elif action == "block":
        supabase.table("groups_hub").update({"status": "blocked"}).eq("group_id", target_id).execute()
        await c.answer("تم الحظر ❌", show_alert=True)
        await bot.send_message(target_id, "🚫 تم رفض طلب التفعيل وحظر القروب.")
    
    await c.message.delete()
    await admin_manage_groups(c)
    

# --- [ 2. إدارة الأقسام والأسئلة (النسخة النهائية المصلحة) ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('custom_add'), state="*")
async def custom_add_menu(c: types.CallbackQuery, state: FSMContext = None):
    if state:
        await state.finish()
    
    data_parts = c.data.split('_')
    try:
        owner_id = int(data_parts[-1])
    except (ValueError, IndexError):
        owner_id = c.from_user.id

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ هذي اللوحة مش حقك! 😂", show_alert=True)

    kb = get_categories_kb(owner_id)

    # هنا نستخدم edit_text لضمان التعديل بدل الإرسال الجديد
    await c.message.edit_text(
        "⚙️ **لوحة إعدادات أقسامك الخاصة:**\n\nاختر من القائمة أدناه لإدارة أقسامك وأسئلتك:", 
        reply_markup=kb, 
        parse_mode="Markdown"
    )
    await c.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('back_to_main'), state="*")
async def back_to_main_panel(c: types.CallbackQuery, state: FSMContext = None):
    if state:
        await state.finish()
    
    owner_id = int(c.data.split('_')[-1])
    
    # استدعاء كيبورد لوحة التحكم الرئيسية
    kb = get_main_control_kb(owner_id)

    # التعديل الجوهري: نستخدم edit_text ليحذف اللوحة السابقة وتظهر الرئيسية مكانها
    await c.message.edit_text(
        f"👋 أهلاً بك في لوحة إعدادات المسابقات الخاصة\n👑 المطور: @Ya_79k",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await c.answer("🔙 تمت العودة للقائمة الرئيسية")

@dp.callback_query_handler(lambda c: c.data.startswith('add_new_cat'), state="*")
async def btn_add_cat(c: types.CallbackQuery):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا يمكنك الإضافة في لوحة غيرك!", show_alert=True)

    await c.answer() 
    await Form.waiting_for_cat_name.set()
    
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("🔙 إلغاء والعودة", callback_data=f"custom_add_{owner_id}")
    )
    # تحديث الرسالة لطلب الاسم لمنع التراكم
    await c.message.edit_text("📝 **اكتب اسم القسم الجديد الآن:**", reply_markup=kb, parse_mode="Markdown")

@dp.message_handler(state=Form.waiting_for_cat_name)
async def save_cat(message: types.Message, state: FSMContext):
    cat_name = message.text.strip()
    user_id = message.from_user.id
    
    try:
        supabase.table("categories").insert({
            "name": cat_name, 
            "created_by": str(user_id)
        }).execute()
        
        await state.finish()
        
        # عند النجاح، نرسل رسالة جديدة كإشعار ثم نعطيه زر العودة الذي يقوم بالتعديل
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🔙 العودة للأقسام", callback_data=f"custom_add_{user_id}")
        )
        await message.answer(f"✅ تم حفظ القسم **'{cat_name}'** بنجاح.", reply_markup=kb, parse_mode="Markdown")

    except Exception as e:
        await state.finish()
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ الرجوع", callback_data=f"custom_add_{user_id}"))
        await message.answer("⚠️ حدث خطأ أو الاسم مكرر. حاول مرة أخرى.", reply_markup=kb)

# --- 1. نافذة إعدادات القسم (عند الضغط على اسمه) ---
@dp.callback_query_handler(lambda c: c.data.startswith('manage_questions_'))
async def manage_questions_window(c: types.CallbackQuery):
    # تفكيك البيانات: manage_questions_ID_USERID
    data = c.data.split('_')
    cat_id = data[2]
    owner_id = int(data[3])

    # حماية من المبعسسين
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ هذه اللوحة ليست لك!", show_alert=True)

    await c.answer()
    # استدعاء الدالة الموحدة
    await show_category_settings_ui(c.message, cat_id, owner_id, is_edit=True)


# --- 2. بدء تعديل اسم القسم ---
@dp.callback_query_handler(lambda c: c.data.startswith('edit_cat_'))
async def edit_category_start(c: types.CallbackQuery, state: FSMContext):
    data = c.data.split('_')
    cat_id = data[2]
    owner_id = int(data[3])

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تملك صلاحية التعديل!", show_alert=True)

    await c.answer()
    await state.update_data(edit_cat_id=cat_id, edit_owner_id=owner_id)
    await Form.waiting_for_new_cat_name.set()
    
    # زر تراجع ذكي يعود لصفحة الإعدادات
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("🚫 تراجع", callback_data=f"manage_questions_{cat_id}_{owner_id}")
    )
    await c.message.edit_text("📝 **نظام التعديل:**\n\nأرسل الآن الاسم الجديد للقسم:", reply_markup=kb)

# --- 3. حفظ الاسم الجديد (استدعاء الدالة الموحدة بعد الحفظ) ---
@dp.message_handler(state=Form.waiting_for_new_cat_name)
async def save_edited_category(message: types.Message, state: FSMContext):
    data = await state.get_data()
    cat_id = data['edit_cat_id']
    owner_id = data['edit_owner_id']
    new_name = message.text.strip()
    
    # تحديث الاسم في Supabase
    supabase.table("categories").update({"name": new_name}).eq("id", cat_id).execute()
    
    # تنظيف الشات
    try: await message.delete()
    except: pass

    await state.finish()
    
    # الاستدعاء الذكي: نرسل رسالة جديدة (is_edit=False) لأننا حذفنا رسالة المستخدم
    # ونعرض لوحة الإعدادات بالاسم الجديد فوراً
    await show_category_settings_ui(message, cat_id, owner_id, is_edit=False)
# ==========================================
# --- 3. نظام إضافة سؤال (محمي ومنظم) ---
# ==========================================

@dp.callback_query_handler(lambda c: c.data.startswith('add_q_'))
async def start_add_question(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    cat_id = data_parts[2]
    owner_id = int(data_parts[3])

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا يمكنك إضافة أسئلة في لوحة غيرك!", show_alert=True)

    await c.answer()
    await state.update_data(current_cat_id=cat_id, current_owner_id=owner_id, last_bot_msg_id=c.message.message_id)
    await Form.waiting_for_question.set()
    
    # زر إلغاء محمي
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("🚫 إلغاء", callback_data=f"manage_questions_{cat_id}_{owner_id}"))
    await c.message.edit_text("❓ **نظام إضافة الأسئلة:**\n\nاكتب الآن السؤال الذي تريد إضافته:", reply_markup=kb)

@dp.message_handler(state=Form.waiting_for_question)
async def process_q_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    await state.update_data(q_content=message.text)
    
    try:
        await message.delete()
        await bot.delete_message(message.chat.id, data['last_bot_msg_id'])
    except: pass

    await Form.waiting_for_ans1.set()
    msg = await message.answer("✅ تم حفظ نص السؤال.\n\nالآن أرسل **الإجابة الصحيحة** الأولى:")
    await state.update_data(last_bot_msg_id=msg.message_id)

@dp.message_handler(state=Form.waiting_for_ans1)
async def process_first_ans(message: types.Message, state: FSMContext):
    data = await state.get_data()
    owner_id = data['current_owner_id']
    await state.update_data(ans1=message.text)
    
    try: await bot.delete_message(message.chat.id, data['last_bot_msg_id'])
    except: pass
    
    # تشفير أزرار نعم/لا بالآيدي لضمان استمرار الحماية
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ نعم، إضافة ثانية", callback_data=f"add_ans2_{owner_id}"),
        InlineKeyboardButton("❌ لا، إجابة واحدة فقط", callback_data=f"no_ans2_{owner_id}")
    )
    msg = await message.answer(f"✅ تم حفظ الإجابة: ({message.text})\n\nهل تريد إضافة إجابة ثانية (بديلة)؟", reply_markup=kb)
    await state.update_data(last_bot_msg_id=msg.message_id)

# --- معالج إضافة إجابة ثانية ---
@dp.callback_query_handler(lambda c: c.data.startswith('add_ans2_'), state='*')
async def add_second_ans_start(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ عذراً، اللوحة محمية!", show_alert=True)
    
    await c.answer()
    await Form.waiting_for_ans2.set()
    await c.message.edit_text("📝 أرسل الآن **الإجابة الثانية** البديلة:")

@dp.message_handler(state=Form.waiting_for_ans2)
async def process_second_ans(message: types.Message, state: FSMContext):
    data = await state.get_data()
    cat_id = data.get('current_cat_id')
    owner_id = data.get('current_owner_id')

    supabase.table("questions").insert({
        "category_id": cat_id,
        "question_content": data.get('q_content'),
        "correct_answer": data.get('ans1'),
        "alternative_answer": message.text,
        "created_by": str(owner_id)
    }).execute()

    await state.finish()
    try: 
        await message.delete()
        await bot.delete_message(message.chat.id, data['last_bot_msg_id'])
    except: pass
    
    # العودة للوحة الإعدادات باستخدام الدالة الموحدة
    await show_category_settings_ui(message, cat_id, owner_id, is_edit=False)

# --- معالج رفض إضافة إجابة ثانية (إصلاح زر لا) ---
@dp.callback_query_handler(lambda c: c.data.startswith('no_ans2_'), state='*')
async def finalize_no_second(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة ليست لك!", show_alert=True)
    
    await c.answer()
    data = await state.get_data()
    cat_id = data.get('current_cat_id')

    supabase.table("questions").insert({
        "category_id": cat_id,
        "question_content": data.get('q_content'),
        "correct_answer": data.get('ans1'),
        "created_by": str(owner_id)
    }).execute()

    await state.finish()
    try: await c.message.delete()
    except: pass
    
    # العودة للوحة الإعدادات باستخدام الدالة الموحدة
    await show_category_settings_ui(c.message, cat_id, owner_id, is_edit=False)

# ==========================================
# --- 5. نظام عرض الأسئلة (المحمي بآيدي صاحب القسم) ---
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith('view_qs_'), state="*")
async def view_questions(c: types.CallbackQuery):
    # تفكيك البيانات: view_qs_CATID_OWNERID
    data = c.data.split('_')
    cat_id = data[2]
    owner_id = int(data[3])

    # 🛑 حماية من المبعسسين
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا يمكنك عرض أسئلة في لوحة غيرك!", show_alert=True)

    await c.answer()

    # جلب الأسئلة من Supabase
    questions = supabase.table("questions").select("*").eq("category_id", cat_id).execute()
    
    # إذا كان القسم فارغاً
    if not questions.data:
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🔙 رجوع", callback_data=f"manage_questions_{cat_id}_{owner_id}")
        )
        return await c.message.edit_text("⚠️ لا توجد أسئلة مضافة في هذا القسم حالياً.", reply_markup=kb)

    # بناء نص عرض الأسئلة
    txt = f"🔍 قائمة الأسئلة المضافة:\n"
    txt += "--- --- --- ---\n\n"
    
    for i, q in enumerate(questions.data, 1):
        txt += f"<b>{i} - {q['question_content']}</b>\n"
        txt += f"✅ ج1: {q['correct_answer']}\n"
        # التحقق من وجود إجابة بديلة (ج2)
        if q.get('alternative_answer'):
            txt += f"💡 ج2: {q['alternative_answer']}\n"
        txt += "--- --- --- ---\n"

    # أزرار التحكم في القائمة (محمية بالآيدي)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🗑️ حذف الأسئلة", callback_data=f"del_qs_menu_{cat_id}_{owner_id}"),
        InlineKeyboardButton("🔙 رجوع لإعدادات القسم", callback_data=f"manage_questions_{cat_id}_{owner_id}")
    )
    
    # استخدام HTML ليكون النص أوضح (bold للعناوين)
    await c.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")

# --- 6. نظام حذف الأسئلة (المحمي) ---

@dp.callback_query_handler(lambda c: c.data.startswith('del_qs_menu_'))
async def delete_questions_menu(c: types.CallbackQuery):
    data = c.data.split('_')
    # del(0) _ qs(1) _ menu(2) _ catid(3) _ ownerid(4)
    cat_id = data[3]
    owner_id = int(data[4])

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تملك صلاحية الحذف هنا!", show_alert=True)

    await c.answer()
    res = supabase.table("questions").select("*").eq("category_id", cat_id).execute()
    questions = res.data
    
    kb = InlineKeyboardMarkup(row_width=1)
    if questions:
        for q in questions:
            kb.add(InlineKeyboardButton(
                f"🗑️ حذف: {q['question_content'][:25]}...", 
                callback_data=f"pre_del_q_{q['id']}_{cat_id}_{owner_id}"
            ))
    
    # تصحيح زر الرجوع ليعود للقائمة السابقة
    kb.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"manage_questions_{cat_id}_{owner_id}"))
    await c.message.edit_text("🗑️ اختر السؤال المراد حذفه:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith('pre_del_q_'))
async def confirm_delete_question(c: types.CallbackQuery):
    data = c.data.split('_')
    # pre(0) _ del(1) _ q(2) _ qid(3) _ catid(4) _ ownerid(5)
    q_id, cat_id, owner_id = data[3], data[4], data[5]

    if c.from_user.id != int(owner_id):
        return await c.answer("⚠️ مبعسس؟ ما تقدر تحذف! 😂", show_alert=True)
    
    kb = InlineKeyboardMarkup(row_width=2).add(
        InlineKeyboardButton("✅ نعم، احذف", callback_data=f"final_del_q_{q_id}_{cat_id}_{owner_id}"),
        InlineKeyboardButton("❌ تراجع", callback_data=f"del_qs_menu_{cat_id}_{owner_id}")
    )
    await c.message.edit_text("⚠️ هل أنت متأكد من حذف هذا السؤال؟", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith('final_del_q_'))
async def execute_delete_question(c: types.CallbackQuery):
    data = c.data.split('_')
    # final(0) _ del(1) _ q(2) _ qid(3) _ catid(4) _ ownerid(5)
    q_id, cat_id, owner_id = data[3], data[4], data[5]
    
    supabase.table("questions").delete().eq("id", q_id).execute()
    await c.answer("🗑️ تم الحذف بنجاح", show_alert=True)
    
    # تحديث البيانات في الـ Callback لاستدعاء القائمة مجدداً
    c.data = f"del_qs_menu_{cat_id}_{owner_id}"
    await delete_questions_menu(c)


# --- 7. حذف القسم نهائياً (النسخة المصلحة) ---
@dp.callback_query_handler(lambda c: c.data.startswith('confirm_del_cat_'))
async def confirm_delete_cat(c: types.CallbackQuery):
    data = c.data.split('_')
    cat_id = data[3]
    owner_id = int(data[4])

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تملك صلاحية حذف الأقسام!", show_alert=True)

    await c.answer()
    kb = InlineKeyboardMarkup(row_width=2).add(
        InlineKeyboardButton("✅ نعم، احذف", callback_data=f"final_del_cat_{cat_id}_{owner_id}"),
        InlineKeyboardButton("❌ لا، تراجع", callback_data=f"manage_questions_{cat_id}_{owner_id}")
    )
    # تعديل نص الرسالة الحالية لطلب التأكيد
    await c.message.edit_text("⚠️ هل أنت متأكد من حذف هذا القسم نهائياً مع كل أسئلته؟", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith('final_del_cat_'))
async def execute_delete_cat(c: types.CallbackQuery):
    data = c.data.split('_')
    cat_id = data[3]
    owner_id = int(data[4])

    # 1. تنفيذ الحذف في سوبابيس
    try:
        supabase.table("categories").delete().eq("id", cat_id).execute()
        await c.answer("🗑️ تم حذف القسم بنجاح", show_alert=True)
    except Exception as e:
        return await c.answer("❌ فشل الحذف من قاعدة البيانات")

    # 2. العودة لقائمة الأقسام بتحديث نفس الرسالة
    # استخدمنا await لضمان التنفيذ وتمرير المتغيرات لعمل Edit
    await custom_add_menu(c)
    
# --- 8. نظام عرض قائمة الأقسام (تصفية وحماية) ---
@dp.callback_query_handler(lambda c: c.data.startswith('list_cats_'))
async def list_categories_for_questions(c: types.CallbackQuery):
    try:
        # استخراج الآيدي من الكولباك لضمان الحماية
        owner_id = int(c.data.split('_')[-1])
        
        if c.from_user.id != owner_id:
            return await c.answer("⚠️ لا يمكنك استعراض أقسام غيرك!", show_alert=True)

        await c.answer()
        
        # طلب الأقسام التي تخص هذا المستخدم فقط من سوبابيس
        res = supabase.table("categories").select("*").eq("created_by", str(owner_id)).execute()
        categories = res.data

        if not categories:
            # إذا لم يكن لديه أقسام، نرسل تنبيهاً ونبقى في نفس اللوحة
            return await c.answer("⚠️ ليس لديك أقسام خاصة بك حالياً، قم بإضافة قسم أولاً.", show_alert=True)

        kb = InlineKeyboardMarkup(row_width=1)
        for cat in categories:
            # تشفير أزرار الأقسام بآيدي القسم وآيدي المالك
            # manage_questions_CATID_OWNERID
            kb.add(InlineKeyboardButton(
                f"📂 {cat['name']}", 
                callback_data=f"manage_questions_{cat['id']}_{owner_id}"
            ))

        # زر الرجوع للوحة "إضافة خاصة" بآيدي المستخدم
        kb.add(InlineKeyboardButton("⬅️ الرجوع", callback_data=f"custom_add_{owner_id}"))
        
        await c.message.edit_text("📋 اختر أحد أقسامك لإدارة الأسئلة:", reply_markup=kb)

    except Exception as e:
        logging.error(f"Filter Error: {e}")
        await c.answer("⚠️ حدث خطأ في جلب الأقسام.")

# --- 1. واجهة تهيئة المسابقة (النسخة النظيفة والمحمية) ---
@dp.callback_query_handler(lambda c: c.data.startswith('setup_quiz'), state="*")
async def setup_quiz_main(c: types.CallbackQuery, state: FSMContext):
    await state.finish()
    
    # تحديد الهوية: هل هو ضغط مباشر أم قادم من زر رجوع مشفر؟
    data_parts = c.data.split('_')
    owner_id = int(data_parts[-1]) if len(data_parts) > 1 else c.from_user.id
    
    # حماية المبعسسين
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ اللوحة مش حقك يا حبيبنا 😂", show_alert=True)
    
    await c.answer()
    
    # حفظ صاحب الجلسة في الـ State
    await state.update_data(owner_id=owner_id, owner_name=c.from_user.first_name)
    
    text = "🎉 **أهلاً بك!**\nقم بتهيئة المسابقة عن طريق اختيار مصدر الأسئلة:"
    
    # هنا الحذف والاستدعاء: استدعينا الدالة من قسم المساعدة
    await c.message.edit_text(
        text, 
        reply_markup=get_setup_quiz_kb(owner_id), 
        parse_mode="Markdown"
    )
# ==========================================
# 1. اختيار مصدر الأسئلة (رسمي / خاص / أعضاء) - نسخة المجلدات والأسماء
# ==========================================
# --- [ أسئلة البوت: نظام المجلدات الجديد ] --
@dp.callback_query_handler(lambda c: c.data.startswith('bot_setup_step1_'), state="*")
async def start_bot_selection(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)
    
    # جلب المجلدات بدلاً من الأقسام مباشرة
    res = supabase.table("folders").select("id, name").execute()
    if not res.data: return await c.answer("⚠️ لا توجد مجلدات رسمية!", show_alert=True)

    eligible_folders = [{"id": str(item['id']), "name": item['name']} for item in res.data]
    
    # تخزين البيانات في الحالة للبدء باختيار المجلدات
    await state.update_data(
        eligible_folders=eligible_folders, 
        selected_folders=[], 
        is_bot_quiz=True, 
        current_owner_id=owner_id
    ) 
    
    # استدعاء دالة عرض المجلدات
    await render_folders_list(c.message, eligible_folders, [], owner_id)

# --- [ أسئلة خاصة: جلب أقسام المستخدم نفسه ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('my_setup_step1_'), state="*")
async def start_private_selection(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)
    
    res = supabase.table("categories").select("*").eq("created_by", str(owner_id)).execute()
    if not res.data: return await c.answer("⚠️ ليس لديك أقسام خاصة!", show_alert=True)
    
    await state.update_data(eligible_cats=res.data, selected_cats=[], is_bot_quiz=False, current_owner_id=owner_id) 
    await render_categories_list(c.message, res.data, [], owner_id)


    # --- [ أسئلة الأعضاء: إظهار الأسماء بدلاً من الأرقام ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('members_setup_step1_'), state="*")
async def start_member_selection(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)
    
    # جلب المعرفات التي لها أسئلة
    res = supabase.table("questions").select("created_by").execute()
    if not res.data: return await c.answer("⚠️ لا يوجد أعضاء حالياً.", show_alert=True)
    
    from collections import Counter
    counts = Counter([q['created_by'] for q in res.data])
    eligible_ids = [m_id for m_id, count in counts.items() if count >= 15]
    
    if not eligible_ids: return await c.answer("⚠️ لا يوجد مبدعون وصلوا لـ 15 سؤال.", show_alert=True)
    
    # الإصلاح: جلب الأسماء من جدول المستخدمين (users) لربط الـ ID بالاسم
    users_res = supabase.table("users").select("user_id, name").in_("user_id", eligible_ids).execute()
    
    # تحويل البيانات لقائمة كائنات تحتوي على الاسم والمعرف
    eligible_list = [{"id": str(u['user_id']), "name": u['name'] or f"مبدع {u['user_id']}"} for u in users_res.data]
    
    await state.update_data(eligible_list=eligible_list, selected_members=[], is_bot_quiz=False, current_owner_id=owner_id)
    await render_members_list(c.message, eligible_list, [], owner_id)
# ==========================================
# 2. معالجات التبديل والاختيار (Toggle & Go) - نسخة المجلدات المحدثة
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith('toggle_folder_'), state="*")
async def toggle_folder_selection(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    f_id = data_parts[2]
    owner_id = int(data_parts[3])
    
    if c.from_user.id != owner_id: 
        return await c.answer("⚠️ مبعسس؟ المجلدات لصاحب المسابقة بس! 😂", show_alert=True)
    
    data = await state.get_data()
    selected = data.get('selected_folders', [])
    eligible = data.get('eligible_folders', [])
    
    if f_id in selected: selected.remove(f_id)
    else: selected.append(f_id)
    
    await state.update_data(selected_folders=selected)
    await c.answer()
    # استدعاء دالة رندر المجلدات لتحديث الشكل
    await render_folders_list(c.message, eligible, selected, owner_id)

 # --- [ 2. معالج الانتقال من المجلدات إلى الأقسام ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('confirm_folders_'), state="*")
async def confirm_folders_to_cats(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)
    
    data = await state.get_data()
    chosen_folder_ids = data.get('selected_folders', [])
    
    if not chosen_folder_ids:
        return await c.answer("⚠️ اختر مجلد واحد على الأقل!", show_alert=True)

    # جلب الأقسام التابعة للمجلدات المختارة فقط من جدول bot_categories
    res = supabase.table("bot_categories").select("id, name").in_("folder_id", chosen_folder_ids).execute()
    
    if not res.data:
        return await c.answer("⚠️ هذه المجلدات لا تحتوي على أقسام حالياً!", show_alert=True)
    
    await state.update_data(eligible_cats=res.data, selected_cats=[])
    await c.answer("✅ تم جلب أقسام المجلدات")
    # الانتقال لعرض الأقسام
    await render_categories_list(c.message, res.data, [], owner_id)

# --- [ 3. معالج تبديل الأعضاء (Members Toggle) ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('toggle_mem_'), state="*")
async def toggle_member(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    m_id = data_parts[2]
    owner_id = int(data_parts[3])
    
    if c.from_user.id != owner_id: return await c.answer("⚠️ مبعسس؟ ما تقدر تختار! 😂", show_alert=True)
    
    data = await state.get_data()
    selected = data.get('selected_members', [])
    eligible = data.get('eligible_list', []) # تحتوي على الأوبجكت {id, name}
    
    if m_id in selected: selected.remove(m_id)
    else: selected.append(m_id)
    
    await state.update_data(selected_members=selected)
    await c.answer()
    await render_members_list(c.message, eligible, selected, owner_id)

# --- [ 4. معالج الانتقال من الأعضاء إلى الأقسام ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('go_to_cats_step_'), state="*")
async def show_selected_members_cats(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة ليست لك!", show_alert=True)
    
    data = await state.get_data()
    chosen_ids = data.get('selected_members', [])
    
    # جلب الأقسام الخاصة بالأعضاء المختارين
    res = supabase.table("categories").select("id, name").in_("created_by", chosen_ids).execute()
    
    await state.update_data(eligible_cats=res.data, selected_cats=[])
    await render_categories_list(c.message, res.data, [], owner_id)

# --- [ 5. معالج تبديل الأقسام (Categories Toggle) ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('toggle_cat_'), state="*")
async def toggle_category_selection(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    cat_id = data_parts[2]
    owner_id = int(data_parts[3])
    
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)

    data = await state.get_data()
    selected = data.get('selected_cats', [])
    eligible = data.get('eligible_cats', [])
    
    if cat_id in selected: selected.remove(cat_id)
    else: selected.append(cat_id)
    
    await state.update_data(selected_cats=selected)
    await c.answer()
    await render_categories_list(c.message, eligible, selected, owner_id)
# --- 4. لوحة الإعدادات (استدعاء دالة المساعدة) ---
@dp.callback_query_handler(lambda c: c.data.startswith('final_quiz_settings'), state="*")
async def final_quiz_settings_panel(c: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    # جلب owner_id من البيانات المخزنة لضمان الحماية
    owner_id = data.get('current_owner_id') or c.from_user.id
    
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ هذه اللوحة محمية لصاحب المسابقة!", show_alert=True)
    
    await c.answer()
    # استدعاء دالة العرض من قسم المساعدة
    await render_final_settings_panel(c.message, data, owner_id)
    
# --- [ 5 + 6 ] المحرك الموحد ومعالج الحفظ النهائي --- #
@dp.callback_query_handler(lambda c: c.data.startswith(('tog_', 'cyc_', 'set_', 'start_quiz_')), state="*")
async def quiz_settings_engines(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    action = data_parts[0] 
    owner_id = int(data_parts[-1])
    
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تتدخل في إعدادات غيرك! 😂", show_alert=True)

    data = await state.get_data()

    # 1️⃣ --- قسم المحركات (التعديل اللحظي) ---
    if action in ['tog', 'cyc', 'set']:
        await c.answer()
        
        # --- [جديد] محرك النطاق (إذاعة عامة / خاصة) ---
        if action == 'tog' and data_parts[1] == 'broad':
            current_broad = data.get('is_broadcast', False)
            new_status = not current_broad
            await state.update_data(is_broadcast=new_status)
            status_txt = "🌐 تم تفعيل الإذاعة العامة" if new_status else "📍 تم تحديد المسابقة داخلية"
            await c.answer(status_txt)

        # محرك التلميح الموحد
        elif action == 'cyc' and data_parts[1] == 'hint':
            is_currently_on = data.get('quiz_hint_bool', False)
            if not is_currently_on:
                await state.update_data(quiz_hint_bool=True, quiz_smart_bool=True)
                await c.answer("✅ تم تفعيل التلميحات")
            else:
                await state.update_data(quiz_hint_bool=False, quiz_smart_bool=False)
                await c.answer("❌ تم إيقاف التلميحات")
        
        # محرك الوقت
        elif action == 'cyc' and data_parts[1] == 'time':
            curr = data.get('quiz_time', 15)
            next_t = 20 if curr == 15 else (30 if curr == 20 else (45 if curr == 30 else 15))
            await state.update_data(quiz_time=next_t)

        # محرك النظام (سرعة/كامل)
        elif action == 'cyc' and data_parts[1] == 'mode':
            curr_m = data.get('quiz_mode', 'السرعة ⚡')
            next_m = 'الوقت الكامل ⏳' if curr_m == 'السرعة ⚡' else 'السرعة ⚡'
            await state.update_data(quiz_mode=next_m)

        # محرك عدد الأسئلة
        elif action == 'set' and data_parts[1] == 'cnt':
            await state.update_data(quiz_count=int(data_parts[2]))

        # تحديث اللوحة فوراً بعد أي تغيير
        new_data = await state.get_data()
        return await render_final_settings_panel(c.message, new_data, owner_id)

    # 2️⃣ --- قسم بدء الحفظ والتشغيل ---
    elif action == 'start' and data_parts[1] == 'quiz':
        if not data.get('selected_cats'):
            return await c.answer("⚠️ اختر قسماً واحداً على الأقل!", show_alert=True)
        
        # فحص النطاق قبل البدء
        is_broadcast = data.get('is_broadcast', False)
        
        if is_broadcast:
            # إذا كانت عامة، نتأكد أن القروبات المفعلة متوفرة
            res = supabase.table("groups_hub").select("group_id").eq("status", "active").execute()
            if not res.data:
                return await c.answer("❌ لا توجد قروبات مفعلة حالياً للإذاعة العامة!", show_alert=True)
            await c.answer(f"🌐 سيتم البث في {len(res.data)} قروب!", show_alert=True)
        else:
            await c.answer("📍 مسابقة داخلية لهذا القروب.")

        await Form.waiting_for_quiz_name.set() 
        return await c.message.edit_text(
            "📝 يا بطل، أرسل الآن اسماً لمسابقتك:\n\n*(سيتم حفظ التلميحات ونطاق الإرسال تحت هذا الاسم)*",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("❌ إلغاء", callback_data=f"final_quiz_settings_{owner_id}")
            )
        )

@dp.message_handler(state=Form.waiting_for_quiz_name)
async def process_quiz_name_final(message: types.Message, state: FSMContext):
    quiz_name = message.text.strip()
    data = await state.get_data()
    
    selected_cats = data.get('selected_cats', [])
    clean_list = [str(c) for c in selected_cats] 
    u_id = str(message.from_user.id)

    # تجهيز البيانات بناءً على الأعمدة الفعلية في جدولك (CSV)
    payload = {
        "created_by": u_id,
        "quiz_name": quiz_name,
        "chat_id": u_id,
        "time_limit": int(data.get('quiz_time', 15)),
        "questions_count": int(data.get('quiz_count', 10)),
        "mode": data.get('quiz_mode', 'السرعة ⚡'),
        "hint_enabled": bool(data.get('quiz_hint_bool', False)),
        "smart_hint": bool(data.get('quiz_smart_bool', False)),
        "is_bot_quiz": bool(data.get('is_bot_quiz', False)), # عمود موجود في جدولك
        "cats": json.dumps(clean_list), # سوبابيس يفضل JSON للنصوص المصفوفة
        "is_public": bool(data.get('is_broadcast', False)) # استخدمنا is_public بدلاً من is_broadcast
    }

    try:
        # تنفيذ الحفظ
        supabase.table("saved_quizzes").insert(payload).execute()
        
        # تنسيق رسالة النجاح
        is_pub = payload["is_public"]
        scope_emoji = "🌐" if is_pub else "📍"
        scope_text = "إذاعة عامة" if is_pub else "مسابقة داخلية"
        
        success_msg = (
            f"✅ **تم حفظ المسابقة بنجاح!**\n"
            f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
            f"🏷 الاسم: `{quiz_name}`\n"
            f"⏱ الوقت: `{payload['time_limit']} ثانية`\n"
            f"📊 الأقسام: `{len(selected_cats)}` قسم\n"
            f"{scope_emoji} النطاق: **{scope_text}**\n"
            f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n\n"
            f"🚀 اكتب كلمة مسابقة ستجدها الآن في 'قائمة مسابقاتك'!"
        )
        
        await message.answer(success_msg, parse_mode="Markdown")
        await state.finish()

    except Exception as e:
        import logging
        logging.error(f"Error saving quiz: {e}")
        # هنا البوت بيعلمك لو فيه عمود ثاني ناقص
        await message.answer(f"❌ خطأ في قاعدة البيانات:\n`{str(e)}`", parse_mode="Markdown")
# ==========================================
# [1] عرض قائمة المسابقات (نسخة ياسر المصفاة)
# ==========================================
@dp.message_handler(lambda message: message.text == "مسابقة")
@dp.callback_query_handler(lambda c: c.data.startswith('list_my_quizzes_'), state="*")
async def show_quizzes(obj):
    is_callback = isinstance(obj, types.CallbackQuery)
    user = obj.from_user
    u_id = str(user.id)
    
    # جلب المسابقات الخاصة بالمستخدم فقط من سوبابيس
    res = supabase.table("saved_quizzes").select("*").eq("created_by", u_id).execute()
    kb = InlineKeyboardMarkup(row_width=1)
    
    if not res.data:
        msg_empty = f"⚠️ يا {user.first_name}، لا توجد لديك مسابقات محفوظة.**"
        if is_callback: return await obj.message.edit_text(msg_empty)
        return await obj.answer(msg_empty)

    # بناء قائمة المسابقات
    for q in res.data:
        kb.add(InlineKeyboardButton(
            f"🏆 {q['quiz_name']}", 
            callback_data=f"manage_quiz_{q['id']}_{u_id}"
        ))
    
    kb.add(InlineKeyboardButton("❌ إغلاق", callback_data=f"close_{u_id}"))
    
    title = f"🎁 مسابقاتك الجاهزة يا {user.first_name}:"

    if is_callback:
        await obj.message.edit_text(title, reply_markup=kb, parse_mode="Markdown")
    else:
        await obj.reply(title, reply_markup=kb, parse_mode="Markdown")

# ==========================================
# [2] المحرك الأمني ولوحة التحكم (التشطيب النهائي المصلح)
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith(('run_', 'close_', 'confirm_del_', 'final_del_', 'edit_time_', 'manage_quiz_', 'quiz_settings_', 'set_c_', 'toggle_speed_', 'toggle_scope_', 'toggle_hint_', 'save_quiz_process_')), state="*")
async def handle_secure_actions(c: types.CallbackQuery, state: FSMContext):
    try:
        data_parts = c.data.split('_')
        owner_id = data_parts[-1]
        user_id = str(c.from_user.id)
        
        # الدرع الأمني
        if user_id != owner_id:
            return await c.answer("🚫 هذه اللوحة ليست لك.", show_alert=True)

        # 1️⃣ شاشة الإدارة الرئيسية للمسابقة
        if c.data.startswith('manage_quiz_'):
            quiz_id = data_parts[2]
            res = supabase.table("saved_quizzes").select("quiz_name").eq("id", quiz_id).single().execute()
            
            kb = InlineKeyboardMarkup(row_width=1).add(
                InlineKeyboardButton("🚀 بدء الانطلاق", callback_data=f"run_{quiz_id}_{user_id}"),
                InlineKeyboardButton("⚙️ الإعدادات", callback_data=f"quiz_settings_{quiz_id}_{user_id}"),
                InlineKeyboardButton("🔙 رجوع", callback_data=f"list_my_quizzes_{user_id}")
            )
            await c.message.edit_text(f"💎 إدارة: {res.data['quiz_name']}", reply_markup=kb)
            return

        # 2️⃣ لوحة الإعدادات
        elif c.data.startswith('quiz_settings_'):
            quiz_id = data_parts[2]
            res = supabase.table("saved_quizzes").select("*").eq("id", quiz_id).single().execute()
            q = res.data
            
            await state.update_data(editing_quiz_id=quiz_id, quiz_name=q['quiz_name'])
            q_time, q_count = q.get('time_limit', 15), q.get('questions_count', 10)
            q_mode = q.get('mode', 'السرعة ⚡')
            is_hint = q.get('smart_hint', False)
            is_public = q.get('is_public', False)

            text = (
                f"❃┏━━━━━ إعدادات: {q['quiz_name']} ━━━━━┓❃\n"
                f"📊 عدد الاسئلة: {q_count}\n"
                f"📡 النطاق: {'إذاعة عامة 🌐' if is_public else 'مسابقة داخلية 📍'}\n"
                f"🔖 النظام: {q_mode}\n"
                f"⏳ المهلة: {q_time} ثانية\n"
                f"💡 التلميح الذكي: {'مفعل ✅' if is_hint else 'معطل ❌'}\n"
                "❃┗━━━━━━━━━━━━━━━━━━━━┛❃"
            )

            kb = InlineKeyboardMarkup(row_width=5)
            kb.row(InlineKeyboardButton("📊 اختر عدد الأسئلة:", callback_data="ignore"))
            counts = [10, 15, 25, 32, 45]
            kb.add(*[InlineKeyboardButton(f"{'✅' if q_count==n else ''}{n}", callback_data=f"set_c_{quiz_id}_{n}_{user_id}") for n in counts])
            kb.row(InlineKeyboardButton(f"⏱️ المهلة: {q_time} ثانية", callback_data=f"edit_time_{quiz_id}_{user_id}"))
            kb.row(
                InlineKeyboardButton(f"🔖 {q_mode}", callback_data=f"toggle_speed_{quiz_id}_{user_id}"),
                InlineKeyboardButton(f"💡 {'مفعل ✅' if is_hint else 'معطل ❌'}", callback_data=f"toggle_hint_{quiz_id}_{user_id}")
            )
            kb.row(InlineKeyboardButton(f"📡 {'نطاق: عام 🌐' if is_public else 'نطاق: داخلي 📍'}", callback_data=f"toggle_scope_{quiz_id}_{user_id}"))
            kb.row(InlineKeyboardButton("💾 حفظ التعديلات 🚀", callback_data=f"save_quiz_process_{quiz_id}_{user_id}"))
            kb.row(InlineKeyboardButton("🗑️ حذف المسابقة", callback_data=f"confirm_del_{quiz_id}_{user_id}"))
            kb.row(InlineKeyboardButton("🔙 رجوع للخلف", callback_data=f"manage_quiz_{quiz_id}_{user_id}"))
            
            await c.message.edit_text(text, reply_markup=kb)
            return

        # 3️⃣ التبديلات (Toggles)
        elif any(c.data.startswith(x) for x in ['toggle_hint_', 'toggle_speed_', 'toggle_scope_', 'set_c_']):
            quiz_id = data_parts[2]
            # محرك النطاق (Scope) - المصلح ليتناسب مع عمود is_public
            if 'toggle_scope_' in c.data:
                res = supabase.table("saved_quizzes").select("is_public").eq("id", quiz_id).single().execute()
                # جلب القيمة الحالية (True أو False)
                curr_is_public = res.data.get('is_public', False)
                # عكس القيمة
                new_is_public = not curr_is_public
                # التحديث في قاعدة البيانات
                supabase.table("saved_quizzes").update({"is_public": new_is_public}).eq("id", quiz_id).execute()
                
                status_text = "عام 🌐" if new_is_public else "داخلي 📍"
                await c.answer(f"✅ أصبح النطاق: {status_text}")
            elif 'toggle_hint_' in c.data:
                res = supabase.table("saved_quizzes").select("smart_hint").eq("id", quiz_id).single().execute()
                new_h = not res.data.get('smart_hint', False)
                supabase.table("saved_quizzes").update({"smart_hint": new_h}).eq("id", quiz_id).execute()
            elif 'toggle_speed_' in c.data:
                res = supabase.table("saved_quizzes").select("mode").eq("id", quiz_id).single().execute()
                new_m = "الوقت الكامل ⏳" if res.data.get('mode') == "السرعة ⚡" else "السرعة ⚡"
                supabase.table("saved_quizzes").update({"mode": new_m}).eq("id", quiz_id).execute()
            elif 'set_c_' in c.data:
                count = int(data_parts[3])
                supabase.table("saved_quizzes").update({"questions_count": count}).eq("id", quiz_id).execute()
            
            await c.answer("تم التحديث ✅")
            # إعادة توجيه ذاتي لتحديث الواجهة
            c.data = f"quiz_settings_{quiz_id}_{user_id}"
            return await handle_secure_actions(c, state)
        
        # 4️⃣ تغيير الوقت
        elif c.data.startswith('edit_time_'):
            quiz_id = data_parts[2]
            res = supabase.table("saved_quizzes").select("time_limit").eq("id", quiz_id).single().execute()
            curr = res.data.get('time_limit', 15)
            next_t = 20 if curr == 15 else (30 if curr == 20 else (45 if curr == 30 else 15))
            supabase.table("saved_quizzes").update({"time_limit": next_t}).eq("id", quiz_id).execute()
            c.data = f"quiz_settings_{quiz_id}_{user_id}"
            return await handle_secure_actions(c, state)

     # 5️⃣ الحفظ وتشغيل وحذف وإغلاق (النسخة المصلحة 2026 🚀)
        elif c.data.startswith('save_quiz_process_'):
            # 🛠️ تصحيح الاندكس من 2 إلى 3 لسحب الرقم الحقيقي
            quiz_id = data_parts[3] 
            await c.answer("✅ تم الحفظ بنجاح!", show_alert=True)
            c.data = f"manage_quiz_{quiz_id}_{user_id}"
            return await handle_secure_actions(c, state)

        elif c.data.startswith('close_'):
            try: return await c.message.delete()
            except: pass

        elif c.data.startswith('confirm_del_'):
            quiz_id = data_parts[2]
            # جعلنا زر التراجع يعود مباشرة للقائمة show_quizzes
            kb = InlineKeyboardMarkup().add(
                InlineKeyboardButton("✅ نعم، احذف", callback_data=f"final_del_{quiz_id}_{user_id}"),
                InlineKeyboardButton("🚫 تراجع", callback_data=f"show_quizzes_{user_id}")
            )
            return await c.message.edit_text("⚠️ **هل أنت متأكد من الحذف؟**", reply_markup=kb)

        elif c.data.startswith('final_del_'):
            quiz_id = data_parts[2]
            # 1. تنفيذ الحذف
            supabase.table("saved_quizzes").delete().eq("id", quiz_id).execute()
            await c.answer("🗑️ تم الحذف بنجاح", show_alert=True)
            
            # 2. بدلاً من handle_secure_actions، نقوم بتغيير الداتا واستدعاء دالة العرض الأصلية
            c.data = f"show_quizzes_{user_id}"
            # استدعاء دالة عرض القائمة (تأكد من اسم الدالة لديك، غالباً هي show_my_quizzes)
            return await show_my_quizzes(c)
            
        # --- [ نظام تشغيل المسابقات: عامة أو خاصة ] ---
        elif c.data.startswith('confirm_del_'):
            quiz_id = data_parts[2]
            # جعلنا زر التراجع يعود مباشرة للقائمة show_quizzes
            kb = InlineKeyboardMarkup().add(
                InlineKeyboardButton("✅ نعم، احذف", callback_data=f"final_del_{quiz_id}_{user_id}"),
                InlineKeyboardButton("🚫 تراجع", callback_data=f"show_quizzes_{user_id}")
            )
            return await c.message.edit_text("⚠️ **هل أنت متأكد من الحذف؟**", reply_markup=kb)

        elif c.data.startswith('final_del_'):
            quiz_id = data_parts[2]
            # 1. تنفيذ الحذف في قاعدة البيانات
            supabase.table("saved_quizzes").delete().eq("id", quiz_id).execute()
            await c.answer("🗑️ تم الحذف بنجاح", show_alert=True)
            
            # 2. العودة للقائمة: تغيير الداتا واستدعاء دالة العرض مباشرة
            c.data = f"show_quizzes_{user_id}"
            return await show_my_quizzes(c) 

        # --- [ نظام تشغيل المسابقات: عامة أو خاصة ] ---
        elif c.data.startswith('run_'):
            quiz_id = data_parts[1]
            user_id = data_parts[2]
            
            res = supabase.table("saved_quizzes").select("*").eq("id", quiz_id).single().execute()
            q_data = res.data
            
            if not q_data: 
                return await c.answer("❌ المسابقة غير موجودة!")

            # 🔥 حل مشكلة عدم حذف اللوحة: نحذفها هنا قبل تشغيل أي محرك
            try:
                await c.message.delete()
            except:
                pass

            if q_data.get('is_public'):
                # 🌐 مسار الإذاعة العامة
                await c.answer("🌐 جاري إطلاق الإذاعة العامة للمجموعات...")
                await start_broadcast_process(c, quiz_id, user_id)
            else:
                # 📍 مسار التشغيل الخاص
                if q_data.get('is_bot_quiz'):
                    asyncio.create_task(engine_bot_questions(c.message.chat.id, q_data, c.from_user.first_name))
                else:
                    asyncio.create_task(engine_user_questions(c.message.chat.id, q_data, c.from_user.first_name))
            
            return # إنهاء المعالج بنجاح

    except Exception as e:
        logging.error(f"Handle Secure Actions Error: {e}")
        try: 
            await c.answer("🚨 خطأ في اللوحة أو البيانات", show_alert=True)
        except: 
            pass
        
# ==========================================
# 3. نظام المحركات المنفصلة (ياسر المطور - نسخة عشوائية)
# ==========================================

# --- [1. محرك أسئلة البوت] ---
async def engine_bot_questions(chat_id, quiz_data, owner_name):
    try:
        raw_cats = quiz_data.get('cats', [])
        if isinstance(raw_cats, str):
            try:
                cat_ids_list = json.loads(raw_cats)
            except:
                cat_ids_list = raw_cats.replace('[','').replace(']','').replace('"','').split(',')
        else:
            cat_ids_list = raw_cats

        cat_ids = [int(c) for c in cat_ids_list if str(c).strip().isdigit()]
        if not cat_ids:
            return await bot.send_message(chat_id, "⚠️ خطأ: لم يتم العثور على أقسام صالحة.")

        # جلب الأسئلة وخلطها عشوائياً
        res = supabase.table("bot_questions").select("*").in_("bot_category_id", cat_ids).execute()
        if not res.data:
            return await bot.send_message(chat_id, "⚠️ لم أجد أسئلة في جدول البوت.")

        questions_pool = res.data
        random.shuffle(questions_pool)
        count = int(quiz_data.get('questions_count', 10))
        selected_questions = questions_pool[:count]

        await run_universal_logic(chat_id, selected_questions, quiz_data, owner_name, "bot")
    except Exception as e:
        logging.error(f"Bot Engine Error: {e}")

# --- [2. محرك أسئلة الأعضاء] ---
async def engine_user_questions(chat_id, quiz_data, owner_name):
    try:
        raw_cats = quiz_data.get('cats', [])
        if isinstance(raw_cats, str):
            try:
                cat_ids_list = json.loads(raw_cats)
            except:
                cat_ids_list = raw_cats.replace('[','').replace(']','').replace('"','').split(',')
        else:
            cat_ids_list = raw_cats

        cat_ids = [int(c) for c in cat_ids_list if str(c).strip().isdigit()]
        if not cat_ids:
            return await bot.send_message(chat_id, "⚠️ خطأ في أقسام الأعضاء.")

        # جلب الأسئلة وخلطها عشوائياً
        res = supabase.table("questions").select("*, categories(name)").in_("category_id", cat_ids).execute()
        if not res.data:
            return await bot.send_message(chat_id, "⚠️ لم أجد أسئلة في أقسام الأعضاء.")

        questions_pool = res.data
        random.shuffle(questions_pool)
        count = int(quiz_data.get('questions_count', 10))
        selected_questions = questions_pool[:count]

        await run_universal_logic(chat_id, selected_questions, quiz_data, owner_name, "user")
    except Exception as e:
        logging.error(f"User Engine Error: {e}")


# --- [ محرك التلميحات الملكي المطور: 3 قلوب + ذاكرة سحابية ✨ ] ---

current_key_index = 0 # متغير تدوير المفاتيح
# ============================================================
# 🔄 محرك التلميحات الذكي بنظام التدوير الآلي (ياسر المطور)
# ============================================================
async def generate_smart_hint(answer_text, force_refresh=False):
    answer_text = str(answer_text).strip()
    
    # 1. فحص الذاكرة السحابية (Skip if force_refresh is True)
    if not force_refresh:
        try:
            cached_res = supabase.table("hints").select("hint").eq("word", answer_text).execute()
            if cached_res.data:
                return cached_res.data[0]['hint']
        except Exception as e:
            logging.error(f"Database Cache Error: {e}")

    # 2. جلب قائمة المفاتيح المتاحة للتدوير
    # سنقوم بتجربة G_KEY_1 و G_KEY_2 و G_KEY_3 بالتوالي
    available_keys = ["G_KEY_1", "G_KEY_2", "G_KEY_3"]
    
    # محاولة جلب المفتاح النشط حالياً لنبدأ به توفيراً للوقت
    try:
        active_res = supabase.table("system_settings").select("key_value").eq("key_name", "ACTIVE_GROQ_KEY").execute()
        if active_res.data:
            start_key = active_res.data[0]['key_value']
            # إعادة ترتيب القائمة ليبدأ بالمفتاح الذي كان يعمل آخر مرة
            if start_key in available_keys:
                available_keys.remove(start_key)
                available_keys.insert(0, start_key)
    except: pass

    # 3. محرك التدوير (Rotation Loop)
    url = "https://api.groq.com/openai/v1/chat/completions"
    
    for key_alias in available_keys:
        try:
            # جلب التوكن الفعلي للمفتاح الحالي في الحلقة
            token_res = supabase.table("system_settings").select("key_value").eq("key_name", key_alias).execute()
            active_token = token_res.data[0]['key_value'] if token_res.data else None
            
            if not active_token:
                continue

            headers = {"Authorization": f"Bearer {active_token}", "Content-Type": "application/json"}
            payload = {
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": "أنت خبير ألغاز محترف. صف الكلمة بذكاء وبدون ذكرها إطلاقاً."},
                    {"role": "user", "content": f"الكلمة: ({answer_text}). وصف قصير و مسلي دقيق باللغة العربية لا يتجاوز 10 كلمات."}
                ],
                "temperature": 0.7
            }

            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=payload, timeout=10.0)
                
                # الحالة (A): النجاح ✅
                if response.status_code == 200:
                    ai_hint = response.json()['choices'][0]['message']['content'].strip()
                    final_hint = (
                        f"💎 <b>〔 تـلـمـيـح ذكـي 〕</b> 💎\n"
                        f"❃╔════════════════╗❃\n\n"
                        f"   <b>📜 الوصف:</b>\n"
                        f"   <i>« {ai_hint} »</i>\n\n"
                        f"❃╚════════════════╝❃"
                    )
                    
                    # تحديث المفتاح الناجح في الإعدادات ليكون هو الأساسي مستقبلاً
                    supabase.table("system_settings").update({"key_value": key_alias}).eq("key_name", "ACTIVE_GROQ_KEY").execute()
                    
                    # حفظ التلميح في الذاكرة السحابية
                    supabase.table("hints").upsert({"word": answer_text, "hint": final_hint}).execute()
                    return final_hint
                
                # الحالة (B): فشل المفتاح الحالي ❌ (سيقوم بتجربة المفتاح التالي في القائمة)
                else:
                    error_status = response.status_code
                    # إرسال تنبيه للمطور عن المفتاح المعطل تحديداً
                    alert_text = (
                        f"⚠️ <b>تنبيه تعطل مفتاح!</b>\n"
                        f"━━━━━━━━━━━━━━\n"
                        f"📌 المفتاح المعطل: <code>{key_alias}</code>\n"
                        f"🚫 كود الخطأ: <code>{error_status}</code>\n"
                        f"🔄 الإجراء: <b>يتم الآن الانتقال للمفتاح التالي تلقائياً...</b>"
                    )
                    await bot.send_message(ADMIN_ID, alert_text, parse_mode="HTML")
                    continue # العودة لبداية الحلقة وتجربة المفتاح التالي

        except Exception as e:
            logging.error(f"Error rotating key {key_alias}: {e}")
            continue

    # 4. تلميح الطوارئ (إذا فشلت جميع المفاتيح في التدوير)
    return (
        f"💡 <b>〔 تلميح بسيط 〕</b>\n"
        f"<b>• الحرف الأول:</b> ( {answer_text[0]} )\n"
        f"<b>• طول الكلمة:</b> {len(answer_text)} حروف"
    )
async def delete_after(message, delay):
    await asyncio.sleep(delay)
    try: 
        await message.delete()
    except Exception: 
        pass
        
# ==========================================
# [2] المحرك الموحد (نسخة الإصلاح والتلميح الناري 🔥)
# ==========================================
async def run_universal_logic(chat_id, questions, quiz_data, owner_name, engine_type):
    random.shuffle(questions)
    overall_scores = {}
    # 🟢 قوائم الصيد للمحرك الخاص
    questions_to_delete = []
    results_to_delete = []

    for i, q in enumerate(questions):
        # 1. استخراج الإجابة والنص حسب نوع المصدر
        if engine_type == "bot":
            ans = str(q.get('correct_answer') or "").strip()
            cat_name = q.get('category') or "بوت"
        elif engine_type == "user":
            ans = str(q.get('answer_text') or q.get('correct_answer') or "").strip()
            cat_name = q['categories']['name'] if q.get('categories') else "عام"
        else:
            ans = str(q.get('correct_answer') or q.get('ans') or "").strip()
            cat_name = "قسم خاص 🔒"

        # 2. تصفير حالة السؤال وتجهيز الذاكرة النشطة
        active_quizzes[chat_id] = {
            "active": True, 
            "ans": ans, 
            "winners": [], 
            "mode": quiz_data['mode'], 
            "hint_sent": False
        }
        
        # --- [ نظام التلميح العادي المنفصل ] ---
        normal_hint_str = ""
        if quiz_data.get('smart_hint'): # إذا فعل المستخدم زر التلميحات
            ans_str = str(ans).strip()
            count_chars = len(ans_str.replace(" ", ""))
            count_words = len(ans_str.split())
            # تنسيق التلميح العادي: عدد الكلمات + أول حرف
            normal_hint_str = f"مكونة من ({count_words}) كلمات، تبدأ بـ ( {ans_str[0]} )"

        # 3. إرسال قالب السؤال (مع الصيد)
        q_msg = await send_quiz_question(chat_id, q, i+1, len(questions), {
            'owner_name': owner_name, 
            'mode': quiz_data['mode'], 
            'time_limit': quiz_data['time_limit'], 
            'cat_name': cat_name,
            'smart_hint': quiz_data.get('smart_hint'),
            'normal_hint': normal_hint_str # تمرير التلميح البنيوي
        })
        if isinstance(q_msg, types.Message):
            questions_to_delete.append(q_msg.message_id)
        
        # 4. محرك الوقت الذكي ومراقبة التلميح الملكي ✨
        start_time = time.time()
        t_limit = int(quiz_data.get('time_limit', 15))
        h_msg = None 
        
        while time.time() - start_time < t_limit:
            if not active_quizzes.get(chat_id) or not active_quizzes[chat_id]['active']:
                break
            
            if quiz_data.get('smart_hint') and not active_quizzes[chat_id]['hint_sent']:
                if (time.time() - start_time) >= (t_limit / 2):
                    try:
                        hint_text = await generate_smart_hint(ans)
                        h_msg = await bot.send_message(chat_id, hint_text, parse_mode="HTML")
                        active_quizzes[chat_id]['hint_sent'] = True
                    except Exception as e:
                        logging.error(f"⚠️ خطأ في التلميح: {e}")

            await asyncio.sleep(0.5)

        if h_msg:
            asyncio.create_task(delete_after(h_msg, 0))

        # 5. إنهاء السؤال وحساب النقاط (التعديل الجوهري 🔥)
        if chat_id in active_quizzes:
            # نغلق السؤال أولاً لمنع استقبال إجابات متأخرة
            active_quizzes[chat_id]['active'] = False
            
            # نجلب الفائزين الذين سجلهم "الرادار" في active_quizzes
            current_winners = active_quizzes[chat_id].get('winners', [])
            
            # 🔥 تحديث النقاط في overall_scores بناءً على الفائزين في الرادار
            for w in current_winners:
                uid = w['id']
                if uid not in overall_scores:
                    overall_scores[uid] = {"name": w['name'], "points": 0}
                
                # إضافة النقاط (تأكد أن الإضافة تتم مرة واحدة فقط لكل سؤال)
                overall_scores[uid]['points'] += 1
        
            # 6. عرض لوحة المبدعين (مع الصيد)
            res_msg = await send_creative_results2(chat_id, ans, current_winners, overall_scores)
            if isinstance(res_msg, types.Message):
                results_to_delete.append(res_msg.message_id)
                
        # --- [ ⏱️ محرك العداد التنازلي المطور لتجنب الـ Flood ] ---
        if i < len(questions) - 1:
            icons = ["🔴", "🟠", "🟡", "🟢", "🔵"]
            try:
                countdown_msg = await bot.send_message(chat_id, f"⌛ استعدوا.. السؤال التالي يبدأ بعد 5 ثواني...")
                
                # سنقوم بالتحديث كل ثانية ونصف أو ثانيتين لتقليل الضغط
                for count in range(4, 0, -2): # تقليل عدد التحديثات (تحديث كل ثانيتين)
                    await asyncio.sleep(2)
                    icon = icons[count] if count < len(icons) else "⚪"
                    try:
                        await countdown_msg.edit_text(f"{icon} استعدوا.. السؤال التالي يبدأ بعد <b>{count}</b> ثواني...")
                    except Exception as e:
                        logging.warning(f"Flood avoidance: {e}")
                        break # توقف عن التحديث إذا ضغط التليجرام
                
                await asyncio.sleep(1.5)
                await countdown_msg.delete()
            except Exception as e:
                logging.error(f"Countdown Error: {e}")
        else:
            await asyncio.sleep(2)
    # 7. إعلان لوحة الشرف النهائية (العرض البصري)
    await send_final_results2(chat_id, overall_scores, len(questions))

    # 🚀 [ الـمـسـتـقـبل الـمـلـكـي : ترحيل البيانات للجدول العالمي ]
    # نقوم بتحويل overall_scores لشكل يتوافق مع المحرك (وضع اللاعبين في مجموعة وهمية واحدة لأنها فردية)
    try:
        # نحولها لشكل { "special_event": overall_scores } لكي يفهمها المحرك كمجموعة فائزة
        data_to_sync = {"special_event": overall_scores}
        
        # استدعاء المحرك مع وضع is_special=True لرفعها في عمود special_wins
        # وتحديد أن المجموعة "special_event" هي الفائزة
        await sync_points_to_global_db(
            group_scores=data_to_sync, 
            winners_list=["special_event"], 
            cat_name="مسابقة خاصة", 
            is_special=True
        )
        logging.info("✅ : تم ترحيل نتائج المسابقة الخاصة للسجل العالمي بنجاح")
    except Exception as e:
        logging.error(f"❌ : فشل ترحيل بيانات المسابقة الخاصة : {e}")

    # 🔥 [ عملية التنظيف الشامل ] 🔥
    # حذف الأسئلة
    for q_mid in questions_to_delete:
        try: 
            await bot.delete_message(chat_id, q_mid)
        except: 
            pass

    # حذف قوالب الإجابة المرحلية
    for r_mid in results_to_delete:
        try: 
            await bot.delete_message(chat_id, r_mid)
        except: 
            pass
            
    logging.info("🧹 : تم تنظيف ساحة المسابقة بنجاح")
    
# ==========================================
# ==========================================

# 1️⃣ صمام الأمان العالمي (خارج الدالة لمنع الطلقة المزدوجة)
active_broadcasts = set()

# 2️⃣ دالة العداد التنازلي المصححة لتجنب أي NameError
async def run_countdown(chat_id):
    try:
        msg = await bot.send_message(chat_id, "⏳ استعدوا.. السؤال القادم بعد: 3")
        for i in range(2, 0, -1):
            await asyncio.sleep(1)
            try: await bot.edit_message_text(f"⏳ استعدوا.. السؤال القادم بعد: {i}", chat_id, msg.message_id)
            except: pass
        await asyncio.sleep(1)
        try: await bot.delete_message(chat_id, msg.message_id)
        except: pass
    except: pass

# 3️⃣ المحرك الرئيسي الموحد (نسخة ياسر المطورة 2026)
# ✅ السطر الجديد (أضف المتغير الرابع):
async def engine_global_broadcast(chat_ids, quiz_data, owner_name, current_quiz_db_id=None):
    input_ids = chat_ids if isinstance(chat_ids, list) else [chat_ids]
    all_chats = list(set(input_ids))

    if not all_chats: return

    # 🔥 [ إضافة قاموس الأسماء هنا ] 🔥
    group_names_map = {}
    try:
        # جلب بيانات المجموعات المشاركة دفعة واحدة لسرعة الأداء
        res = supabase.table("groups_hub").select("group_id, group_name").in_("group_id", all_chats).execute()
        # تحويل النتيجة إلى قاموس يسهل الوصول إليه: {ID: Name}
        group_names_map = {str(item['group_id']): item['group_name'] for item in res.data}
    except Exception as e:
        logging.error(f"⚠️ Error fetching group names: {e}")
    
    # تأمين وجود اسم لكل آيدي حتى لو فشل الجلب
    for cid in all_chats:
        if str(cid) not in group_names_map:
            group_names_map[str(cid)] = f"جروب {cid}"

    # --- [ ب ] منع الطلقة المزدوجة (القفل العالمي) ---
    for cid in all_chats:
        if cid in active_broadcasts:
            logging.warning(f"⚠️ مسابقة نشطة بالفعل في {cid}")
            return
    for cid in all_chats: active_broadcasts.add(cid)

    try:
        # --- [ ج ] جلب وتجهيز الأسئلة ---
        raw_cats = quiz_data.get('cats', [])
        if isinstance(raw_cats, str):
            try: cat_ids_list = json.loads(raw_cats)
            except: cat_ids_list = raw_cats.replace('[','').replace(']','').replace('"','').split(',')
        else: cat_ids_list = raw_cats
        cat_ids = [int(c) for c in cat_ids_list if str(c).strip().isdigit()]

        is_bot = quiz_data.get("is_bot_quiz", False)
        table = "bot_questions" if is_bot else "questions"
        cat_col = "bot_category_id" if is_bot else "category_id"
        
        res_q = supabase.table(table).select("*, categories(name)" if not is_bot else "*").in_(cat_col, cat_ids).execute()
        
        if not res_q.data:
            logging.error(f"⚠️ لم يتم العثور على أسئلة")
            return

        pool = res_q.data
        random.shuffle(pool)
        count = int(quiz_data.get('questions_count', 10))
        selected_questions = pool[:count] 

        total_q = len(selected_questions)
        group_scores = {cid: {} for cid in all_chats}
        messages_to_delete = {cid: [] for cid in all_chats}
        results_to_delete = {cid: [] for cid in all_chats}
        # 🟢 [الخطوة 1] فتح سجل للمسابقة في سوبابيس 
        current_quiz_db_id = None
        try:
            quiz_entry = supabase.table("active_quizzes").insert({
                "quiz_name": f"إذاعة {owner_name}",
                "created_by": 2026, 
                "is_global": True,
                "is_active": True,
                "participants_ids": all_chats, 
                "total_questions": total_q
            }).execute()
            
            if quiz_entry.data:
                current_quiz_db_id = quiz_entry.data[0]['id']
                logging.info(f"✅ تم بدء السجل الرقمي بنجاح ID: {current_quiz_db_id}")

                # 🔥 [ الإضافة الجديدة هنا ] 🔥
                # تسجيل المجموعات رسمياً في جدول المشاركين للربط العالمي
                participants_records = [{"quiz_id": current_quiz_db_id, "chat_id": cid} for cid in all_chats]
                supabase.table("quiz_participants").insert(participants_records).execute()
                logging.info(f"🔗 تم ربط {len(all_chats)} مجموعة بجدول المشاركين")

        except Exception as e:
            logging.error(f"❌ خطأ سوبابيس (بدء المسابقة): {e}")

        # --- [ د ] دورة البث الموحدة ---
        for i, q in enumerate(selected_questions):
            # 🔥 [الإضافة الجوهرية هنا] 🔥
            # تصفير قائمة الممنوعين لهذا السؤال رقم (i+1)
            answered_users_global[i + 1] = [] 

            ans = str(q.get('correct_answer') or q.get('answer_text') or "").strip()
            cat_name = q.get('category') or "عام"
            
            # 🔵 [الخطوة 2] تحديث سوبابيس
            if current_quiz_db_id:
                try:
                    supabase.table("active_quizzes").update({
                        "current_answer": ans,
                        "current_index": i + 1
                    }).eq("id", current_quiz_db_id).execute()
                except: pass
                
            
            # داخل دالة engine_global_broadcast -> حلقة الأسئلة
            for cid in all_chats:
                active_quizzes[cid] = {
                    "active": True,
                    "ans": ans,  # استخدمنا ans ليتطابق مع المتغير فوق
                    "winners": [],
                    "mode": quiz_data.get('mode', 'السرعة ⚡'),
                    "db_quiz_id": current_quiz_db_id, # نستخدم المعرف المرر للدالة
                    "current_index": i + 1,
                    "participants_ids": all_chats  # الحبل السري الذي يربط القروبات
                }

 
            # --- [ تجهيز التلميح العادي المدمج للإذاعة ] ---
            normal_hint_str = ""
            is_hint_on = quiz_data.get('smart_hint', False)
            if is_hint_on:
                ans_str = str(ans).strip()
                count_words = len(ans_str.split())
                # تلميح بنيوي: عدد الكلمات + أول حرف
                normal_hint_str = f"مكونة من ({count_words}) كلمات، تبدأ بـ ( {ans_str[0]} )"

            # 4️⃣ بث السؤال (يجب أن يكون تحت الـ for بـ 12 مسافة)
            send_tasks = [
                send_quiz_question(cid, q, i+1, total_q, {
                    'owner_name': owner_name, # سيظهر اسمك أو اسم المنظم الحقيقي
                    'mode': quiz_data.get('mode', 'السرعة ⚡'),
                    'time_limit': quiz_data.get('time_limit', 15),
                    'cat_name': cat_name,
                    'smart_hint': is_hint_on,
                    'normal_hint': normal_hint_str, # تمرير التلميح المدمج للقالب
                    'is_public': True
                }) for cid in all_chats
            ]
            
            # تنفيذ البث الجماعي (16 مسافة)
            q_msgs = await asyncio.gather(*send_tasks, return_exceptions=True)

            for idx, m in enumerate(q_msgs):
                if isinstance(m, types.Message):
                    messages_to_delete[all_chats[idx]].append(m.message_id)
                    
            # 5️⃣ محرك الانتظار الذكي (النسخة الصاروخية 🚀)
            t_limit = int(quiz_data.get('time_limit', 15))
            start_wait = time.time()

            while time.time() - start_wait < t_limit:
                # فحص هل تم الحسم من أي مجموعة؟
                still_active = any(active_quizzes.get(c, {}).get('active', False) for c in all_chats)
                
                if not still_active:
                    logging.info("⚡ الرادار أعطى إشارة إغلاق.. الانتقال للنتائج فوراً.")
                    break
                
                # تقليل النوم لـ 0.05 لضمان حساسية عالية جداً
                await asyncio.sleep(0.08)
            # 6️⃣ إغلاق السؤال وتحديث النقاط (داخل حلقة الأسئلة)
            res_tasks = []
            
            # 🟢 [إضافة] نجمع كل الفائزين من كل المجموعات في قائمة واحدة "عالمية"
            global_winners = []
            for cid in all_chats:
                global_winners.extend(active_quizzes.get(cid, {}).get('winners', []))
            
            # ترتيب الفائزين عالمياً حسب السرعة (الأسرع هو الأول)
            global_winners = sorted(global_winners, key=lambda x: x.get('time', 0))

            for cid in all_chats:
                if cid in active_quizzes:
                    active_quizzes[cid]['active'] = False
                
                # تحديث نقاط الأعضاء المحليين في هذه المجموعة
                local_winners = active_quizzes.get(cid, {}).get('winners', [])
                for w in local_winners:
                    uid = w['id']
                    uname = w['name'] # اسم العضو الفائز
                    
                    if uid not in group_scores[cid]:
                        group_scores[cid][uid] = {"name": uname, "points": 0}
                    group_scores[cid][uid]['points'] += 10
                    
                    # 🔥 [ الربط العالمي للمجموعات ] 🔥
                    # استدعاء الدالة لتحديث جدول المجموعات في سوبابيس
                    try:
                        # نجلب اسم المجموعة من القاموس الذي عرفته سابقاً
                        gname = group_names_map.get(cid, "مجموعة مجهولة")
                        
                        await update_group_stats(
                            group_id=cid,      # آيدي المجموعة
                            group_name=gname,   # اسم المجموعة
                            user_id=uid,       # آيدي العضو الفائز
                            user_name=uname,    # اسم العضو الفائز
                            points=10          # النقاط المضافة للمجموعة
                        )
                    except Exception as e:
                        logging.error(f"⚠️ خطأ في تحديث إحصائيات المجموعة: {e}")
              
                # 🔵 [التعديل العالمي الشامل]
                res_tasks.append(send_creative_results(
                    chat_id=cid, 
                    correct_ans=ans, 
                    winners=global_winners,      # بطل الجولة (يراه الجميع)
                    group_scores=group_scores,   # ترتيب كل اللاعبين والمجموعات (بدون حذف)
                    is_public=True,              # تفعيل وضع الإذاعة العامة
                    mode=quiz_data.get('mode', 'السرعة ⚡'),
                    group_names=group_names_map  # قاموس الأسماء الذي عرفناه في بداية الدالة
                ))
            
            # 🔥 استبدل السطر القديم بهذا البلوك لصيد مُعرفات رسائل الإجابة
            res_msgs = await asyncio.gather(*res_tasks, return_exceptions=True)
            for idx, rm in enumerate(res_msgs):
                if isinstance(rm, types.Message):
                    results_to_delete[all_chats[idx]].append(rm.message_id)
            
            # (اختياري) عداد تنازلي هنا للسؤال التالي
            # 7️⃣ العداد التنازلي للسؤال القادم
            if i < total_q - 1:
                for cid in all_chats:
                    if cid in active_quizzes:
                        active_quizzes[cid]['winners'] = []
                count_tasks = [run_countdown(cid) for cid in all_chats]
                await asyncio.gather(*count_tasks, return_exceptions=True)
            else:
                await asyncio.sleep(2)

        
        # 8️⃣ النتائج النهائية والتنظيف الرقمي
        for cid in all_chats:
            try: 
                # أ. إرسال لوحة النتائج النهائية للمجموعة
                await send_broadcast_final_results(
                    chat_id=cid, 
                    scores=group_scores, 
                    total_q=total_q, 
                    group_names=group_names_map
                )
            except Exception as e: 
                logging.error(f"Error in final results: {e}")
            
            # ب. تنظيف رسائل الأسئلة والنتائج المؤقتة (Telegram)
            for mid in messages_to_delete.get(cid, []):
                try: await bot.delete_message(cid, mid)
                except: pass

            for r_mid in results_to_delete.get(cid, []):
                try: await bot.delete_message(cid, r_mid)
                except: pass
       
        # 🚀 [ الخطوة الجوهرية: ترحيل النقاط من السجل الرقمي ] 🚀
        try:
            if current_quiz_db_id:
                # 1. جلب "الحصاد الدقيق" من سجل الإجابات بدلاً من الذاكرة
                log_res = supabase.table("answers_log").select("*").eq("quiz_id", current_quiz_db_id).execute()
                
                if log_res.data:
                    # 2. تحديث إحصائيات المجموعات بناءً على السجل الحقيقي
                    # (يمكنك استخدام group_scores هنا فقط لمعرفة من شارك، لكن النقاط تؤخذ من اللوج)
                    for cid in all_chats:
                        # جلب عدد المبدعين الحقيقيين من اللوج لهذه المجموعة
                        members_in_log = len(set([r['user_id'] for r in log_res.data if r['chat_id'] == cid]))
                        supabase.table("groups_global_stats").update({
                            "members_count": members_in_log
                        }).eq("group_id", cid).execute()

                    # 3. المزامنة العالمية (نمرر المعرف ليقوم المحرك بالجرد من اللوج)
                    # ملاحظة: سنعدل الدالة لتقبل quiz_id وتجلب بياناتها من هناك
                    await sync_points_to_global_db(quiz_id=current_quiz_db_id, cat_name=cat_name)
                    logging.info("✅ تم الجرد والترحيل من سجل الإجابات بنجاح.")

                # 🔥 [ التشطيب النهائي: تفريغ سوبابيس ] 🔥
                # نحذف "الأب" وبسبب CASCADE يختفي اللوج والمشاركين فوراً
                supabase.table("active_quizzes").delete().eq("id", current_quiz_db_id).execute()
                logging.info(f"🧹 تم تطهير النظام بالكامل للمسابقة {current_quiz_db_id}")

        except Exception as sync_err:
            logging.error(f"🚨 خطأ أثناء الترحيل من السجل أو التنظيف: {sync_err}")
        

# =======================================
# 4. نظام رصد الإجابات الذكي (ياسر المطور - النسخة الديناميكية)
# ==========================================

def is_answer_correct(user_msg, correct_ans):
    if not user_msg or not correct_ans: return False

    # 1. قاموس تحويل الأرقام (حل مشكلة 20 vs عشرين)
    num_map = {
        # الآحاد (بمختلف الصيغ)
        "واحد": "1", "واحده": "1", "احد": "1",
        "اثنان": "2", "اثنين": "2", "اثنتان": "2", "اثنتين": "2",
        "ثلاثه": "3", "ثلاث": "3",
        "اربع": "4", "اربعه": "4",
        "خمسه": "5", "خمس": "5",
        "سته": "6", "ست": "6",
        "سبعه": "7", "سبع": "7",
        "ثمانيه": "8", "ثمان": "8", "ثماني": "8",
        "تسعه": "9", "تسع": "9",
        "عشره": "10", "عشر": "10",

        # الأعداد المركبة (11-19)
        "احد عشر": "11", "احدى عشر": "11", "احدى عشره": "11",
        "اثنا عشر": "12", "اثني عشر": "12", "اثنتا عشره": "12",
        "ثلاثه عشر": "13", "ثلاث عشر": "13", "ثلاث عشره": "13",
        "اربعه عشر": "14", "اربع عشر": "14",
        "خمسه عشر": "15", "خمس عشر": "15",
        "سته عشر": "16", "ست عشر": "16",
        "سبعه عشر": "17", "سبع عشر": "17",
        "ثمانيه عشر": "18", "ثماني عشر": "18",
        "تسعه عشر": "19", "تسع عشر": "19",

        # العقود
        "عشرين": "20", "عشرون": "20",
        "ثلاثين": "30", "ثلاثون": "30",
        "اربعين": "40", "اربعون": "40",
        "خمسين": "50", "خمسون": "50",
        "ستين": "60", "ستون": "60",
        "سبعين": "70", "سبعون": "70",
        "ثمانين": "80", "ثمانون": "80",
        "تسعين": "90", "تسعون": "90",
        "مائه": "100", "مئة": "100", "مائة": "100",
        "الف": "1000"
    }

    # 2. قاموس الحروف للتهجئة الديناميكية (English to Arabic Char Map)
    char_map = {
        'a': 'ا', 'b': 'ب', 'c': 'ك', 'd': 'د', 'e': 'ا', 'f': 'ف', 'g': 'ج', 
        'h': 'ه', 'i': 'ي', 'j': 'ج', 'k': 'ك', 'l': 'ل', 'm': 'م', 'n': 'ن', 
        'o': 'و', 'p': 'ب', 'q': 'ق', 'r': 'ر', 's': 'س', 't': 'ت', 'u': 'و', 
        'v': 'ف', 'w': 'و', 'x': 'اكس', 'y': 'ي', 'z': 'ز'
    }

    stop_words = ["هو", "هي", "ال", "انه", "انها", "يكون", "يعتبر", "اسمها", "اسمه"]

    def clean_logic(text):
        text = text.strip().lower()
        # تنظيف التشكيل
        text = re.sub(r'[\u064B-\u0652]', '', text) 

        # --- [ المحرك الديناميكي: تحويل الكلمات الإنجليزية لنطق عربي ] ---
        words = text.split()
        translated_words = []
        for w in words:
            # إذا كانت الكلمة تحتوي على حروف إنجليزية، يتم تهجئتها بالعربي حرفاً بحرف
            if any(c.isascii() and c.isalpha() for c in w):
                new_w = ""
                for char in w:
                    new_w += char_map.get(char, char)
                w = new_w
            translated_words.append(w)
        text = " ".join(translated_words)
        
        # توحيد الحروف الضعيفة
        text = re.sub(r'[أإآ]', 'ا', text)
        text = re.sub(r'ة', 'ه', text)
        text = re.sub(r'ى', 'ي', text)
        # إزالة الرموز وعلامات الترقيم
        text = re.sub(r'[^\w\s]', '', text)
        
        words = text.split()
        cleaned_words = []
        for w in words:
            if w.startswith("ال") and len(w) > 4:
                w = w[2:]
            w = num_map.get(w, w)
            if w in stop_words: continue
            cleaned_words.append(w)
        return " ".join(cleaned_words)

    user_clean = clean_logic(user_msg)
    correct_clean = clean_logic(correct_ans)

    # تجهيز القوائم للفحص التفصيلي
    user_words = user_clean.split()
    correct_words = correct_clean.split()

    # 1. التطابق التام (سريع)
    if user_clean == correct_clean:
        return True

    # --- [ نظام الاحتواء المتدرج مع الحماية التقنية ] ---
    # فحص إذا كانت الإجابة تبدأ برقم أو حرف إنجليزي (بعد التنظيف يكون قد تحول لعربي)
    first_word = correct_words[0] if correct_words else ""
    # ملاحظة: التحقق من الأرقام فقط لأن الحروف الإنجليزية تحولت بالفعل
    is_technical = any(char.isdigit() for char in first_word)

    # لا نسمح بنظام الاحتواء إذا كانت الإجابة تقنية/رقمية
    if not is_technical:
        correct_len = len(correct_words)
        
        # أ. إجابة من كلمتين: إذا كتب واحدة منها (بشرط طول الكلمة > 3)
        if correct_len == 2:
            for u_w in user_words:
                if len(u_w) > 3 and u_w in correct_words:
                    return True
        
        # ب. إجابة من 3 كلمات أو أكثر: إذا كتب كلمتين منها على الأقل
        elif correct_len >= 3:
            matched_count = 0
            for u_w in user_words:
                if u_w in correct_words:
                    matched_count += 1
            if matched_count >= 2:
                return True

    # 2. نظام التشابه المرن (Fuzzy Matching) - 80%
    similarity = difflib.SequenceMatcher(None, user_clean, correct_clean).ratio()
    if similarity >= 0.80:
        return True

    return False
# ==========================================
# 🎯 رادار الإجابات الموحد (نسخة ياسر النهائية)
# ==========================================
@dp.message_handler(lambda m: not m.text or not m.text.startswith('/'))
async def unified_answer_checker(m: types.Message):
    cid = m.chat.id
    uid = m.from_user.id
    user_text = m.text.strip() if m.text else ""

    # 1️⃣ فحص المسابقات النشطة (الإذاعة العامة والخاصة)
    if cid in active_quizzes and active_quizzes[cid].get('active'):
        quiz = active_quizzes[cid]
        correct_ans = str(quiz['ans']).strip()
        
        # ⚖️ فحص صحة الإجابة
        if is_answer_correct(user_text, correct_ans):
            
            # 🔥 [نظام منع التكرار العابر للمجموعات] 🔥
            # نفحص كل المجموعات المرتبطة بهذه المسابقة: هل هذا المستخدم (uid) موجود في قائمة الفائزين في أي منها؟
            p_ids = quiz.get('participants_ids', [cid])
            is_already_winner_globally = False
            
            for p_cid in p_ids:
                if p_cid in active_quizzes:
                    if any(w['id'] == uid for w in active_quizzes[p_cid].get('winners', [])):
                        is_already_winner_globally = True
                        break
            
            if is_already_winner_globally:
                # اللاعب أجاب مسبقاً في مجموعة أخرى؛ نتجاهله بصمت أو نرسل تحذير بسيط
                logging.info(f"🚫 محاولة تكرار مرفوضة من {m.from_user.first_name} (ID: {uid})")
                return

            # --- [ إذا وصل الكود هنا، معناه أن هذه أول إجابة صحيحة له في هذه الجولة ] ---

            # 🛑 [نظام الإغلاق العالمي الفوري] ⚡ (في وضع السرعة)
            if quiz.get('mode') == 'السرعة ⚡':
                # إغلاق السؤال في كل المجموعات فوراً لمنع أي شخص آخر من الإجابة
                for p_cid in p_ids:
                    if p_cid in active_quizzes:
                        active_quizzes[p_cid]['active'] = False
                
                logging.info(f"⚡ إغلاق عالمي: البطل {m.from_user.first_name} حسم السؤال.")

            # 💾 حفظ الإجابة في سوبابيس (Answers Log) - [المسابقات العامة]
            db_id = quiz.get('db_quiz_id')
            if db_id:
                def save_to_db():
                    try:
                        supabase.table("answers_log").insert({
                            "quiz_id": db_id,
                            "quiz_type": "public",  # تحديد النوع كعامة
                            "question_no": quiz.get('current_index', 1),
                            "total_quiz_questions": quiz.get('total_questions', 1), # إجمالي الأسئلة للتنظيف
                            "chat_id": cid, 
                            "group_name": m.chat.title, # إضافة اسم المجموعة
                            "user_id": uid, 
                            "user_name": m.from_user.first_name,
                            "answer_text": user_text, 
                            "is_correct": True,
                            "points_earned": 10,
                            "speed_rank": len(quiz.get('winners', [])) + 1 # ترتيب السرعة
                        }).execute()
                    except Exception as e: logging.error(f"❌ خطأ حفظ النتيجة (عامة): {e}")
                
                asyncio.create_task(asyncio.to_thread(save_to_db))

                # تسجيل الفائز في الذاكرة المؤقتة للمجموعة
                quiz['winners'].append({"name": m.from_user.first_name, "id": uid})
                return

            else:
                # ==========================================
                # 🔒 مسار المسابقات الخاصة (نظام داخلي)
                # ==========================================
                # التأكد أن اللاعب لم يفز مسبقاً في هذا السؤال
                if not any(w['id'] == uid for w in quiz.get('winners', [])):
                    
                    # 💾 حفظ الإجابة في سوبابيس (Answers Log) - [المسابقات الخاصة]
                    # بما أن المسابقة خاصة، قد لا تملك db_quiz_id ثابت، نستخدم معرف الشات كمرجع
                    def save_private_to_db():
                        try:
                            supabase.table("answers_log").insert({
                                "quiz_id": None, # أو ضع id المسابقة الخاصة إذا كان متوفراً
                                "quiz_type": "private", # تحديد النوع كخاصة
                                "question_no": quiz.get('current_index', 1),
                                "total_quiz_questions": quiz.get('total_questions', 1),
                                "chat_id": cid,
                                "group_name": m.chat.title,
                                "user_id": uid,
                                "user_name": m.from_user.first_name,
                                "answer_text": user_text,
                                "is_correct": True,
                                "points_earned": 10,
                                "speed_rank": len(quiz.get('winners', [])) + 1
                            }).execute()
                        except Exception as e: logging.error(f"❌ خطأ حفظ النتيجة (خاصة): {e}")
                    
                    asyncio.create_task(asyncio.to_thread(save_private_to_db))

                    # تسجيل الفائز
                    quiz.setdefault('winners', []).append({"name": m.from_user.first_name, "id": uid})
                    
                    if quiz.get('mode') == 'السرعة ⚡':
                        quiz['active'] = False
                        # ملاحظة: المحرك run_universal_logic هو من سيظهر قالب النتائج 2 
                        # فور استشعار أن active أصبحت False
                    return
# ============================================================
# --- [ إعداد حالات الإدارة - Admin States ] ---
# ============================================================
class AdminStates(StatesGroup):
    waiting_for_new_token = State()      
    waiting_for_broadcast = State()      
    waiting_for_broadcast_photo = State()
    waiting_for_key_value = State() # الحالة التي ننتظر فيها النص الجديد للمفتاح

# =========================================
#          👑 غرفة عمليات المطور 👑
# =========================================

def get_main_admin_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📊 إدارة الأسئلة", callback_data="botq_main"),
        InlineKeyboardButton("📝 مراجعة الطلبات", callback_data="admin_view_pending"),
        InlineKeyboardButton("📢 إذاعة عامة", callback_data="admin_broadcast"),
        InlineKeyboardButton("🔄 تحديث النظام", callback_data="admin_restart_now"),
        # التحديث الجديد: زر إدارة مفاتيح GROQ
        InlineKeyboardButton("🔑 مفاتيح GROQ", callback_data="admin_keys_hub") 
    )
    kb.row(InlineKeyboardButton("🔐 استبدال توكين البوت", callback_data="admin_change_token"))
    kb.row(InlineKeyboardButton("❌ إغلاق اللوحة", callback_data="botq_close"))
    return kb

# كيبورد إدارة مفاتيح GROQ (الدور الثاني)
def get_keys_management_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("➕ تحديث/إضافة مفتاح جديد", callback_data="admin_update_any_key"),
        InlineKeyboardButton("💎 تفعيل G_KEY_1", callback_data="gkey_G_KEY_1"),
        InlineKeyboardButton("💎 تفعيل G_KEY_2", callback_data="gkey_G_KEY_2"),
        InlineKeyboardButton("💎 تفعيل G_KEY_3", callback_data="gkey_G_KEY_3"),
        InlineKeyboardButton("🔙 رجوع", callback_data="admin_back")
    )
    return kb

# --- 1. معالج الأمر الرئيسي /admin (المعدل للنظام الموحد) ---
@dp.message_handler(commands=['admin'], user_id=ADMIN_ID)
@dp.message_handler(lambda m: m.text in ['لوحتي', 'المطور', 'غرفة العمليات'], user_id=ADMIN_ID)
async def admin_dashboard(message: types.Message):
    try:
        res = supabase.table("groups_hub").select("*").execute()
        active = len([g for g in res.data if g['status'] == 'active'])
        blocked = len([g for g in res.data if g['status'] == 'blocked'])
        total_global_points = sum([g.get('total_group_score', 0) for g in res.data])

        txt = (
            "👑 <b>غرفة العمليات الرئيسية</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"✅ المجموعات النشطة: <b>{active}</b>\n"
            f"🚫 المجموعات المحظورة: <b>{blocked}</b>\n"
            f"🏆 إجمالي نقاط الهب: <b>{total_global_points:,}</b>\n"
            "━━━━━━━━━━━━━━\n"
            "👇 اختر قسماً لإدارته:"
        )
        await message.answer(txt, reply_markup=get_main_admin_kb(), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Admin Panel Error: {e}")
        await message.answer("❌ خطأ في الاتصال بقاعدة البيانات الموحدة.")

# --- 2. معالج العودة للقائمة الرئيسية (المعدل) ---
@dp.callback_query_handler(lambda c: c.data == "admin_back", user_id=ADMIN_ID, state="*")
async def admin_back_to_main(c: types.CallbackQuery, state: FSMContext):
    await state.finish()
    try:
        res = supabase.table("groups_hub").select("*").execute()
        active = len([g for g in res.data if g['status'] == 'active'])
        blocked = len([g for g in res.data if g['status'] == 'blocked'])
        total_global_points = sum([g.get('total_group_score', 0) for g in res.data])
        
        txt = (
            "👑 <b>غرفة العمليات الرئيسية</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"✅ المجموعات النشطة: <b>{active}</b>\n"
            f"🚫 المجموعات المحظورة: <b>{blocked}</b>\n"
            f"🏆 إجمالي نقاط الهب: <b>{total_global_points:,}</b>\n"
            "━━━━━━━━━━━━━━"
        )
        await c.message.edit_text(txt, reply_markup=get_main_admin_kb(), parse_mode="HTML")
    except Exception as e:
        await c.answer("⚠️ حدث خطأ أثناء تحديث البيانات الموحدة")

# --- 3. قسم إدارة مفاتيح GROQ (التحديث الجديد) ---

@dp.callback_query_handler(text="admin_keys_hub", user_id=ADMIN_ID)
async def show_keys_hub(c: types.CallbackQuery):
    txt = (
        "🔑 <b>إدارة مفاتيح GROQ الاحتياطية</b>\n"
        "━━━━━━━━━━━━━━\n"
        "اختر المفتاح المراد تفعيله للعمل حالياً، أو قم بتحديث مفتاح موجود:"
    )
    await c.message.edit_text(txt, reply_markup=get_keys_management_kb(), parse_mode="HTML")

@dp.callback_query_handler(text="admin_update_any_key", user_id=ADMIN_ID)
async def start_key_update(c: types.CallbackQuery):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🔑 تحديث G_KEY_1", callback_data="target_G_KEY_1"),
        InlineKeyboardButton("🔑 تحديث G_KEY_2", callback_data="target_G_KEY_2"),
        InlineKeyboardButton("🔑 تحديث G_KEY_3", callback_data="target_G_KEY_3"),
        InlineKeyboardButton("🔙 إلغاء", callback_data="admin_keys_hub")
    )
    await c.message.edit_text("🎯 <b>اختر الرقم الذي تريد حفظ المفتاح الجديد فيه:</b>", reply_markup=kb, parse_mode="HTML")

@dp.callback_query_handler(lambda c: c.data.startswith("target_"), user_id=ADMIN_ID)
async def set_target_key(c: types.CallbackQuery, state: FSMContext):
    target = c.data.replace("target_", "")
    await state.update_data(selected_key_name=target)
    await AdminStates.waiting_for_new_token.set() 
    await c.message.answer(f"📥 <b>أرسل الآن مفتاح GROQ الجديد:</b>\nسيتم حفظه في: <code>{target}</code>", parse_mode="HTML")
    await c.answer()

@dp.message_handler(state=AdminStates.waiting_for_new_token, user_id=ADMIN_ID)
async def save_key_to_db(message: types.Message, state: FSMContext):
    new_token = message.text.strip()
    user_data = await state.get_data()
    target_key_name = user_data.get("selected_key_name")

    if not new_token.startswith("gsk_"):
        await message.answer("⚠️ يبدو أن هذا ليس مفتاح Groq صالح. حاول مرة أخرى.")
        return

    try:
        # تحديث السجل المختار والمفتاح النشط فوراً
        supabase.table("system_settings").update({"key_value": new_token}).eq("key_name", target_key_name).execute()
        supabase.table("system_settings").update({"key_value": new_token}).eq("key_name", "ACTIVE_GROQ_KEY").execute()

        await message.answer(f"✅ <b>تم التحديث والتفعيل بنجاح!</b>\n📍 الموقع: <code>{target_key_name}</code>", parse_mode="HTML")
        await state.finish()
        await admin_dashboard(message) 
    except Exception as e:
        await message.answer(f"❌ خطأ في السوبابيس: {e}")
        await state.finish()
# ============================================================
# --- [ معالج تفعيل (تبديل) المفتاح النشط ] ---
# ============================================================

@dp.callback_query_handler(lambda c: c.data.startswith("gkey_"), user_id=ADMIN_ID)
async def activate_key_by_slot(c: types.CallbackQuery):
    """
    هذا المعالج يقرأ القيمة المخزنة في G_KEY_1 أو 2 أو 3 
    ويقوم بنسخها إلى ACTIVE_GROQ_KEY ليعمل بها البوت فوراً.
    """
    selected_slot = c.data.replace("gkey_", "") # استخراج اسم السلوت
    
    try:
        # 1. جلب القيمة من السجل المختار
        res = supabase.table("system_settings").select("key_value").eq("key_name", selected_slot).execute()
        
        if res.data and res.data[0]['key_value']:
            target_token = res.data[0]['key_value']
            
            # 2. تحديث سجل ACTIVE_GROQ_KEY ليكون هو المحرك الحالي
            supabase.table("system_settings").update({
                "key_value": target_token,
                "description": f"Currently active key from {selected_slot}"
            }).eq("key_name", "ACTIVE_GROQ_KEY").execute()
            
            # 3. إشعار المطور بنجاح التبديل
            await c.answer(f"🚀 تم تفعيل {selected_slot} بنجاح!", show_alert=True)
            
            # تحديث نص الرسالة لإظهار المفتاح الحالي
            new_txt = (
                f"✅ <b>تم تغيير المحرك بنجاح!</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"المفتاح النشط الآن: <code>{selected_slot}</code>\n"
                f"تم سحب البيانات من جدول <code>system_settings</code>."
            )
            await c.message.edit_text(new_txt, reply_markup=get_keys_management_kb(), parse_mode="HTML")
        
        else:
            await c.answer(f"❌ خطأ: سجل {selected_slot} فارغ، قم بتحديثه أولاً.", show_alert=True)

    except Exception as e:
        logging.error(f"Activation Error: {e}")
        await c.answer("⚠️ فشل الاتصال بقاعدة البيانات لتفعيل المفتاح.", show_alert=True)
# =========================================
# --- 3. معالج زر التحديث (Restart) ---
@dp.callback_query_handler(text="admin_restart_now", user_id=ADMIN_ID)
async def system_restart(c: types.CallbackQuery):
    await c.message.edit_text("🔄 <b>جاري تحديث النظام وإعادة التشغيل...</b>", parse_mode="HTML")
    await bot.close()
    await storage.close()
    os._exit(0)
# --- 4. معالج زر استبدال التوكين ---
@dp.callback_query_handler(text="admin_change_token", user_id=ADMIN_ID)
async def ask_new_token(c: types.CallbackQuery):
    await c.message.edit_text(
        "📝 <b>أرسل التوكين الجديد الآن:</b>\n"
        "⚠️ سيتم الحفظ في Supabase وإعادة التشغيل فوراً.", 
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ تراجع", callback_data="admin_back"))
    )
    await AdminStates.waiting_for_new_token.set()

    # --- [ إدارة أسئلة البوت الرسمية - نسخة ياسر الملك المحدثة 2026 ] ---

@dp.callback_query_handler(lambda c: c.data.startswith('botq_'), user_id=ADMIN_ID)
async def process_bot_questions_panel(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    action = data_parts[1]

    if action == "close":
        await c.message.delete()
        await c.answer("تم الإغلاق")

    elif action == "main":
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("📥 رفع (Bulk)", callback_data="botq_upload"),
            InlineKeyboardButton("📂 عرض المجلدات", callback_data="botq_viewfolders"),
            InlineKeyboardButton("⬅️ عودة للرئيسية", callback_data="admin_back")
        )
        await c.message.edit_text("🛠️ <b>إدارة الأسئلة (نظام المجلدات)</b>", reply_markup=kb, parse_mode="HTML")

    elif action == "upload":
        await c.message.edit_text(
            "📥 <b>وضع الرفع المطور:</b>\n\n"
            "أرسل الأسئلة بالصيغة التالية:\n"
            "<code>سؤال+إجابة+القسم+المجلد</code>\n\n"
            "أرسل <b>خروج</b> للعودة.", 
            parse_mode="HTML"
        )
        await state.set_state("wait_for_bulk_questions")

    # --- المستوى الأول: عرض المجلدات ---
    elif action == "viewfolders":
        res = supabase.table("folders").select("*").execute()
        if not res.data:
            return await c.answer("⚠️ لا توجد مجلدات مسجلة.", show_alert=True)
        
        kb = InlineKeyboardMarkup(row_width=2)
        for folder in res.data:
            kb.insert(InlineKeyboardButton(f"📁 {folder['name']}", callback_data=f"botq_showcats_{folder['id']}"))
        
        kb.add(InlineKeyboardButton("⬅️ عودة للرئيسية", callback_data="botq_main"))
        await c.message.edit_text("📂 <b>المجلدات الرئيسية:</b>\nاختر مجلداً لعرض أقسامه:", reply_markup=kb, parse_mode="HTML")

    # --- المستوى الثاني: عرض الأقسام داخل المجلد ---
    elif action == "showcats":
        folder_id = data_parts[2]
        res = supabase.table("bot_categories").select("*").eq("folder_id", folder_id).execute()
        
        kb = InlineKeyboardMarkup(row_width=2)
        if res.data:
            for cat in res.data:
                kb.insert(InlineKeyboardButton(f"🏷️ {cat['name']}", callback_data=f"botq_mng_{cat['id']}"))
        else:
            kb.add(InlineKeyboardButton("🚫 لا توجد أقسام هنا", callback_data="none"))
            
        kb.add(InlineKeyboardButton("🔙 عودة للمجلدات", callback_data="botq_viewfolders"))
        await c.message.edit_text("🗂️ <b>الأقسام المتوفرة في هذا المجلد:</b>", reply_markup=kb, parse_mode="HTML")

    # --- المستوى الثالث: إدارة القسم المختار ---
    elif action == "mng":
        cat_id = data_parts[2]
        res = supabase.table("bot_questions").select("id", count="exact").eq("bot_category_id", int(cat_id)).execute()
        q_count = res.count if res.count is not None else 0
        
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton(f"🗑️ حذف جميع الأسئلة ({q_count})", callback_data=f"botq_confdel_{cat_id}"),
            InlineKeyboardButton("🔙 عودة للأقسام", callback_data="botq_viewfolders")
        )
        await c.message.edit_text(
            f"📊 <b>إدارة القسم (ID: {cat_id})</b>\n\n"
            f"عدد الأسئلة المتوفرة: <b>{q_count}</b>\n\n"
            "⚠️ تنبيه: خيار الحذف سيقوم بمسح كافة الأسئلة التابعة لهذا القسم فقط.", 
            reply_markup=kb, parse_mode="HTML"
        )

    # --- نظام الحماية: تأكيد الحذف (نعم / لا) ---
    elif action == "confdel":
        cat_id = data_parts[2]
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ نعم، احذف", callback_data=f"botq_realdel_{cat_id}"),
            InlineKeyboardButton("❌ تراجع (إلغاء)", callback_data=f"botq_mng_{cat_id}")
        )
        await c.message.edit_text(
            "⚠️ <b>تأكيد الحذف النهائي!</b>\n\n"
            "هل أنت متأكد من مسح جميع أسئلة هذا القسم؟\n"
            "لا يمكن التراجع عن هذه العملية بعد التنفيذ.", 
            reply_markup=kb, parse_mode="HTML"
        )

    # تنفيذ الحذف الفعلي
    elif action == "realdel":
        cat_id = data_parts[2]
        try:
            supabase.table("bot_questions").delete().eq("bot_category_id", int(cat_id)).execute()
            await c.answer("✅ تم الحذف بنجاح", show_alert=True)
            await process_bot_questions_panel(c, state) # العودة للرئيسية
        except Exception as e:
            await c.answer(f"❌ خطأ: {e}", show_alert=True)

    await c.answer()

# --- معالج الرفع المطور (سؤال+إجابة+قسم+مجلد) ---
@dp.message_handler(state="wait_for_bulk_questions", user_id=ADMIN_ID)
async def process_bulk_questions(message: types.Message, state: FSMContext):
    if message.text.strip() in ["خروج", "إلغاء", "exit"]:
        await state.finish()
        await message.answer("✅ تم الخروج من وضع الرفع.")
        return

    lines = message.text.split('\n')
    success, error = 0, 0
    
    for line in lines:
        if '+' in line:
            parts = line.split('+')
            if len(parts) == 4:
                q_text, q_ans, cat_name, f_name = [p.strip() for p in parts]
                try:
                    # 1. فحص المجلد
                    f_res = supabase.table("folders").select("id").eq("name", f_name).execute()
                    f_id = f_res.data[0]['id'] if f_res.data else supabase.table("folders").insert({"name": f_name}).execute().data[0]['id']

                    # 2. فحص القسم وربطه
                    c_res = supabase.table("bot_categories").select("id").eq("name", cat_name).execute()
                    if c_res.data:
                        cat_id = c_res.data[0]['id']
                        supabase.table("bot_categories").update({"folder_id": f_id}).eq("id", cat_id).execute()
                    else:
                        cat_id = supabase.table("bot_categories").insert({"name": cat_name, "folder_id": f_id}).execute().data[0]['id']

                    # 3. رفع السؤال
                    supabase.table("bot_questions").insert({
                        "question_content": q_text,
                        "correct_answer": q_ans,
                        "bot_category_id": cat_id,
                        "category": cat_name,
                        "created_by": str(ADMIN_ID)
                    }).execute()
                    success += 1
                except Exception as e:
                    logging.error(f"Error: {e}")
                    error += 1
            else: error += 1
        elif line.strip(): error += 1

    await message.answer(
        f"📊 <b>ملخص الرفع النهائي (ياسر الملك):</b>\n"
        f"✅ نجاح: {success}\n"
        f"❌ فشل: {error}\n\n"
        f"📥 أرسل الدفعة التالية أو أرسل 'خروج'.", 
        parse_mode="HTML"
    )

# ==========================================
# إدارة مجموعات الهب (الموافقة، الحظر، التفعيل)
# ==========================================

# 1. قائمة المجموعات (عرض الحالات: انتظار ⏳، نشط ✅، محظور 🚫)
@dp.callback_query_handler(lambda c: c.data == "admin_view_pending", user_id=ADMIN_ID)
async def admin_manage_groups(c: types.CallbackQuery):
    try:
        res = supabase.table("groups_hub").select("group_id, group_name, status").execute()
        
        if not res.data:
            return await c.answer("📭 لا توجد مجموعات مسجلة بعد.", show_alert=True)
        
        txt = (
            "🛠️ <b>إدارة مجموعات الهب الموحد:</b>\n\n"
            "⏳ = بانتظار الموافقة (Pending)\n"
            "✅ = نشطة وشغالة (Active)\n"
            "🚫 = محظورة (Blocked)\n"
            "━━━━━━━━━━━━━━"
        )
        
        kb = InlineKeyboardMarkup(row_width=1)
        for g in res.data:
            status_icon = "⏳" if g['status'] == 'pending' else "✅" if g['status'] == 'active' else "🚫"
            
            kb.add(
                InlineKeyboardButton(
                    f"{status_icon} {g['group_name']}", 
                    callback_data=f"manage_grp_{g['group_id']}"
                )
            )
        
        kb.add(InlineKeyboardButton("⬅️ العودة للقائمة الرئيسية", callback_data="admin_back"))
        await c.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error viewing groups: {e}")
        await c.answer("❌ خطأ في جلب البيانات")

# 2. لوحة التحكم بمجموعة محددة (إعطاء الصلاحية أو سحبها)
@dp.callback_query_handler(lambda c: c.data.startswith('manage_grp_'), user_id=ADMIN_ID)
async def group_control_options(c: types.CallbackQuery):
    g_id = c.data.split('_')[2]
    res = supabase.table("groups_hub").select("group_name, status").eq("group_id", g_id).execute()
    
    if not res.data: 
        return await c.answer("⚠️ المجموعة غير موجودة.")
    
    g = res.data[0]
    status_map = {'active': 'نشطة ✅', 'pending': 'بانتظار الموافقة ⏳', 'blocked': 'محظورة 🚫'}
    
    txt = (
        f"📍 <b>إدارة المجموعة:</b> {g['group_name']}\n"
        f"🆔 الآيدي: <code>{g_id}</code>\n"
        f"⚙️ الحالة الحالية: <b>{status_map.get(g['status'], g['status'])}</b>\n"
        f"━━━━━━━━━━━━━━"
    )

    kb = InlineKeyboardMarkup(row_width=2)
    if g['status'] != 'active':
        kb.add(InlineKeyboardButton("✅ موافقة وتفعيل", callback_data=f"auth_approve_{g_id}"))
    if g['status'] != 'blocked':
        kb.add(InlineKeyboardButton("🚫 رفض وحظر", callback_data=f"auth_block_{g_id}"))
    
    kb.add(InlineKeyboardButton("⬅️ رجوع للقائمة", callback_data="admin_view_pending"))
    await c.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    
# ==========================================
# 7. معالج العمليات (Admin Callbacks)
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith(('auth_approve_', 'auth_block_')), user_id=ADMIN_ID)
async def process_auth_callback(c: types.CallbackQuery):
    action = c.data.split('_')[1]
    target_id = c.data.split('_')[2]
    
    if action == "approve":
        supabase.table("groups_hub").update({"status": "active"}).eq("group_id", target_id).execute()
        await c.answer("تم تفعيل المجموعة بنجاح! ✅", show_alert=True)
        
        try:
            full_template = (
                f"🎉 <b>تم تفعيل القروب بنجاح!</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"⚙️ الحالة: متصل (Active) ✅\n"
                f"━━━━━━━━━━━━━━\n\n"
                f"🚀 <b>دليلك السريع للبدء:</b>\n"
                f"🔹 <b>تحكم :</b> لوحة الإعدادات ⚙️\n"
                f"🔹 <b>مسابقة :</b> لبدء التنافس 📝\n"
                f"🔹 <b>عني :</b> ملفك الشخصي ونقاطك 👤\n"
                f"🔹 <b>القروبات :</b> الترتيب العالمي 🌍\n\n"
                f"━━━━━━━━━━━━━━"
            )
            await bot.send_message(target_id, full_template, parse_mode="HTML")
        except: pass

    elif action == "block":
        supabase.table("groups_hub").update({"status": "blocked"}).eq("group_id", target_id).execute()
        await c.answer("تم الحظر بنجاح ❌", show_alert=True)
    
    await c.message.delete()
    await admin_manage_groups(c)
# ==========================================
# 5. نهاية الملف: ضمان التشغيل 24/7 (Keep-Alive)
# ==========================================
from aiohttp import web

# دالة الرد على "نغزة" المواقع الخارجية مثل Cron-job
async def handle_ping(request):
    return web.Response(text="Bot is Active and Running! 🚀")

if __name__ == '__main__':
    # 1. إعداد سيرفر ويب صغير في الخلفية للرد على طلبات الـ HTTP
    app = web.Application()
    app.router.add_get('/', handle_ping)
    
    loop = asyncio.get_event_loop()
    runner = web.AppRunner(app)
    loop.run_until_complete(runner.setup())
    
    # 2. تحديد المنفذ (Port): Render يستخدم غالباً 10000، و Koyeb يستخدم ما يحدده النظام
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    # تشغيل السيرفر كـ "مهمة" جانبية حتى لا يعطل البوت
    loop.create_task(site.start())
    print(f"✅ Keep-alive server started on port {port}")

    # 3. إعدادات السجلات والتشغيل النهائي للبوت
    logging.basicConfig(level=logging.INFO)
    
    # بدء استقبال الرسائل (Polling) مع تخطي التحديثات القديمة
    executor.start_polling(dp, skip_updates=True)

                           
