import os
import json
import random
import discord
import asyncio  # NUEVO
from discord import app_commands
from discord.ext import commands, tasks
from datetime import datetime, timedelta, UTC  # FIX

DATA_FILE = "censo_data.json"  # FIX Railway + Volume
from dotenv import load_dotenv  # NUEVO
load_dotenv()  # NUEVO
GUILD_ID_TEST = int(os.getenv("GUILD_ID_TEST", "0"))  # FIX Railway

# =========================
# Persistencia simple JSON
# =========================
def now_utc():  # FIX
    return datetime.now(UTC)

def load_data() -> dict:
    if not os.path.exists(DATA_FILE):
        return {"guilds": {}}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"guilds": {}}

def save_data(data: dict):
    # NUEVO: asegurar carpeta si DATA_FILE tiene ruta tipo /app/data/...
    try:  # NUEVO
        folder = os.path.dirname(DATA_FILE)  # NUEVO
        if folder:  # NUEVO
            os.makedirs(folder, exist_ok=True)  # NUEVO
    except Exception:  # NUEVO
        pass  # NUEVO

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def ensure_guild(data: dict, guild_id: int) -> dict:
    data.setdefault("guilds", {})
    gid = str(guild_id)
    g = data["guilds"].setdefault(gid, {})
    g.setdefault("active", False)
    g.setdefault("paused", False)
    g.setdefault("busy", False)  # NUEVO: lock para evitar choques start/scheduler
    g.setdefault("censo_id", None)
    g.setdefault("role_id", None)          # rol objetivo
    g.setdefault("role_no_id", None)       # antiguo miembro
    g.setdefault("role_pending_id", None)  # pendiente (opcional)
    g.setdefault("log_channel_id", None)   # canal log público
    g.setdefault("deadline_utc", None)     # ISO
    g.setdefault("attempts_max", 3)        # 1 + 2 reintentos
    g.setdefault("users", {})              # user_id -> info
    g.setdefault("panel_channel_id", None)  # NUEVO
    g.setdefault("panel_message_id", None)  # NUEVO
    g.setdefault("answers_log", [])  # NUEVO: historial de respuestas
    g.setdefault("history", [])      # NUEVO: historial de censos
    return g

# =========================
# View de respuesta en DM
# =========================
class CensoDMView(discord.ui.View):
    def __init__(self, bot: commands.Bot, guild_id: int, censo_id: str, user_id: int):
        super().__init__(timeout=None)
        self.bot = bot
        self.guild_id = guild_id
        self.censo_id = censo_id
        self.user_id = user_id

    async def _apply_answer(self, interaction: discord.Interaction, answer: str):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Este mensaje no es para ti.", ephemeral=True)
            return

        data = load_data()
        g = ensure_guild(data, self.guild_id)

        if not g.get("active") or g.get("censo_id") != self.censo_id:
            await interaction.response.send_message("⚠️ Este censo ya no está activo.", ephemeral=True)
            return

        ukey = str(self.user_id)
        u = g["users"].setdefault(ukey, {
            "status": "PENDING",
            "attempts": 0,
            "last_sent_utc": None,
            "response_utc": None
        })

        if u.get("status") in ("YES", "NO"):
            await interaction.response.send_message("✅ Ya habías respondido. Gracias.", ephemeral=True)
            return

        u["status"] = "YES" if answer == "YES" else "NO"
        u["response_utc"] = now_utc().isoformat()

        # NUEVO: guardar historial de respuestas (últimas 20)
        try:
            g.setdefault("answers_log", [])
            g["answers_log"].append({
                "ts": now_utc().isoformat(),
                "user_id": self.user_id,
                "answer": answer
            })
            g["answers_log"] = g["answers_log"][-20:]
        except Exception:
            pass

        save_data(data)

        # FIX: responder primero a Discord (evita "interrumpido")
        try:  # FIX
            if not interaction.response.is_done():  # FIX
                await interaction.response.send_message("✅ Respuesta registrada. Gracias.", ephemeral=True)  # FIX
        except Exception:  # FIX
            pass  # FIX

        # FIX: refrescar panel DESPUÉS de responder
        await refresh_panel_message(self.bot, self.guild_id)  # FIX

        guild = self.bot.get_guild(self.guild_id)
        if not guild:
            return

        member = guild.get_member(self.user_id)
        if member is None:  # FIX
            try:  # FIX
                member = await guild.fetch_member(self.user_id)  # FIX
            except Exception as e:  # FIX
                print("❌ No pude fetch_member:", repr(e))  # FIX
                member = None  # FIX

        if not member:
            return

        role_target = guild.get_role(int(g["role_id"])) if g.get("role_id") else None
        role_no = guild.get_role(int(g["role_no_id"])) if g.get("role_no_id") else None
        role_pending = guild.get_role(int(g["role_pending_id"])) if g.get("role_pending_id") else None

        log_channel = guild.get_channel(int(g["log_channel_id"])) if g.get("log_channel_id") else None

        # FIX: referencia robusta al "member del bot" (guild.me a veces es None)
        bot_member = guild.me  # FIX
        if bot_member is None:  # FIX
            try:  # FIX
                bot_member = guild.get_member(self.bot.user.id)  # FIX
            except Exception:  # FIX
                bot_member = None  # FIX

        # quitar rol pendiente si existe
        try:
            if role_pending:
                await member.remove_roles(role_pending, reason="Censo OGT: respondió")
        except Exception as e:  # FIX
            print("❌ Error quitando rol pendiente:", repr(e))  # FIX

        if answer == "YES":
            if log_channel:
                await log_channel.send(
                    f"✅ {member.mention} confirmó que **sigue activo**. 🕒 {now_utc().strftime('%Y-%m-%d %H:%M UTC')}"
                )
        else:
            # Quitar rol objetivo + agregar antiguo

            # FIX: diagnóstico rápido antes de tocar roles
            try:  # FIX
                if bot_member:  # FIX
                    print("DEBUG perms manage_roles:", bot_member.guild_permissions.manage_roles)  # FIX
                    print("DEBUG bot top role:", bot_member.top_role, "pos:", bot_member.top_role.position)  # FIX
                else:  # FIX
                    print("DEBUG bot_member: None (no pude obtener member del bot)")  # FIX
                if role_target:  # FIX
                    print("DEBUG target role:", role_target, "pos:", role_target.position)  # FIX
                else:  # FIX
                    print("DEBUG target role: None")  # FIX
            except Exception:  # FIX
                pass  # FIX

            try:
                if role_target:
                    await member.remove_roles(
                        role_target,
                        reason="Censo OGT: indicó que no continúa"
                    )
                    print("✅ Rol objetivo removido a", member, "rol:", role_target.name)  # FIX
            except Exception as e:
                print("❌ Error quitando rol objetivo:", repr(e))  # FIX
                try:  # FIX
                    if bot_member:  # FIX
                        print("   Bot top role:", bot_member.top_role, "| target role:", role_target)  # FIX
                except Exception:  # FIX
                    pass  # FIX

            try:
                if role_no:
                    await member.add_roles(role_no, reason="Censo OGT: antiguo miembro")
                    print("✅ Rol NO agregado a", member, "rol:", role_no.name)  # FIX
            except Exception as e:
                print("❌ Error agregando rol NO:", repr(e))  # FIX

            if log_channel:
                await log_channel.send(
                    f"❌ {member.mention} indicó que **no continuará** → Rol actualizado. 🕒 {now_utc().strftime('%Y-%m-%d %H:%M UTC')}"
                )

        # FIX: evita crash si role_target es None o bot_member es None
        try:  # FIX
            if bot_member:  # FIX
                print("DEBUG perms:", bot_member.guild_permissions.manage_roles)  # FIX
                bot_pos = bot_member.top_role.position  # FIX
            else:  # FIX
                bot_pos = None  # FIX
            target_pos = role_target.position if role_target else None  # FIX
            print("DEBUG hierarchy bot:", bot_pos, "target:", target_pos)  # FIX
        except Exception as e:  # FIX
            print("❌ Error en debug hierarchy:", repr(e))  # FIX

    @discord.ui.button(label="✅ Sí, sigo activo", style=discord.ButtonStyle.success, custom_id="ogt_censo_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._apply_answer(interaction, "YES")

    @discord.ui.button(label="❌ No, me retiro", style=discord.ButtonStyle.danger, custom_id="ogt_censo_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._apply_answer(interaction, "NO")

# =========================
# Selects para configuración
# =========================
class RoleTargetSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(placeholder="Selecciona el rol objetivo (a quien se le hará el censo)", min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        data = load_data()
        g = ensure_guild(data, interaction.guild_id)
        g["role_id"] = self.values[0].id
        save_data(data)
        await interaction.response.send_message(f"✅ Rol objetivo guardado: {self.values[0].mention}", ephemeral=True)

class RoleNoSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(placeholder="Selecciona el rol 'Antiguo miembro' (si responden NO)", min_values=1, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        data = load_data()
        g = ensure_guild(data, interaction.guild_id)
        g["role_no_id"] = self.values[0].id
        save_data(data)
        await interaction.response.send_message(f"✅ Rol NO guardado: {self.values[0].mention}", ephemeral=True)

class RolePendingSelect(discord.ui.RoleSelect):
    def __init__(self):
        super().__init__(placeholder="(Opcional) Rol 'Pendiente de confirmar' (puedes seleccionar 0 o 1)", min_values=0, max_values=1)

    async def callback(self, interaction: discord.Interaction):
        data = load_data()
        g = ensure_guild(data, interaction.guild_id)
        if len(self.values) == 0:
            g["role_pending_id"] = None
            save_data(data)
            await interaction.response.send_message("✅ Rol Pendiente removido (None).", ephemeral=True)
        else:
            g["role_pending_id"] = self.values[0].id
            save_data(data)
            await interaction.response.send_message(f"✅ Rol Pendiente guardado: {self.values[0].mention}", ephemeral=True)

class LogChannelSelect(discord.ui.ChannelSelect):
    def __init__(self):
        super().__init__(
            placeholder="Selecciona el canal público de log (ej. #censo-ogt)",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.text]
        )

    async def callback(self, interaction: discord.Interaction):
        data = load_data()
        g = ensure_guild(data, interaction.guild_id)
        g["log_channel_id"] = self.values[0].id
        save_data(data)
        await interaction.response.send_message(f"✅ Canal log guardado: {self.values[0].mention}", ephemeral=True)

# =========================
# Panel (View) + Botones
# =========================
class CensoPanelView(discord.ui.View):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=None)
        self.bot = bot

        # Selects (3 filas, cada uno ocupa fila completa)
        self.add_item(RoleTargetSelect())   # OK
        self.add_item(RoleNoSelect())       # OK
        # self.add_item(RolePendingSelect())  # (opcional)
        self.add_item(LogChannelSelect())   # OK

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.guild_permissions.administrator or interaction.user.guild_permissions.manage_guild

    async def _refresh(self, interaction: discord.Interaction):
        embed = build_status_embed(interaction.guild_id)
        try:
            await interaction.message.edit(embed=embed, view=self)
        except Exception:
            pass

    # =========================
    # helper "defender" para evitar Unknown interaction (10062)
    # =========================
    async def _defender(self, interaction: discord.Interaction):  # FIX
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
        except Exception:
            pass

    async def _safe_reply(self, interaction: discord.Interaction, content: str):  # FIX
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content, ephemeral=True)
            else:
                await interaction.response.send_message(content, ephemeral=True)
        except Exception:
            pass

    # ✅ 5 botones máximo por fila
    @discord.ui.button(label="▶️ Iniciar (7 días)", style=discord.ButtonStyle.success, row=3)
    async def start_7d(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._defender(interaction)
        ok, msg = await start_censo(self.bot, interaction.guild, deadline_days=7)
        if interaction.guild:
            await refresh_panel_message(self.bot, interaction.guild.id)  # FIX
        await self._safe_reply(interaction, ("✅ " if ok else "❌ ") + msg)
        await self._refresh(interaction)

    @discord.ui.button(label="⏸️ Pausar", style=discord.ButtonStyle.secondary, row=3)
    async def pause(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._defender(interaction)
        data = load_data()
        g = ensure_guild(data, interaction.guild_id)
        if not g.get("active"):
            await self._safe_reply(interaction, "⚠️ No hay censo activo.")
            return
        g["paused"] = True
        save_data(data)
        await self._safe_reply(interaction, "⏸️ Censo pausado.")
        if interaction.guild:
            await refresh_panel_message(self.bot, interaction.guild.id)  # FIX
        await self._refresh(interaction)

    @discord.ui.button(label="▶️ Reanudar", style=discord.ButtonStyle.primary, row=3)
    async def resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._defender(interaction)
        data = load_data()
        g = ensure_guild(data, interaction.guild_id)
        if not g.get("active"):
            await self._safe_reply(interaction, "⚠️ No hay censo activo.")
            return
        g["paused"] = False
        save_data(data)
        await self._safe_reply(interaction, "▶️ Censo reanudado.")
        if interaction.guild:
            await refresh_panel_message(self.bot, interaction.guild.id)  # NUEVO
        await self._refresh(interaction)

    @discord.ui.button(label="⏳ Extender +3 días", style=discord.ButtonStyle.secondary, row=3)
    async def extend_3d(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._defender(interaction)
        data = load_data()
        g = ensure_guild(data, interaction.guild_id)
        if not g.get("active") or not g.get("deadline_utc"):
            await self._safe_reply(interaction, "⚠️ No hay censo activo con deadline.")
            return
        try:
            dl = datetime.fromisoformat(g["deadline_utc"])
        except Exception:
            dl = now_utc()
        if dl.tzinfo is None:  # NUEVO
            dl = dl.replace(tzinfo=UTC)  # NUEVO
        dl = dl + timedelta(days=3)
        g["deadline_utc"] = dl.isoformat()
        save_data(data)
        await self._safe_reply(interaction, "⏳ Deadline extendido +3 días.")
        if interaction.guild:
            await refresh_panel_message(self.bot, interaction.guild.id)  # NUEVO
        await self._refresh(interaction)

    @discord.ui.button(label="📨 Reenviar a pendientes", style=discord.ButtonStyle.primary, row=3)
    async def resend_pending(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._defender(interaction)
        sent = await send_to_pending(self.bot, interaction.guild_id, force=True)
        await self._safe_reply(interaction, f"📨 Envíos disparados ahora: {sent}")
        if interaction.guild:
            await refresh_panel_message(self.bot, interaction.guild.id)  # NUEVO
        await self._refresh(interaction)

    @discord.ui.button(label="🛑 Cerrar censo", style=discord.ButtonStyle.danger, row=4)
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._defender(interaction)
        data = load_data()
        g = ensure_guild(data, interaction.guild_id)
        g["active"] = False
        g["paused"] = False
        g["busy"] = False  # NUEVO
        save_data(data)
        await self._safe_reply(interaction, "🛑 Censo cerrado.")
        if interaction.guild:
            await refresh_panel_message(self.bot, interaction.guild.id)  # NUEVO
        await self._refresh(interaction)

# =========================
# Embeds + Lógica core
# =========================
def build_status_embed(guild_id: int) -> discord.Embed:
    data = load_data()
    g = ensure_guild(data, guild_id)

    users = g.get("users", {}) if g.get("active") else {}  # FIX
    counts = {"YES": 0, "NO": 0, "PENDING": 0, "DM_FAILED": 0, "EXPIRED": 0}
    for u in users.values():
        st = u.get("status", "PENDING")
        counts[st] = counts.get(st, 0) + 1

    e = discord.Embed(title="OGT | Panel Censo de Actividad", color=discord.Color.blurple())
    e.add_field(name="Activo", value=str(bool(g.get("active"))), inline=True)
    e.add_field(name="Pausado", value=str(bool(g.get("paused"))), inline=True)
    e.add_field(name="Rol objetivo", value=f"<@&{g['role_id']}>" if g.get("role_id") else "No configurado", inline=False)
    e.add_field(name="Rol NO (Antiguo miembro)", value=f"<@&{g['role_no_id']}>" if g.get("role_no_id") else "No configurado", inline=False)
    e.add_field(name="Rol Pendiente", value=f"<@&{g['role_pending_id']}>" if g.get("role_pending_id") else "None", inline=False)
    e.add_field(name="Canal log", value=f"<#{g['log_channel_id']}>" if g.get("log_channel_id") else "No configurado", inline=False)
    e.add_field(name="Deadline UTC", value=str(g.get("deadline_utc")), inline=False)
    e.add_field(name="✅ Sí", value=str(counts["YES"]), inline=True)
    e.add_field(name="❌ No", value=str(counts["NO"]), inline=True)
    e.add_field(name="⏳ Pendiente", value=str(counts["PENDING"]), inline=True)
    e.add_field(name="🚫 DM fallido", value=str(counts["DM_FAILED"]), inline=True)
    e.add_field(name="⌛ Vencido", value=str(counts["EXPIRED"]), inline=True)

    # NUEVO: Últimas respuestas (solo si está activo)
    lines = []
    for item in (g.get("answers_log", [])[-10:] if g.get("active") else []):  # FIX
        uid = item.get("user_id")
        ans = item.get("answer")
        ts = item.get("ts")

        try:
            dt = parse_dt_utc(ts)
            ts_unix = int(dt.timestamp())
            when = f"<t:{ts_unix}:R>"
        except Exception:
            when = str(ts)

        emoji = "✅" if ans == "YES" else "❌"
        lines.append(f"{emoji} <@{uid}> — {when}")

    e.add_field(
        name="Últimas respuestas",
        value="\n".join(lines) if lines else ("Sin respuestas aún." if g.get("active") else "Censo inactivo."),
        inline=False
    )
    return e  # FIX

# =========================
# Refresh panel message (edita el panel guardado)
# =========================
async def refresh_panel_message(bot: commands.Bot, guild_id: int):  # NUEVO
    data = load_data()
    g = ensure_guild(data, guild_id)

    ch_id = g.get("panel_channel_id")
    msg_id = g.get("panel_message_id")
    if not ch_id or not msg_id:
        return

    guild = bot.get_guild(guild_id)
    if not guild:
        return

    channel = guild.get_channel(int(ch_id))
    if not channel:
        return

    try:
        msg = await channel.fetch_message(int(msg_id))
    except Exception:
        return

    embed = build_status_embed(guild_id)
    try:
        await msg.edit(embed=embed, view=CensoPanelView(bot))
    except Exception:
        pass

# =========================
# parse dt utc
# =========================
def parse_dt_utc(dt_iso: str | None) -> datetime:  # FIX
    if not dt_iso:
        return now_utc()
    try:
        dt = datetime.fromisoformat(dt_iso)
    except Exception:
        return now_utc()

    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)  # FIX
    return dt.astimezone(UTC)  # FIX

# =========================
# Def Start Censo.
# =========================
async def start_censo(bot: commands.Bot, guild: discord.Guild, deadline_days: int = 7):
    data = load_data()
    g = ensure_guild(data, guild.id)

    # NUEVO: lock
    if g.get("busy"):  # NUEVO
        return False, "⚠️ El censo está ocupado (intenta de nuevo en 10s)."  # NUEVO
    g["busy"] = True  # NUEVO
    save_data(data)   # NUEVO

    # validar config
    if not g.get("role_id") or not g.get("role_no_id") or not g.get("log_channel_id"):
        g["busy"] = False  # NUEVO
        save_data(data)    # NUEVO
        return False, "Falta configurar Rol objetivo, Rol NO o Canal log (usa el panel)."

    role_target = guild.get_role(int(g["role_id"]))
    role_no = guild.get_role(int(g["role_no_id"]))
    log_channel = guild.get_channel(int(g["log_channel_id"]))

    if not role_target or not role_no or not log_channel:
        g["busy"] = False  # NUEVO
        save_data(data)    # NUEVO
        return False, "No encontré el rol/canal por ID. Revisa selección en el panel."

    # activar
    censo_id = f"{guild.id}-{int(now_utc().timestamp())}"
    g["censo_id"] = censo_id
    g["active"] = True
    g["paused"] = False
    deadline = now_utc() + timedelta(days=int(deadline_days))
    g["deadline_utc"] = deadline.isoformat()

    # NUEVO: limpiar log para que panel nuevo no confunda
    g["answers_log"] = []  # NUEVO

    # guardar historial del censo anterior si existía
    try:  # NUEVO
        if g.get("users"):  # NUEVO
            g.setdefault("history", [])  # NUEVO
            g["history"].append({  # NUEVO
                "censo_id": g.get("censo_id"),
                "deadline_utc": g.get("deadline_utc"),
                "users": g.get("users")
            })
            g["history"] = g["history"][-5:]  # NUEVO
    except Exception:  # NUEVO
        pass  # NUEVO

    g["users"] = {}  # (nuevo censo = reset estados)

    # congelar miembros actuales del rol
    g.setdefault("users", {})
    for m in role_target.members:
        ukey = str(m.id)
        if ukey not in g["users"]:
            g["users"][ukey] = {
                "status": "PENDING",
                "attempts": 0,
                "last_sent_utc": None,
                "response_utc": None
            }
        else:
            if g["users"][ukey].get("status") not in ("YES", "NO"):
                g["users"][ukey]["status"] = "PENDING"

        # rol pendiente opcional (throttle anti-429)
        if g.get("role_pending_id"):
            rp = guild.get_role(int(g["role_pending_id"]))
            if rp and rp not in m.roles:
                try:
                    await m.add_roles(rp, reason="Censo OGT: pendiente de confirmar")
                    await asyncio.sleep(random.uniform(0.8, 1.6))  # throttle anti-429
                except Exception:
                    pass

    save_data(data)

    deadline_ts = int(deadline.timestamp())

    await log_channel.send(
        f"📣 **CENSO DE ACTIVIDAD – OGT | Hell Let Loose**\n"
        f"Rol objetivo: <@&{g['role_id']}>\n"
        f"⏰ Deadline: <t:{deadline_ts}:F> (<t:{deadline_ts}:R>)\n"
        f"📨 El bot enviará DM (anti-spam: 1 + 2 reintentos)."
    )

    # envío inicial
    sent = await send_to_pending(bot, guild.id, force=True)

    # NUEVO: unlock
    data = load_data()  # NUEVO
    g = ensure_guild(data, guild.id)  # NUEVO
    g["busy"] = False  # NUEVO
    save_data(data)    # NUEVO

    return True, f"Censo iniciado. DMs enviados ahora: {sent}"

def should_send_next(attempts: int, last_sent_iso: str | None) -> bool:
    if not last_sent_iso:
        return True
    try:
        last_dt = datetime.fromisoformat(last_sent_iso)
    except Exception:
        return True
    if last_dt.tzinfo is None:  # NUEVO
        last_dt = last_dt.replace(tzinfo=UTC)  # NUEVO
    hours = (now_utc() - last_dt).total_seconds() / 3600.0
    return hours >= 24

async def send_to_pending(bot: commands.Bot, guild_id: int, force: bool = False) -> int:
    data = load_data()
    g = ensure_guild(data, guild_id)

    if not g.get("active") or g.get("paused"):
        return 0

    if g.get("busy"):  # NUEVO: no enviar si está iniciando censo
        return 0

    guild = bot.get_guild(guild_id)
    if not guild:
        return 0

    log_channel = guild.get_channel(int(g["log_channel_id"])) if g.get("log_channel_id") else None

    deadline = parse_dt_utc(g.get("deadline_utc"))
    deadline_ts = int(deadline.timestamp())
    attempts_max = int(g.get("attempts_max", 3))
    sent_now = 0

    user_items = list(g.get("users", {}).items())
    random.shuffle(user_items)

    for uid, u in user_items:
        status = u.get("status", "PENDING")

        if status in ("YES", "NO", "EXPIRED"):
            continue

        if now_utc() > deadline:
            u["status"] = "EXPIRED"
            continue

        attempts = int(u.get("attempts", 0))
        if attempts >= attempts_max:
            continue

        if not force and not should_send_next(attempts, u.get("last_sent_utc")):
            continue

        member = guild.get_member(int(uid))
        if not member:
            continue

        if status == "DM_FAILED" and attempts >= 1:
            continue

        censo_id = g.get("censo_id") or "NA"
        view = CensoDMView(bot, guild_id, censo_id, int(uid))
        dl_text = deadline.strftime("%Y-%m-%d %H:%M UTC")

        if attempts == 0:
            content = (
                "👋 **Soldado de OGT**, \n\n"
                "Estamos realizando una **actualización de actividad** del clan en *Hell Let Loose*.\n\n"
                "👉 ¿Vas a **seguir activo** con OGT?\n\n"
                f"⏰ **Fecha límite:** <t:{deadline_ts}:F>\n"
                f"⌛ **Tiempo restante:** <t:{deadline_ts}:R>\n\n"
                "✅ Si respondes **Sí**, mantienes tu rol.\n"
                "❌ Si respondes **No**, pasarás a **Antiguo miembro OGT**.\n\n"
                "— Staff OGT"
            )
        else:
            content = (
                "⏰ **Recordatorio OGT**\n\n"
                "Aún no hemos recibido tu respuesta al censo de actividad.\n"
                f"Por favor confirma antes del **{dl_text}** usando los botones.\n\n"
                "— Staff OGT"
            )

        try:
            await member.send(content=content, view=view)

            u["status"] = "PENDING"
            u["attempts"] = attempts + 1
            u["last_sent_utc"] = now_utc().isoformat()
            sent_now += 1

            # FIX: throttle simple (evita tz naive/aware y reduce 429)
            await asyncio.sleep(random.uniform(1.2, 3.0))  # FIX

        except discord.Forbidden:
            u["status"] = "DM_FAILED"
            u["attempts"] = attempts + 1
            u["last_sent_utc"] = now_utc().isoformat()
            if log_channel and member:
                try:
                    await log_channel.send(
                        f"🚫 {member.mention} — No fue posible enviar DM (mensajes privados cerrados). "
                        f"📌 Debe confirmar con el staff antes del deadline."
                    )
                except Exception:
                    pass
        except Exception:
            pass

    save_data(data)

    # NUEVO: refrescar panel después de envíos (para que se vea en tiempo real)
    try:  # NUEVO
        await refresh_panel_message(bot, guild_id)  # NUEVO
    except Exception:  # NUEVO
        pass  # NUEVO

    return sent_now

# =========================
# Bot + Slash commands
# =========================
intents = discord.Intents.default()
intents.members = True  # necesario para roles/members
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.tree.command(name="censo_set_pendiente", description="Configura (opcional) el rol 'Pendiente de confirmar' para el censo.")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(rol="Selecciona el rol pendiente (o deja vacío para quitarlo)")
async def censo_set_pendiente(interaction: discord.Interaction, rol: discord.Role | None = None):
    data = load_data()
    g = ensure_guild(data, interaction.guild_id)
    g["role_pending_id"] = rol.id if rol else None
    save_data(data)
    await interaction.response.send_message(
        f"✅ Rol Pendiente actualizado: {rol.mention if rol else 'None (quitado)'}",
        ephemeral=True
    )

@bot.event
async def on_ready():
    print(f"✅ Conectado como {bot.user} (ID: {bot.user.id})")

@bot.tree.command(name="censo_panel", description="Muestra el panel staff del censo OGT.")
@app_commands.checks.has_permissions(manage_guild=True)
async def censo_panel(interaction: discord.Interaction):
    embed = build_status_embed(interaction.guild_id)

    data = load_data()  # NUEVO
    g = ensure_guild(data, interaction.guild_id)  # NUEVO

    # NUEVO: si ya existe panel guardado, lo editamos (no duplicar)
    ch_id = g.get("panel_channel_id")  # NUEVO
    msg_id = g.get("panel_message_id")  # NUEVO
    if ch_id and msg_id:
        try:
            channel = interaction.guild.get_channel(int(ch_id))
            if channel:
                msg = await channel.fetch_message(int(msg_id))
                await msg.edit(embed=embed, view=CensoPanelView(bot))
                await interaction.response.send_message("✅ Panel actualizado.", ephemeral=True)
                return
        except Exception:
            pass

    # FIX: público (ephemeral=False)
    await interaction.response.send_message(embed=embed, view=CensoPanelView(bot), ephemeral=False)  # FIX
    msg = await interaction.original_response()  # NUEVO

    g["panel_channel_id"] = interaction.channel_id  # NUEVO
    g["panel_message_id"] = msg.id  # NUEVO
    save_data(data)  # NUEVO

@bot.tree.command(name="censo_iniciar", description="Inicia el censo (si ya configuraste todo en el panel).")
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(dias="Días hasta el deadline (ej. 7)")
async def censo_iniciar(interaction: discord.Interaction, dias: int = 7):
    await interaction.response.defer(ephemeral=True)  # NUEVO: evita expirar
    ok, msg = await start_censo(bot, interaction.guild, deadline_days=dias)
    await interaction.followup.send(("✅ " if ok else "❌ ") + msg, ephemeral=True)  # FIX
    if interaction.guild:
        await refresh_panel_message(bot, interaction.guild.id)  # NUEVO

@bot.tree.command(name="censo_reenviar_pendientes", description="Dispara envío inmediato solo a pendientes (no a los que ya respondieron).")
@app_commands.checks.has_permissions(manage_guild=True)
async def censo_reenviar_pendientes(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    sent = await send_to_pending(bot, interaction.guild_id, force=True)
    await interaction.followup.send(f"📨 Envíos disparados ahora: {sent}", ephemeral=True)

@tasks.loop(minutes=10)
async def censo_scheduler():
    try:
        data = load_data()
        for gid_str, g in list(data.get("guilds", {}).items()):
            try:
                gid = int(gid_str)
            except Exception:
                continue
            if not g.get("active") or g.get("paused") or g.get("busy"):
                continue
            await send_to_pending(bot, gid, force=False)
    except Exception as e:
        print("❌ Error en censo_scheduler:", repr(e))

@censo_scheduler.before_loop
async def before_scheduler():
    await bot.wait_until_ready()

@bot.command()
@commands.is_owner()
async def sync(ctx: commands.Context):
    synced = await bot.tree.sync()
    await ctx.send(f"Synced {len(synced)} comandos.")

# --- setup_hook ---
async def _setup_hook():  # FIX
    if not censo_scheduler.is_running():
        censo_scheduler.start()

    # FIX: si GUILD_ID_TEST no está definido, no intentes sync guild (evita 403 Missing Access)
    if GUILD_ID_TEST and GUILD_ID_TEST != 0:  # FIX
        try:  # FIX
            guild = discord.Object(id=GUILD_ID_TEST)  # FIX
            bot.tree.copy_global_to(guild=guild)  # FIX
            await bot.tree.sync(guild=guild)  # FIX
            print("✅ Slash commands sincronizados (guild test).")  # FIX
        except Exception as e:  # FIX
            print("⚠️ No pude sync guild test:", repr(e))  # FIX
    else:  # NUEVO
        try:  # NUEVO
            await bot.tree.sync()  # NUEVO (global)
            print("✅ Slash commands sincronizados (global).")  # NUEVO
        except Exception as e:  # NUEVO
            print("⚠️ No pude sync global:", repr(e))  # NUEVO

bot.setup_hook = _setup_hook  # FIX

if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("Falta DISCORD_TOKEN en variables de entorno.")
    bot.run(token)
