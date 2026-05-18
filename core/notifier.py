import asyncio
from telegram import Bot
from loguru import logger
from config.settings import settings

class TelegramNotifier:
    def __init__(self):
        self.token = getattr(settings, 'TELEGRAM_BOT_TOKEN', '')
        self.chat_id = getattr(settings, 'TELEGRAM_CHAT_ID', '')
        self.active = bool(self.token and self.chat_id)
        
        if self.active:
            self.bot = Bot(token=self.token)
            logger.info("Telegram Notifier core successfully initialized.")
        else:
            logger.warning("Telegram credentials missing or incomplete. Notifier running in log-only mode.")

    async def send_signal_alert(self, signal_data: dict):
        """
        Formats and dispatches a high-conviction trade signal directly to your mobile device.
        """
        direction_emoji = "🏹 LONG SETUP" if signal_data['direction'] == "LONG" else "🎯 SHORT SETUP"
        
        message_text = (
            f"⚡ **QUANT CORE // POSITION EMITTED**\n"
            f"───────────────────\n"
            f"**Asset:** {signal_data['symbol']}\n"
            f"**Action:** {direction_emoji}\n"
            f"**Regime Context:** {signal_data['regime']}\n"
            f"───────────────────\n"
            f"**Entry Range:** {signal_data['entry_range']}\n"
            f"**Stop Loss:** {signal_data['stop_loss']}\n"
            f"**Take Profit 1:** {signal_data['tp1']}\n"
            f"**Take Profit 2:** {signal_data['tp2']}\n"
            f"───────────────────\n"
            f"**XGBoost Probability:** {signal_data['ml_prob']}\n"
            f"**Allocation Risk:** {signal_data['risk_size']}\n"
            f"───────────────────\n"
            f"🕒 *Timestamp:* {signal_data['timestamp']} UTC"
        )

        if self.active:
            try:
                await self.bot.send_message(chat_id=self.chat_id, text=message_text, parse_mode="Markdown")
                logger.success("Signal alert successfully dispatched to Telegram mobile endpoint.")
            except Exception as e:
                logger.error(f"Failed to transmit Telegram dispatch: {e}")
        else:
            # Local console simulation fallback
            logger.info(f"--- SIMULATED TELEGRAM OUTBOUND DISPATCH ---\n{message_text}")
