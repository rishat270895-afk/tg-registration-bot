# Telegram Registration Bot (Railway)

## Files
- main.py — bot code
- requirements.txt — Python deps
- Procfile — tells Railway how to run the bot

## Railway setup
1) Create a new Railway project -> Deploy from GitHub repo (this repo).
2) Railway -> Variables:
   - BOT_TOKEN = token from @BotFather
   - RESET_PASSWORD = password for wiping DB from admin menu
3) Edit main.py and set:
   - ADMIN_IDS = {admin1_id, admin2_id}

## Admin
- /admin opens admin menu
- Export today button exports today's registrations (UTC)
- Reset DB requires RESET_PASSWORD and confirmation
