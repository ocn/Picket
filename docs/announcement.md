# Announcing zk-activity V2: The EVE Online Killmail Bot, Reborn in Rust!

Hello everyone,

I'm thrilled to announce a massive update to **zk-activity**, the EVE Online killmail bot for Discord. This isn't just a minor patch; it's a complete, ground-up rewrite in Rust, and it's packed with new features, performance improvements, and fixes for long-standing issues.

The bot's core mission remains the same: to deliver relevant EVE Online killmails from zkillboard.com directly to your Discord channels. But now, it does so with more power, speed, and precision than ever before.

---

### 🔥 The Rewrite: Performance and Reliability

The entire bot has been rewritten in Rust, a language renowned for its performance and safety. This change brings several key advantages:

*   **Blazing Speed:** We've migrated from the old zKillboard API to a direct **RedisQ** stream. This means killmails are processed and posted to your Discord with significantly lower latency. You'll see the action almost as it happens.
*   **Enhanced Stability:** The new codebase is more robust and reliable, fixing many of the persistent bugs from the previous version.
*   **Feature Parity:** All the functionality you relied on in the old bot has been meticulously reimplemented and improved.

---

### ✨ What's New in V2?

This rewrite was the perfect opportunity to introduce some powerful new features that give you unprecedented control over your killmail feeds.

*   **🚀 Advanced AST-Based Filtering:** At the heart of the new bot is a powerful Abstract Syntax Tree (AST) filtering engine. This allows you to create incredibly specific and complex rules by combining filters with `AND`, `OR`, and `NOT` logic. If you can dream it, you can probably filter for it.

*   **🔔 Optional Pings & Anti-Spam:** You can now configure subscriptions to ping `@here` or `@everyone`. To prevent spam from old kills, you can set a `max_ping_delay`, ensuring that only fresh, actionable intelligence triggers a notification.

*   **🛡️ Simplified Security Status Filtering:** Monitoring specific areas of space is easier than ever. You can now define a security status range (e.g., `"-1.0..=0.0"` for all of nullsec) to filter kills.

*   **🚢 Ship Type & Group Filtering:** Filter by individual ship types (e.g., `Gila`) or entire ship groups (e.g., `Dreadnoughts`, `Interceptors`). This makes it simple to track specific doctrines or classes of ships.

*   **🛰️ Light-Year Range-Based Filtering:** This is a game-changer for situational awareness. You can create a "radar" around one or more key systems, each with its own independent light-year range. Get alerted to any activity within a specific jump radius of your staging, home system, or target area.

---

### 🛠️ Example: The "Capital Radar"

Want to see the new filtering in action? Here’s how you could set up a subscription to ping `@everyone` if a capital ship is killed within 7 light-years of your staging system in Turnur, but only if the kill is less than 10 minutes old.

```
/subscribe id: capitals-radar description: Capitals near Turnur ship_group_ids: 485 ly_ranges_json: [{"system_id":30002086, "range":7.0}] ping_type: Everyone max_ping_delay_minutes: 10
```

---

### 🔗 Get Started Now

*   **Invite the Bot to Your Server:**
    [**Invite zk-activity Bot**](https://discordapp.com/api/oauth2/authorize?client_id=YOUR_CLIENT_ID&permissions=149504&scope=bot)
    *(Server owners will need to replace `YOUR_CLIENT_ID` with the bot's actual client ID).*

*   **Check out the Code:**
    The project is fully open-source! You can find the code, raise issues, and contribute on GitHub:
    [**https://github.com/ocn/zk-activity**](https://github.com/ocn/zk-activity)

Thank you for your continued support. I can't wait to see the creative and powerful filtering setups you all come up with.

Fly safe!
o7
