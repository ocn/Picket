# 02 — Rename the GitHub repository

**What to do:** Rename `ocn/zk-activity` to `ocn/picket`, set the description and topics, and fix the local remote and README shields. GitHub redirects the old URL, so nothing external breaks.

**Blocked by:** None. Human action: this is the one step with an outward effect.

**Status:** ready-for-human

```
gh repo rename picket --repo ocn/zk-activity --yes
gh repo edit ocn/picket \
  --description "Picket is an EVE Online intel bot for Discord. Killmails, sov timers, public contracts, and watchlists, filtered per channel." \
  --add-topic eve-online --add-topic discord-bot --add-topic esi --add-topic zkillboard --add-topic rust \
  --remove-topic redis --remove-topic redisq --remove-topic eveonline --remove-topic bot --remove-topic discord
git remote set-url origin git@ocn-github.com:ocn/picket.git
sed -i '' 's#ocn/zk-activity#ocn/picket#g' README.md
```

- [ ] `gh repo view ocn/picket` shows the new name and description.
- [ ] `git fetch origin` works against the new remote.
- [ ] README shields render under the new path.
