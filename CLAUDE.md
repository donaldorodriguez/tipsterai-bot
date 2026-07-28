# tipster-bot

Bot de Telegram de picks deportivos (fútbol) con IA. Todo el código vive en `bot.js` (un solo archivo, ~600K). Responde siempre en español.

## Arquitectura de dos repos

- **tipster-bot** (este repo): bot de Telegram + servidor Express con la API del SuperPick.
- **~/infoproducto-validator** (repo aparte): bot de WhatsApp (multi-negocio, incluye TipsterAI). Consume la API del SuperPick de este repo vía HTTP. Su handler relevante: `src/bot/services/superpickService.js` y `src/bot/webhooks/whatsapp.webhook.js`.

Los dos se despliegan por separado en Railway y NO comparten deploy.

## Despliegue — IMPORTANTE

**No se despliega vía git push a GitHub.** `main` local va adelante de `origin/main` (muchos commits sin pushear); el push a GitHub es opcional/informativo, no dispara el deploy.

**Deploy real:** `railway up` ejecutado desde `~/tipster-bot` (el worktree principal, con el proyecto Railway ya linkeado — `railway status` debe mostrar `jubilant-purpose / tipsterai-bot`).

**Flujo típico al trabajar en un worktree/rama:**
1. Commit en la rama de trabajo.
2. `git -C ~/tipster-bot merge --ff-only <rama>` (mergear a main en el worktree principal).
3. `(cd ~/tipster-bot && railway up)` para desplegar.

**Gotcha crítico:** `railway up` con `--detach` retorna antes de que el proceso realmente reinicie. Dos `railway up` seguidos con poco tiempo de diferencia pueden pisarse (el segundo no reinicia nada). **Siempre confirmar el reinicio** antes de dar un fix por desplegado: revisar los logs (`railway logs`) buscando el heartbeat `💓 OK ... | silencio: Xmin` — debe resetearse a un valor bajo tras el deploy — o usar `railway up` en foreground y esperar el build.

Verificar salud tras cada deploy: `curl https://tipsterai-bot-production.up.railway.app/health`.

## Acceso a producción

- **URL pública:** `https://tipsterai-bot-production.up.railway.app`
- **Token API:** variable de entorno `SUPERPICK_API_TOKEN` en Railway; copia local en `~/tipster-bot/.superpick_token_local` (gitignored, nunca commitear).
- **Leer data cruda de producción** (picks/superpicks del volumen persistente): `railway ssh "cat /data/picks.json"` o `/data/superpicks.json` (correr desde `~/tipster-bot`, requiere estar linkeado al proyecto).
- **Cuota de API-Football:** plan free, 100 requests/día TOTAL, compartida con producción en vivo. Evitar curls de diagnóstico contra APIF; usar Highlightly o logs para diagnosticar en su lugar.

## Endpoints propios (Bearer `$SUPERPICK_API_TOKEN`)

- `GET /api/superpick` — el SuperPick actual a servir a un lead (bloquea al primer servicio).
- `GET /api/superpick/plan` — plan completo del día, read-only: `picks` (SuperPicks), `extras` (ExtraPicks), `valor` (Picks de Valor).
- `GET /api/superpick/evaluados?horas=N` — SuperPicks liquidados recientes.
- `GET /api/brief` — resumen de ayer + plan de hoy en un solo shot (no gasta cuota, solo lee JSON local).
- `GET /health` — sin auth.

## Sistema de picks — tres niveles

1. **SuperPicks** (`picks`): prob≥60% Y EV≥8%, máx 5/día, tope 2 por familia de mercado, sin espaciado horario.
2. **ExtraPicks** (`extras`): EV≥8% Y prob≥55%, máx 8, 1 por partido.
3. **Picks de Valor** (`valor`): favoritos sólidos prob≥63% con EV 5-8%, cuota≥1.60, máx 5.

El detalle de por qué existen estos umbrales y su historia vive en memoria (no aquí, para no duplicar).

## Notas operativas

- El bot de WhatsApp avisa al admin automáticamente cuando entra/sale un SuperPick (vía `notifySuperPickWhatsApp` → `SUPERPICK_NOTIFY_URL`).
- El plan se refresca solo cada ~90 min desde las 6am Col (`SUPERPICK_BUILD_FROM_HOUR`); no forzar refrescos manuales innecesarios (gasta cuota APIF).
- Historial de fixes, decisiones y feedback del usuario: ver memoria (`~/.claude/projects/.../memory/`), no este archivo.
