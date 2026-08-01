#!/usr/bin/env node
/**
 * Acierto real de los SuperPicks, desglosado POR MERCADO y por día.
 *
 * Existe porque el track record global (66%, 75%…) esconde qué mercado está
 * hundiendo el resultado. El 1-ago-2026 los córners POR EQUIPO iban 1W-4L (20%)
 * y eran el 42% de los picks emitidos: el número global no lo mostraba y solo
 * salió a la luz porque el usuario lo notó a ojo. Esto lo vuelve medible.
 *
 *   node analiza_mercados.js            # últimas 72 h
 *   node analiza_mercados.js 168        # última semana (máximo del endpoint)
 *
 * Token: lo lee de SUPERPICK_API_TOKEN o del archivo .superpick_token_local.
 */
const fs = require('fs');
const path = require('path');

const HORAS = Math.min(parseInt(process.argv[2], 10) || 72, 168);
const BASE  = process.env.BOT_URL || 'https://tipsterai-bot-production.up.railway.app';

function token() {
  if (process.env.SUPERPICK_API_TOKEN) return process.env.SUPERPICK_API_TOKEN.trim();
  const f = path.join(__dirname, '.superpick_token_local');
  if (fs.existsSync(f)) return fs.readFileSync(f, 'utf8').trim();
  console.error('❌ Falta el token: exporta SUPERPICK_API_TOKEN o crea .superpick_token_local');
  process.exit(1);
}

// Familia de mercado a partir del texto de la selección.
// El orden importa: "Corners Visitante Over 3.5" tiene que caer en "por equipo",
// no en "totales".
function familia(sel = '') {
  const t = sel.toLowerCase();
  if (/combinada|\+/.test(t))                                   return 'Combinada';
  if (/c[óo]rner/.test(t) && /(local|visitante)/.test(t))       return 'Córners por equipo';
  if (/c[óo]rner/.test(t))                                      return 'Córners totales';
  if (/tarjeta|amarilla/.test(t))                               return 'Tarjetas';
  if (/ambos marcan|btts/.test(t))                              return 'BTTS';
  if (/gol/.test(t))                                            return 'Goles';
  if (/doble oportunidad|1x|x2|dnb|empate no/.test(t))          return 'Doble oportunidad / DNB';
  if (/h[áa]ndicap|asi[áa]tico/.test(t))                        return 'Hándicap';
  if (/victoria|gana/.test(t))                                  return 'Resultado';
  return 'Otros';
}

const pct  = o => (o.W + o.L) ? ((o.W / (o.W + o.L)) * 100).toFixed(0) + '%' : '—';
const roi  = o => {
  const jug = o.W + o.L;
  if (!jug) return '—';
  const r = ((o.retorno - jug) / jug) * 100;
  return (r >= 0 ? '+' : '') + r.toFixed(1) + '%';
};

(async () => {
  const res = await fetch(`${BASE}/api/superpick/evaluados?horas=${HORAS}`, {
    headers: { Authorization: `Bearer ${token()}` },
  });
  if (!res.ok) {
    console.error(`❌ HTTP ${res.status} — ¿token correcto? ¿bot arriba?`);
    process.exit(1);
  }
  const picks = (await res.json()).evaluados || [];
  const jugados = picks.filter(p => p.resultado === 'W' || p.resultado === 'L');

  if (!jugados.length) {
    console.log(`Sin picks liquidados en las últimas ${HORAS} h.`);
    return;
  }

  const acc = (mapa, clave, p) => {
    const o = mapa[clave] || (mapa[clave] = { W: 0, L: 0, retorno: 0, items: [] });
    o[p.resultado]++;
    if (p.resultado === 'W') o.retorno += (parseFloat(p.cuota) || 1);
    o.items.push(p);
    return o;
  };

  const porDia = {}, porMercado = {};
  for (const p of jugados) {
    const dia = new Date(p.emitidoAt).toLocaleDateString('es-CO', {
      timeZone: 'America/Bogota', weekday: 'long', day: '2-digit', month: 'short',
    });
    acc(porDia, dia, p);
    acc(porMercado, familia(p.seleccion), p);
  }

  console.log(`\n📅 ÚLTIMAS ${HORAS} HORAS — ${jugados.length} picks liquidados\n`);

  console.log('═══ POR DÍA ═══');
  for (const [dia, o] of Object.entries(porDia)) {
    console.log(`\n${dia} → ${o.W}W-${o.L}L (${pct(o)})`);
    for (const p of o.items) {
      const icono = p.resultado === 'W' ? '✅' : '❌';
      console.log(`   ${icono} ${String(p.seleccion).padEnd(34)} @${p.cuota}  ${p.local} vs ${p.visitante}`);
    }
  }

  console.log('\n═══ POR MERCADO ═══');
  const filas = Object.entries(porMercado).sort((a, b) => (b[1].W + b[1].L) - (a[1].W + a[1].L));
  console.log('mercado'.padEnd(26) + 'récord'.padEnd(10) + 'acierto'.padEnd(10) + 'ROI'.padEnd(9) + '% del volumen');
  for (const [m, o] of filas) {
    const vol = (((o.W + o.L) / jugados.length) * 100).toFixed(0) + '%';
    console.log(
      m.padEnd(26) +
      `${o.W}W-${o.L}L`.padEnd(10) +
      pct(o).padEnd(10) +
      roi(o).padEnd(9) +
      vol
    );
  }

  const tot = Object.values(porMercado).reduce(
    (a, o) => ({ W: a.W + o.W, L: a.L + o.L, retorno: a.retorno + o.retorno }),
    { W: 0, L: 0, retorno: 0 }
  );
  console.log('\n' + 'TOTAL'.padEnd(26) + `${tot.W}W-${tot.L}L`.padEnd(10) + pct(tot).padEnd(10) + roi(tot));

  // Señal accionable: mercados con muestra suficiente y acierto pobre.
  const sospechosos = filas.filter(([, o]) => (o.W + o.L) >= 4 && (o.W / (o.W + o.L)) < 0.45);
  if (sospechosos.length) {
    console.log('\n⚠️  Mercados con ≥4 picks y menos de 45% de acierto:');
    for (const [m, o] of sospechosos) console.log(`   • ${m} — ${o.W}W-${o.L}L (${pct(o)})`);
    console.log('   Muestra chica: es señal para vigilar, no veredicto.');
  }
})().catch(e => { console.error('❌', e.message); process.exit(1); });
