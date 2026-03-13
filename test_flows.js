require('dotenv').config();
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const API = axios.create({
  baseURL: 'https://v3.football.api-sports.io',
  headers: { 'x-apisports-key': process.env.APIFOOTBALL_KEY },
  timeout: 15000,
});

// logging
API.interceptors.request.use(req => {
  const p = new URLSearchParams(req.params || {}).toString();
  console.log(`🔍 API: ${req.url}${p ? '?' + p : ''}`);
  return req;
});
API.interceptors.response.use(res => {
  console.log(`📊 results=${res.data.results ?? '?'} errors=${JSON.stringify(res.data.errors || [])}`);
  return res;
});

const LEAGUE_SEASONS = { 78:2025, 140:2025, 39:2025 };
const liveCache = { raw: null, ts: 0 };

async function fetchLiveRaw() {
  if (Date.now() - liveCache.ts < 30000 && liveCache.raw) return liveCache.raw;
  const { data } = await API.get('/fixtures', { params: { live: 'all' } });
  liveCache.raw = data.response || []; liveCache.ts = Date.now();
  return liveCache.raw;
}

async function getFixtureStatistics(fixtureId) {
  const { data } = await API.get('/fixtures/statistics', { params: { fixture: fixtureId } });
  if (!data.response?.length) return null;
  const stats = {};
  data.response.forEach(t => { stats[t.team.name] = {}; t.statistics.forEach(s => { stats[t.team.name][s.type] = s.value; }); });
  return stats;
}

async function getTeamStats(teamId, leagueId) {
  const season = LEAGUE_SEASONS[leagueId] || 2025;
  const { data } = await API.get('/teams/statistics', { params: { team: teamId, league: leagueId, season } });
  const r = data.response;
  if (!r) return null;
  return { equipo: r.team?.name, golesAnotadosHome: r.goals?.for?.average?.home, golesAnotadosAway: r.goals?.for?.average?.away, forma: r.form?.slice(-5) };
}

async function getH2H(id1, id2) {
  const { data } = await API.get('/fixtures/headtohead', { params: { h2h: `${id1}-${id2}`, last: 10 } });
  return (data.response || []).map(f => ({ date: f.fixture.date.split('T')[0], home: f.teams.home.name, away: f.teams.away.name, gh: f.goals.home, ga: f.goals.away, btts: f.goals.home > 0 && f.goals.away > 0 }));
}

const TIPSTER_SYSTEM = require('fs').readFileSync('./bot.js','utf8').match(/const TIPSTER_SYSTEM = `([\s\S]*?)`;/)[1];

async function sonnet(system, user) {
  const m = await anthropic.messages.create({ model:'claude-sonnet-4-6', max_tokens:2000, system, messages:[{ role:'user', content:user }] });
  return m.content[0].text;
}

async function main() {

  // ═══ CASO 1: Bundesliga en vivo ═══
  console.log('\n' + '═'.repeat(60));
  console.log('CASO 1: "partidos de bundesliga en vivo ahora"');
  console.log('═'.repeat(60));

  const live = await fetchLiveRaw();
  const bundesLive = live.filter(f => f.league.id === 78);
  console.log(`📊 Partidos Bundesliga en vivo: ${bundesLive.length}`);

  if (bundesLive.length === 0) {
    console.log('BOT RESPONDERÍA: "No hay partidos de Bundesliga en vivo ahora mismo."');
  } else {
    const toAnalyze = bundesLive.slice(0, 3);
    const statsAll = await Promise.allSettled(toAnalyze.map(f => getFixtureStatistics(f.fixture.id)));
    const enriched = toAnalyze.map((f, i) => ({
      local: f.teams.home.name, visitante: f.teams.away.name,
      marcador: `${f.goals.home}-${f.goals.away}`, minuto: f.fixture.status.elapsed,
      estadisticas: statsAll[i].status === 'fulfilled' ? statsAll[i].value : null,
    }));
    console.log('Datos enviados a Sonnet:', JSON.stringify(enriched, null, 2).slice(0, 500));
    const resp = await sonnet(TIPSTER_SYSTEM, `Datos reales Bundesliga en vivo:\n${JSON.stringify(enriched, null, 2)}\nDa picks in-play.`);
    console.log('\nRESPUESTA BOT:\n', resp.slice(0, 800));
  }

  // ═══ CASO 2: Real Madrid próximo partido ═══
  console.log('\n' + '═'.repeat(60));
  console.log('CASO 2: "analiza el proximo partido del real madrid"');
  console.log('═'.repeat(60));

  const teamRes = await API.get('/teams', { params: { search: 'Real Madrid' } });
  const rmId = teamRes.data.response[0].team.id;
  console.log('Real Madrid ID:', rmId);

  const liveRm = live.find(f => f.teams.home.id === rmId || f.teams.away.id === rmId);
  let nextFixture = liveRm;

  if (!nextFixture) {
    const today = new Date().toISOString().split('T')[0];
    const dayRes = await API.get('/fixtures', { params: { date: today, timezone: 'America/Bogota' } });
    nextFixture = dayRes.data.response.find(f =>
      (f.teams.home.id === rmId || f.teams.away.id === rmId) && ['NS','1H','HT','2H'].includes(f.fixture.status.short)
    );
  }

  if (nextFixture) {
    const hId = nextFixture.teams.home.id, aId = nextFixture.teams.away.id, lid = nextFixture.league.id;
    const [h2h, sh, sa] = await Promise.all([getH2H(hId, aId), getTeamStats(hId, lid), getTeamStats(aId, lid)]);
    const data = { partido: { liga: nextFixture.league.name, local: nextFixture.teams.home.name, visitante: nextFixture.teams.away.name }, h2h, bttsEnH2H: h2h.filter(m=>m.btts).length, statsLocal: sh, statsVisitante: sa };
    console.log('Datos API:', JSON.stringify(data).slice(0, 400));
    const resp = await sonnet(TIPSTER_SYSTEM, `Datos reales API-Football:\n${JSON.stringify(data, null, 2)}`);
    console.log('\nRESPUESTA BOT:\n', resp.slice(0, 800));
  } else {
    console.log('BOT: No encontré próximos partidos del Real Madrid en los próximos 14 días.');
  }

  // ═══ CASO 3: Picks del día ═══
  console.log('\n' + '═'.repeat(60));
  console.log('CASO 3: "picks del dia"');
  console.log('═'.repeat(60));

  const TRACKED = new Set([39,140,135,78,61,2,3,848,11,9,71,65,128,262,253,88,94,207,203,169,235,144,197,218,333,98,179,4,5,480,240,40,141,136,79,62,72,66,129,263,89]);
  const PRIO = {2:100,3:95,848:90,39:88,140:87,135:86,78:85,61:84};
  const today = new Date().toLocaleDateString('en-CA',{timeZone:'America/Bogota'});
  const todayRes = await API.get('/fixtures', { params: { date: today, timezone: 'America/Bogota' } });
  const fixtures = todayRes.data.response.filter(f => TRACKED.has(f.league.id));
  const selected = [...fixtures].sort((a,b)=>(PRIO[b.league.id]||0)-(PRIO[a.league.id]||0)).slice(0,8);
  console.log(`📊 Total fixtures hoy en ligas monitoreadas: ${fixtures.length}`);
  console.log('Top 8 seleccionados:', selected.map(f=>`${f.teams.home.name} vs ${f.teams.away.name} (${f.league.name})`).join('\n  '));
  const resp = await sonnet(TIPSTER_SYSTEM + '\nEmite 3 picks individuales + 1 combinada.',
    `Picks del día ${today}:\n${JSON.stringify(selected.map(f=>({ liga:f.league.name, local:f.teams.home.name, visitante:f.teams.away.name, hora:new Date(f.fixture.date).toLocaleTimeString('es-CO',{hour:'2-digit',minute:'2-digit',timeZone:'America/Bogota'}) })),null,2)}`);
  console.log('\nRESPUESTA BOT:\n', resp.slice(0, 1000));
}

main().catch(console.error);
