require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');
const fs = require('fs');
const path = require('path');
const express = require('express');
const crypto = require('crypto');
const Airtable = require('airtable');

// ─── Clients ─────────────────────────────────────────────────────────────────

const bot = new TelegramBot(process.env.TELEGRAM_TOKEN, { polling: true });
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ─── API-Football ─────────────────────────────────────────────────────────────

const API = axios.create({
  baseURL: 'https://v3.football.api-sports.io',
  headers: { 'x-apisports-key': process.env.APIFOOTBALL_KEY },
  timeout: 15000,
});

// Logging interceptor — every API call is logged
API.interceptors.request.use(req => {
  const params = new URLSearchParams(req.params || {}).toString();
  console.log(`🔍 API: ${req.baseURL}${req.url}${params ? '?' + params : ''}`);
  return req;
});
API.interceptors.response.use(res => {
  console.log(`📊 Respuesta: results=${res.data.results ?? '?'} errors=${JSON.stringify(res.data.errors || [])}`);
  return res;
});

// ─── League config ────────────────────────────────────────────────────────────

const LEAGUE_SEASONS = {
  39:2025, 140:2025, 135:2025, 78:2025, 61:2025,
  2:2025,  3:2025,   848:2025, 88:2025, 94:2025,
  207:2025,179:2025, 197:2025, 169:2025,144:2025,
  235:2025,40:2025,  141:2025, 136:2025,79:2025,
  62:2025, 89:2025,
  11:2026, 9:2026,   71:2026,  65:2026, 128:2026,
  262:2026,253:2026, 72:2026,  66:2026, 129:2026,
  263:2026,240:2026,
  203:2025,98:2025,  333:2025, 218:2025,
  4:2025,  5:2025,   480:2025,
  210:2025,103:2025, // Chipre Primera División, Noruega Eliteserien
  239:2026, // Liga BetPlay Colombia (Primera A) — ID real
};

const LEAGUE_IDS = new Set([
  39,140,135,78,61,2,3,848,11,9,
  71,239,128,262,253,88,94,207,203,169,
  235,144,197,218,333,98,179,4,5,480,
  240,40,141,136,79,62,72,66,129,263,89,
  210,103,
]);

const LEAGUE_MAP = {
  39: { name:'Premier League',     country:'England'     },
  140:{ name:'LaLiga',             country:'Spain'       },
  135:{ name:'Serie A',            country:'Italy'       },
  78: { name:'Bundesliga',         country:'Germany'     },
  61: { name:'Ligue 1',            country:'France'      },
  2:  { name:'Champions League',   country:'Europe'      },
  3:  { name:'Europa League',      country:'Europe'      },
  848:{ name:'Conference League',  country:'Europe'      },
  11: { name:'Libertadores',       country:'South Am.'   },
  9:  { name:'Sudamericana',       country:'South Am.'   },
  71: { name:'Brasileirao',        country:'Brazil'      },
  239:{ name:'Liga BetPlay',        country:'Colombia'    },
  128:{ name:'Liga Argentina',     country:'Argentina'   },
  262:{ name:'Liga MX',            country:'Mexico'      },
  253:{ name:'MLS',                country:'USA'         },
  88: { name:'Eredivisie',         country:'Netherlands' },
  94: { name:'Primeira Liga',      country:'Portugal'    },
  207:{ name:'Super Lig',          country:'Turkey'      },
  203:{ name:'Saudi Pro League',   country:'Saudi Arabia'},
  169:{ name:'Jupiler Pro',        country:'Belgium'     },
  235:{ name:'Premier Liga Rusia', country:'Russia'      },
  144:{ name:'Jupiler Pro League', country:'Belgium'     },
  197:{ name:'Super League Grecia',country:'Greece'      },
  218:{ name:'Liga Egipto',        country:'Egypt'       },
  333:{ name:'K League',           country:'South Korea' },
  98: { name:'J League',           country:'Japan'       },
  179:{ name:'Scottish Premier',   country:'Scotland'    },
  4:  { name:'Euro Championship',  country:'Europe'      },
  5:  { name:'Nations League',     country:'Europe'      },
  480:{ name:'Copa America',       country:'South Am.'   },
  240:{ name:'Torneo Águila',      country:'Colombia'    },
  40: { name:'Championship',       country:'England'     },
  141:{ name:'LaLiga2',            country:'Spain'       },
  136:{ name:'Serie B',            country:'Italy'       },
  79: { name:'2.Bundesliga',       country:'Germany'     },
  62: { name:'Ligue 2',            country:'France'      },
  72: { name:'Brasileirao B',      country:'Brazil'      },
  66: { name:'Liga Colombia B',    country:'Colombia'    },
  129:{ name:'Primera B Argentina',country:'Argentina'   },
  263:{ name:'Ascenso MX',         country:'Mexico'      },
  89: { name:'Eerste Divisie',     country:'Netherlands' },
  210:{ name:'Primera División',   country:'Cyprus'      },
  103:{ name:'Eliteserien',        country:'Norway'      },
};

// Maps user-written league names → league ID
const LEAGUE_NAME_TO_ID = {
  'bundesliga':78, '2.bundesliga':79, 'segunda bundesliga':79,
  'premier league':39, 'premier':39, 'epl':39,
  'laliga':140, 'la liga':140, 'primera division':140,
  'laliga2':141, 'segunda division':141,
  'serie a':135, 'serie b':136,
  'ligue 1':61, 'ligue1':61, 'ligue 2':62, 'ligue2':62,
  'champions league':2, 'champions':2, 'ucl':2, 'champions league':2,
  'europa league':3, 'europa':3, 'uel':3,
  'conference league':848, 'conference':848,
  'libertadores':11, 'copa libertadores':11,
  'sudamericana':9, 'copa sudamericana':9,
  'brasileirao':71, 'serie a brasileira':71, 'seriea brasileira':71,
  'brasil':71, 'brazil':71, 'liga brasil':71, 'liga brazil':71,
  'serie a brasil':71, 'seriea brasil':71, 'série a brasil':71,
  'brasileirao b':72, 'serie b brasil':72, 'serieb brasil':72,
  'liga betplay':239, 'primera a':239, 'liga colombia':239, 'betplay':239,
  'liga colombia b':66, 'primera b':240, 'torneo aguila':240, 'torneo águila':240,
  'liga argentina':128, 'primera division argentina':128, 'primera b argentina':129,
  'liga mx':262, 'ligamx':262, 'ascenso mx':263,
  'mls':253,
  'eredivisie':88, 'eerste divisie':89,
  'primeira liga':94, 'liga nos':94,
  'super lig':207, 'superlig':207, 'turquia':207, 'turkey':207, 'liga turca':207, 'tff':207, 'turkiye':207,
  'saudi pro league':203, 'saudi league':203,
  'jupiler pro league':144, 'jupiler':144,
  'super league grecia':197, 'super league':197,
  'liga egipto':218,
  'k league':333, 'k-league':333,
  'j league':98, 'j1 league':98,
  'scottish premier':179, 'scottish premiership':179,
  'championship':40,
  'chipre':210, 'primera division chipre':210, 'primera división chipre':210, 'cyprus':210,
  'noruega':103, 'eliteserien':103, 'norway':103,
};

function findLeagueId(name) {
  if (!name) return null;
  const q = name.toLowerCase().trim().replace(/\s+/g, ' ');
  // exact match
  if (LEAGUE_NAME_TO_ID[q]) return LEAGUE_NAME_TO_ID[q];
  // partial match — prioriza la clave más larga para evitar falsos positivos
  // (ej: "serie a brasil" no debe matchear "serie a" → Italia)
  let bestKey = null, bestId = null;
  for (const [key, id] of Object.entries(LEAGUE_NAME_TO_ID)) {
    if (q.includes(key) || key.includes(q)) {
      if (!bestKey || key.length > bestKey.length) {
        bestKey = key;
        bestId = id;
      }
    }
  }
  return bestId;
}

// ─── League priority for sorting ─────────────────────────────────────────────

const LEAGUE_PRIORITY = {
  2:100,3:95,848:90,
  39:88,140:87,135:86,78:85,61:84,
  11:80,9:78,
  88:70,94:69,207:68,144:67,169:66,
  71:65,262:64,128:63,239:62,253:61,
  40:55,141:54,136:53,79:52,62:51,
  240:45,72:44,66:43,129:42,263:41,89:40,
  210:38,103:37,
};

// ─── Plans config ─────────────────────────────────────────────────────────────

const PLANES = {
  free: {
    nombre: 'Freemium',
    consultas_diarias: 1,
    dias_prueba: 3,
    puede_imagen: false,
  },
  vip: {
    nombre: 'VIP',
    consultas_diarias: 10,
    dias_prueba: 0,
    puede_imagen: false,
  },
  pro: {
    nombre: 'PRO',
    consultas_diarias: 50,
    dias_prueba: 0,
    puede_imagen: true,
  },
};

// ─── Cache ────────────────────────────────────────────────────────────────────

const dateCache = new Map();
let liveCache = { raw: null, ts: 0 };

// Caché de picks del día: evita análisis duplicados y picks contradictorios
// Clave: `${fecha}_${scope}` donde scope = 'all' | leagueId
// Expira automáticamente a medianoche hora Colombia
const PICKS_CACHE_FILE = path.join(__dirname, 'picks_hoy_cache.json');

function loadPicksCache() {
  try { return JSON.parse(fs.readFileSync(PICKS_CACHE_FILE, 'utf8')); }
  catch { return {}; }
}

function savePicksCache(cache) {
  try { fs.writeFileSync(PICKS_CACHE_FILE, JSON.stringify(cache, null, 2)); }
  catch (e) { console.error('savePicksCache error:', e.message); }
}

function getPicksCache(scope) {
  const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
  const cache = loadPicksCache();
  const entry = cache[`${today}_${scope}`];
  if (!entry) return null;
  // Verificar que es del mismo día Colombia (seguridad extra)
  if (entry.fecha !== today) return null;
  return entry;
}

function setPicksCache(scope, picksText, fixtureIds) {
  const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
  const cache = loadPicksCache();
  // Limpiar entradas de días anteriores
  for (const key of Object.keys(cache)) {
    if (!key.startsWith(today)) delete cache[key];
  }
  cache[`${today}_${scope}`] = {
    fecha:      today,
    generadoAt: new Date().toISOString(),
    picksText,
    fixtureIds: fixtureIds || [],
  };
  savePicksCache(cache);
}

// ─── API helpers ──────────────────────────────────────────────────────────────

function parseFixture(f) {
  return {
    fixtureId:  f.fixture.id,
    date:       f.fixture.date,
    status:     f.fixture.status.short,
    elapsed:    f.fixture.status.elapsed,
    leagueId:   f.league.id,
    leagueName: LEAGUE_MAP[f.league.id]?.name || f.league.name,
    country:    LEAGUE_MAP[f.league.id]?.country || f.league.country,
    homeId:     f.teams.home.id,
    awayId:     f.teams.away.id,
    homeTeam:   f.teams.home.name,
    awayTeam:   f.teams.away.name,
    homeGoals:  f.goals.home,
    awayGoals:  f.goals.away,
  };
}

async function fetchFixturesByDate(date) {
  if (dateCache.has(date)) return dateCache.get(date);
  // Sin timezone: la API devuelve todos los partidos del día UTC
  // Esto evita que partidos nocturnos (ej: 8 PM Bogota = 1 AM UTC día siguiente) se pierdan
  const { data } = await API.get('/fixtures', { params: { date } });
  const result = data.response || [];
  dateCache.set(date, result);
  return result;
}

async function getFixturesByDate(date) {
  const all = await fetchFixturesByDate(date);
  return all.filter(f => LEAGUE_IDS.has(f.league.id)).map(parseFixture);
}

async function fetchLiveRaw() {
  if (Date.now() - liveCache.ts < 30000 && liveCache.raw) return liveCache.raw;
  const { data } = await API.get('/fixtures', { params: { live: 'all' } });
  liveCache = { raw: data.response || [], ts: Date.now() };
  return liveCache.raw;
}

async function getLiveFixtures(leagueId = null) {
  const raw = await fetchLiveRaw();
  const filtered = leagueId
    ? raw.filter(f => f.league.id === leagueId)
    : raw.filter(f => LEAGUE_IDS.has(f.league.id));
  return filtered.map(parseFixture);
}

async function getFixtureStatistics(fixtureId) {
  const { data } = await API.get('/fixtures/statistics', { params: { fixture: fixtureId } });
  if (!data.response || data.response.length === 0) return null;
  const stats = {};
  data.response.forEach(teamStats => {
    const key = teamStats.team.name;
    stats[key] = {};
    teamStats.statistics.forEach(s => { stats[key][s.type] = s.value; });
  });
  return stats;
}

function normalizeTeamName(str) {
  return str
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // quita tildes y diacríticos (ç→c, ü→u, ö→o, etc.)
    .replace(/[/\-_.&']/g, ' ')                       // normaliza separadores (Bodo/Glimt → bodo glimt)
    .replace(/\s+/g, ' ')
    .trim();
}

// Aliases de nombres cortos/populares → nombre exacto en la API
const TEAM_ALIASES = {
  'roma':            'AS Roma',
  'inter':           'Inter Milan',
  'inter milan':     'Inter Milan',
  'atletico':        'Atletico Madrid',
  'atletico madrid': 'Atletico Madrid',
  'atleti':          'Atletico Madrid',
  'milan':           'AC Milan',
  'ac milan':        'AC Milan',
  'psg':             'Paris Saint Germain',
  'paris':           'Paris Saint Germain',
  'paris sg':        'Paris Saint Germain',
  'paris saint-germain': 'Paris Saint Germain',
  'man city':        'Manchester City',
  'manchester city': 'Manchester City',
  'man united':      'Manchester United',
  'man utd':         'Manchester United',
  'manchester utd':  'Manchester United',
  'united':          'Manchester United',
  'tottenham':       'Tottenham Hotspur',
  'spurs':           'Tottenham Hotspur',
  'newcastle':       'Newcastle United',
  'wolves':          'Wolverhampton',
  'wolverhampton':   'Wolverhampton',
  'bayer':           'Bayer Leverkusen',
  'leverkusen':      'Bayer Leverkusen',
  'dortmund':        'Borussia Dortmund',
  'bvb':             'Borussia Dortmund',
  'gladbach':        'Borussia Monchengladbach',
  'monchengladbach': 'Borussia Monchengladbach',
  'frankfurt':       'Eintracht Frankfurt',
  'eintracht':       'Eintracht Frankfurt',
  'ajax':            'Ajax',
  'porto':           'FC Porto',
  'benfica':         'SL Benfica',
  'sporting':        'Sporting CP',
  'sporting cp':     'Sporting CP',
  'braga':           'SC Braga',
  'sevilla':         'Sevilla FC',
  'valencia':        'Valencia CF',
  'villarreal':      'Villarreal CF',
  'betis':           'Real Betis',
  'real betis':      'Real Betis',
  'sociedad':        'Real Sociedad',
  'real sociedad':   'Real Sociedad',
  'celta':           'Celta Vigo',
  'osasuna':         'Osasuna',
  'rayo':            'Rayo Vallecano',
  'getafe':          'Getafe CF',
  'girona':          'Girona FC',
  'alaves':          'Deportivo Alaves',
  'fiorentina':      'Fiorentina',
  'napoli':          'Napoli',
  'juventus':        'Juventus',
  'atalanta':        'Atalanta',
  'lazio':           'Lazio',
  'torino':          'Torino',
  'udinese':         'Udinese',
  'bologna':         'Bologna',
  'genoa':           'Genoa',
  'lyon':            'Olympique Lyonnais',
  'marseille':       'Olympique Marseille',
  'monaco':          'AS Monaco',
  'lille':           'Lille OSC',
  'nice':            'OGC Nice',
  'rennes':          'Stade Rennais',
  'lens':            'RC Lens',
  'brest':           'Stade Brestois',
  'river':           'River Plate',
  'river plate':     'River Plate',
  'boca':            'Boca Juniors',
  'boca juniors':    'Boca Juniors',
  'flamengo':        'Flamengo',
  'fluminense':      'Fluminense',
  'palmeiras':       'Palmeiras',
  'santos':          'Santos FC',
  'america':         'Club America',
  'chivas':          'Guadalajara',
  'cruz azul':       'Cruz Azul',
  'pumas':           'Pumas UNAM',
  'tigres':          'Tigres UANL',
  'Nacional':        'Club Nacional',
};

async function searchTeam(name, countryHint = '') {
  // Resolver alias antes de buscar
  const aliasKey = name.toLowerCase().trim().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const resolvedName = TEAM_ALIASES[aliasKey] || name;

  const { data } = await API.get('/teams', { params: { search: resolvedName } });
  const results = data.response || [];
  if (results.length === 0) return null;
  if (results.length === 1) return results[0];

  const q = normalizeTeamName(name);
  const country = countryHint.trim().toLowerCase();
  const RESERVE = /\b(ii|b|reserve|reserva|sub|youth|juvenil|u\d{2}|amateur|filial)\b/i;

  function score(t) {
    const tname = normalizeTeamName(t.team.name);
    const tcountry = (t.team.country || '').toLowerCase();
    let s = 0;
    if (tname === q) s += 100;
    else if (tname.endsWith(' ' + q) || tname.endsWith(q)) s += 80;
    else if (tname.startsWith(q + ' ') || tname.startsWith(q)) s += 50;
    else if (tname.includes(q)) s += 20;
    if (country && tcountry.includes(country)) s += 40;
    if (RESERVE.test(t.team.name)) s -= 40;
    return s;
  }
  return results.sort((a, b) => score(b) - score(a))[0];
}

async function findNextFixtureByDate(teamId, daysAhead = 14) {
  const LIVE_STATUSES = ['1H','HT','2H','ET','P','BT','LIVE'];
  const UPCOMING_STATUSES = ['NS', ...LIVE_STATUSES];

  // 1. Partido en vivo ahora mismo para este equipo (consulta fresca, sin cache)
  try {
    const { data } = await API.get('/fixtures', { params: { team: teamId, live: 'all' } });
    const live = (data.response || []).find(f =>
      f.teams.home.id === teamId || f.teams.away.id === teamId
    );
    if (live) return live;
  } catch {}

  // 2. Próximos partidos del equipo (1 sola llamada directa, más fiable que buscar por fecha)
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() + daysAhead);
  try {
    const { data } = await API.get('/fixtures', { params: { team: teamId, next: 10 } });
    const next = (data.response || []).find(f =>
      UPCOMING_STATUSES.includes(f.fixture.status.short) &&
      new Date(f.fixture.date) <= cutoff
    );
    if (next) return next;
  } catch {}

  // 3. Fallback: loop por fecha (por si next no devuelve el partido de hoy en edge cases)
  const today = new Date();
  for (let i = 0; i <= Math.min(daysAhead, 3); i++) {
    const d = new Date(today);
    d.setDate(d.getDate() + i);
    const ds = d.toISOString().split('T')[0];
    if (i === 0) dateCache.delete(ds);
    const all = await fetchFixturesByDate(ds);
    const match = all.find(f =>
      (f.teams.home.id === teamId || f.teams.away.id === teamId) &&
      UPCOMING_STATUSES.includes(f.fixture.status.short)
    );
    if (match) return match;
  }
  return null;
}

async function getTeamLastFixtures(teamId, last = 15, venue = null) {
  const params = { team: teamId, last };
  if (venue === 'home') params.venue = 'home';
  else if (venue === 'away') params.venue = 'away';
  const { data } = await API.get('/fixtures', { params });
  return (data.response || [])
    .filter(f => f.fixture.status.short === 'FT')
    .map(f => ({
      fixtureId:  f.fixture.id,
      date:       f.fixture.date.split('T')[0],
      homeTeam:   f.teams.home.name,
      awayTeam:   f.teams.away.name,
      homeId:     f.teams.home.id,
      awayId:     f.teams.away.id,
      leagueName: f.league.name,
      leagueId:   f.league.id,
      goalsHome:  f.goals.home ?? 0,
      goalsAway:  f.goals.away ?? 0,
      htHome:     f.score?.halftime?.home ?? null,
      htAway:     f.score?.halftime?.away ?? null,
    }));
}

async function getLeagueStandings(leagueId) {
  const season = LEAGUE_SEASONS[leagueId] || 2025;
  const { data } = await API.get('/standings', { params: { league: leagueId, season } });
  const standings = data.response?.[0]?.league?.standings;
  if (!standings) return [];
  const group = Array.isArray(standings[0]) ? standings[0] : standings;
  return group.map(s => ({ teamId: s.team.id, teamName: s.team.name, rank: s.rank }));
}

async function getH2H(id1, id2) {
  const { data } = await API.get('/fixtures/headtohead', { params: { h2h: `${id1}-${id2}`, last: 10 } });
  return (data.response || []).map(f => ({
    date:      f.fixture.date.split('T')[0],
    home:      f.teams.home.name,
    away:      f.teams.away.name,
    golesHome: f.goals.home,
    golesAway: f.goals.away,
    btts:      f.goals.home > 0 && f.goals.away > 0,
  }));
}

async function getTeamStats(teamId, leagueId) {
  const season = LEAGUE_SEASONS[leagueId] || 2025;
  const { data } = await API.get('/teams/statistics', { params: { team: teamId, league: leagueId, season } });
  const r = data.response;
  if (!r) return null;
  return {
    equipo:             r.team?.name,
    liga:               r.league?.name,
    temporada:          season,
    forma:              r.form?.replace(/W/g,'G').replace(/L/g,'P').replace(/D/g,'E').slice(-6).split('').join('-'),
    golesAnotadosHome:  r.goals?.for?.average?.home,
    golesAnotadosAway:  r.goals?.for?.average?.away,
    golesRecibidosHome: r.goals?.against?.average?.home,
    golesRecibidosAway: r.goals?.against?.average?.away,
    cleanSheetsHome:    r.clean_sheet?.home,
    cleanSheetsAway:    r.clean_sheet?.away,
    failedToScoreHome:  r.failed_to_score?.home,
    failedToScoreAway:  r.failed_to_score?.away,
    victorias:          r.fixtures?.wins,
    empates:            r.fixtures?.draws,
    derrotas:           r.fixtures?.loses,
  };
}

// ─── Probability & Analytics Engine ──────────────────────────────────────────

// Factorial helper para Poisson (limitado a n<=20 para evitar overflow)
function factorial(n) {
  if (n <= 1) return 1;
  let r = 1;
  for (let i = 2; i <= Math.min(n, 20); i++) r *= i;
  return r;
}

// Distribución de Poisson: P(X = k) dado lambda
function poissonPMF(lambda, k) {
  if (lambda <= 0) return k === 0 ? 1 : 0;
  return (Math.pow(lambda, k) * Math.exp(-lambda)) / factorial(k);
}

// P(X >= threshold) = 1 - P(X < threshold)
function poissonCDF_above(lambda, threshold) {
  let cumulative = 0;
  for (let k = 0; k < threshold; k++) cumulative += poissonPMF(lambda, k);
  return 1 - cumulative;
}

/**
 * Calcula probabilidades usando modelo de Poisson Dixon-Coles simplificado.
 * Requiere promedios de goles por partido de ambos equipos.
 *
 * @param {number} homeFor    - Promedio goles anotados por local en casa
 * @param {number} homeAgainst - Promedio goles recibidos por local en casa
 * @param {number} awayFor    - Promedio goles anotados por visitante fuera
 * @param {number} awayAgainst - Promedio goles recibidos por visitante fuera
 * @returns {object} probabilidades de mercados clave
 */
function calcPoissonProbs(homeFor, homeAgainst, awayFor, awayAgainst) {
  // Goles esperados (xG implícito)
  const homeLambda = ((homeFor || 1.2) + (awayAgainst || 1.2)) / 2;
  const awayLambda = ((awayFor || 1.0) + (homeAgainst || 1.0)) / 2;

  // Construir matriz de score hasta 6-6
  const MAX = 7;
  let pHomeWin = 0, pDraw = 0, pAwayWin = 0;
  let pBtts = 0, pOver15 = 0, pOver25 = 0, pOver35 = 0, pUnder25 = 0;

  for (let h = 0; h < MAX; h++) {
    for (let a = 0; a < MAX; a++) {
      const p = poissonPMF(homeLambda, h) * poissonPMF(awayLambda, a);
      const total = h + a;
      if (h > a) pHomeWin += p;
      else if (h === a) pDraw += p;
      else pAwayWin += p;
      if (h > 0 && a > 0) pBtts += p;
      if (total >= 2) pOver15 += p;
      if (total >= 3) pOver25 += p;
      if (total >= 4) pOver35 += p;
      if (total <= 2) pUnder25 += p; // Bajo 2.5 = 0, 1, o 2 goles
    }
  }

  // DNB: excluye el empate y redistribuye
  const pDnbHome = pHomeWin / (pHomeWin + pAwayWin);
  const pDnbAway = pAwayWin / (pHomeWin + pAwayWin);

  return {
    homeLambda: +homeLambda.toFixed(2),
    awayLambda: +awayLambda.toFixed(2),
    homeWin:  +(pHomeWin * 100).toFixed(1),
    draw:     +(pDraw    * 100).toFixed(1),
    awayWin:  +(pAwayWin * 100).toFixed(1),
    btts:     +(pBtts    * 100).toFixed(1),
    over15:   +(pOver15  * 100).toFixed(1),
    over25:   +(pOver25  * 100).toFixed(1),
    over35:   +(pOver35  * 100).toFixed(1),
    under25:  +(pUnder25 * 100).toFixed(1),
    dnbHome:  +(pDnbHome * 100).toFixed(1),
    dnbAway:  +(pDnbAway * 100).toFixed(1),
  };
}

/**
 * Calcula el Expected Value de una apuesta.
 * EV > 0 = apuesta con valor real; EV > 0.05 = buen valor (>5%)
 *
 * @param {number} estimatedProb - Probabilidad estimada (0-1)
 * @param {number} decimalOdds   - Cuota decimal (ej: 1.85)
 * @returns {number} EV como porcentaje (ej: 14.2 = +14.2%)
 */
function calcEV(estimatedProb, decimalOdds) {
  if (!estimatedProb || !decimalOdds || decimalOdds <= 1) return null;
  const ev = (estimatedProb * (decimalOdds - 1)) - (1 - estimatedProb);
  return +(ev * 100).toFixed(1);
}

/**
 * Calcula el momentum en vivo basado en las estadísticas del partido.
 * Score positivo = domina el local; negativo = domina el visitante.
 *
 * @param {object} stats - Estadísticas del partido de getFixtureStatistics()
 * @param {string} homeName - Nombre del equipo local
 * @param {string} awayName - Nombre del equipo visitante
 * @returns {object} momentum con dominador, score e intensidad
 */
function calcLiveMomentum(stats, homeName, awayName) {
  if (!stats) return null;
  const teams = Object.keys(stats);
  if (teams.length < 2) return null;

  const homeKey = teams.find(k => normalizeTeamName(k).includes(normalizeTeamName(homeName).split(' ')[0])) || teams[0];
  const awayKey = teams.find(k => k !== homeKey) || teams[1];

  const h = stats[homeKey] || {};
  const a = stats[awayKey] || {};

  const parseVal = v => parseInt(v) || 0;
  const parsePct = v => parseFloat((v || '0%').replace('%', '')) || 0;

  const shotsDiff       = parseVal(h['Shots on Goal']) - parseVal(a['Shots on Goal']);
  const possessionDiff  = parsePct(h['Ball Possession']) - 50; // Positivo si domina local
  const cornersDiff     = parseVal(h['Corner Kicks']) - parseVal(a['Corner Kicks']);
  const attacksDiff     = parseVal(h['Total Shots']) - parseVal(a['Total Shots']);
  const dangerousAttacks= parseVal(h['Shots insidebox']) - parseVal(a['Shots insidebox']);

  // Score ponderado: más peso a tiros a puerta y ataques peligrosos
  const score = (shotsDiff * 12) + (possessionDiff * 0.4) + (cornersDiff * 4) + (attacksDiff * 3) + (dangerousAttacks * 8);
  const intensity = Math.abs(score);

  let dominates, label;
  if (score > 15)       { dominates = 'home'; label = `Domina ${homeName}`; }
  else if (score < -15) { dominates = 'away'; label = `Domina ${awayName}`; }
  else                  { dominates = 'equal'; label = 'Partido equilibrado'; }

  return {
    dominates,
    label,
    score: +score.toFixed(1),
    intensity: +intensity.toFixed(1),
    homeStats: {
      shotsOnTarget: parseVal(h['Shots on Goal']),
      possession:    parsePct(h['Ball Possession']),
      corners:       parseVal(h['Corner Kicks']),
      totalShots:    parseVal(h['Total Shots']),
    },
    awayStats: {
      shotsOnTarget: parseVal(a['Shots on Goal']),
      possession:    parsePct(a['Ball Possession']),
      corners:       parseVal(a['Corner Kicks']),
      totalShots:    parseVal(a['Total Shots']),
    },
  };
}

/**
 * Proyecta estadísticas en vivo al ritmo actual.
 * Útil para corners y tarjetas.
 *
 * @param {number} current  - Valor actual (ej: 6 corners al min 55)
 * @param {number} elapsed  - Minutos transcurridos
 * @param {number} total    - Minutos totales del partido (90 o 120)
 * @returns {object} proyección con valor estimado y confianza
 */
function calcLiveProjection(current, elapsed, total = 90) {
  if (!elapsed || elapsed <= 0) return null;
  const pace = current / elapsed;
  const projected = pace * total;
  const remaining = (total - elapsed) * pace;
  const confidence = elapsed >= 30 ? 'alta' : elapsed >= 15 ? 'media' : 'baja';
  return {
    projected:  +projected.toFixed(1),
    remaining:  +remaining.toFixed(1),
    pace:       +(pace * 90).toFixed(1), // corners/90 equivalentes
    confidence,
  };
}

/**
 * Enriquece los datos de un partido con probabilidades calculadas.
 * Se añade el bloque `probabilidades` al objeto de análisis.
 *
 * @param {object} homeStats - Stats del equipo local (de getTeamStats)
 * @param {object} awayStats - Stats del equipo visitante (de getTeamStats)
 * @param {Array}  h2h       - Array de H2H (de getH2H)
 * @param {boolean} isHome   - true si homeStats es el equipo local
 * @returns {object} bloque de probabilidades para incluir en el prompt
 */
function buildProbBlock(homeStats, awayStats, h2h = []) {
  if (!homeStats || !awayStats) return null;

  const hFor  = parseFloat(homeStats.golesAnotadosHome) || 0;
  const hAgt  = parseFloat(homeStats.golesRecibidosHome) || 0;
  const aFor  = parseFloat(awayStats.golesAnotadosAway) || 0;
  const aAgt  = parseFloat(awayStats.golesRecibidosAway) || 0;

  const probs = calcPoissonProbs(hFor, hAgt, aFor, aAgt);

  // BTTS empírico desde H2H
  const h2hBttsRate = h2h.length > 0
    ? +((h2h.filter(m => m.btts).length / h2h.length) * 100).toFixed(1)
    : null;

  // Probabilidad de BTTS combinada: Poisson + empírico H2H (si disponible)
  const bttsBlended = h2hBttsRate !== null
    ? +((probs.btts * 0.6 + h2hBttsRate * 0.4)).toFixed(1)
    : probs.btts;

  // EV de mercados comunes (con cuotas de referencia de mercado)
  const refOdds = { over25: 1.85, btts: 1.80, homeWin: 1.70, awayWin: 3.50, draw: 3.20, over35: 2.60, dnbHome: 1.50 };
  const ev = {};
  for (const [k, odds] of Object.entries(refOdds)) {
    const prob = k === 'btts' ? bttsBlended / 100 : (probs[k] || 0) / 100;
    ev[k] = calcEV(prob, odds);
  }

  return {
    modeloPoisson: {
      xGLocal: probs.homeLambda,
      xGVisitante: probs.awayLambda,
      probLocalGana: `${probs.homeWin}%`,
      probEmpate:    `${probs.draw}%`,
      probVisitanteGana: `${probs.awayWin}%`,
      probBTTS_Poisson:  `${probs.btts}%`,
      probBTTS_H2H:      h2hBttsRate !== null ? `${h2hBttsRate}%` : 'N/D',
      probBTTS_Combinada: `${bttsBlended}%`,
      probOver15: `${probs.over15}%`,
      probOver25: `${probs.over25}%`,
      probOver35: `${probs.over35}%`,
      probUnder25: `${probs.under25}%`,
      probDNB_Local: `${probs.dnbHome}%`,
      probDNB_Visitante: `${probs.dnbAway}%`,
    },
    expectedValue_vs_CuotasReferencia: {
      'Over 2.5 @ 1.85': ev.over25 !== null ? `${ev.over25 > 0 ? '+' : ''}${ev.over25}%` : 'N/D',
      'BTTS @ 1.80':     ev.btts   !== null ? `${ev.btts   > 0 ? '+' : ''}${ev.btts}%`   : 'N/D',
      'Local Gana @ 1.70': ev.homeWin !== null ? `${ev.homeWin > 0 ? '+' : ''}${ev.homeWin}%` : 'N/D',
      'Over 3.5 @ 2.60': ev.over35 !== null ? `${ev.over35 > 0 ? '+' : ''}${ev.over35}%` : 'N/D',
      'DNB Local @ 1.50': ev.dnbHome !== null ? `${ev.dnbHome > 0 ? '+' : ''}${ev.dnbHome}%` : 'N/D',
    },
    nota: 'EV positivo = apuesta con valor real vs cuota de mercado. Usa estos datos para calibrar el stake.',
  };
}

// ─── Goal Alert Engine ────────────────────────────────────────────────────────

/**
 * Determina los minutos restantes y el semiperíodo activo del partido.
 */
function matchTimeInfo(status, elapsed) {
  const e = elapsed || 1;
  if (status === '1H') return { period: '1T', remaining: Math.max(45 - e, 1), total: 45 };
  if (status === '2H') return { period: '2T', remaining: Math.max(90 - e, 1), total: 90 };
  if (status === 'ET') return { period: 'ET', remaining: Math.max(120 - e, 1), total: 120 };
  return null; // HT, FT, etc. — no aplica
}

/**
 * Selecciona el mercado más apropiado y calcula odds estimadas
 * basado en la situación actual del partido.
 */
function selectGoalMarket(homeGoals, awayGoals, pGoal, elapsed, period) {
  const total = homeGoals + awayGoals;
  const diff  = homeGoals - awayGoals;

  // Over de la siguiente línea entera
  const overLine = total + 0.5;

  // BTTS: útil si un equipo aún no marcó y p > 40%
  const bttsViable = (homeGoals === 0 || awayGoals === 0) && pGoal > 0.42;

  // Empate productivo → Over X.5 tiene valor si es 0-0 y tiempo avanzado
  const drawLate = total === 0 && elapsed >= 60;

  let market, impliedOdds;

  if (drawLate) {
    // 0-0 pasado el min 60 → cuota Over 0.5 sube naturalmente (buena ventana)
    market = `Over ${overLine} goles (partido terminará en goles)`;
    impliedOdds = +(1 / pGoal).toFixed(2);
  } else if (bttsViable && elapsed < 75) {
    // Un equipo sin marcar + tiempo suficiente → BTTS
    const noScorer = homeGoals === 0 ? 'local' : 'visitante';
    market = `Ambos marcan — ${noScorer} todavía sin gol`;
    // BTTS odds suelen ser mayores
    impliedOdds = +(1 / (pGoal * 0.85)).toFixed(2);
  } else if (Math.abs(diff) === 1 && elapsed >= 55) {
    // Partido igualado 1 gol de diferencia → equipo perdedor presiona
    const trailing = diff > 0 ? awayGoals : homeGoals;
    market = `Over ${overLine} goles (equipo perdedor presiona)`;
    impliedOdds = +(1 / pGoal).toFixed(2);
  } else {
    market = `Over ${overLine} goles`;
    impliedOdds = +(1 / pGoal).toFixed(2);
  }

  return { market, impliedOdds, overLine };
}

/**
 * Calcula la probabilidad de que haya al menos 1 gol más en el tiempo restante.
 * Combina xG histórico (team stats) con el ritmo real del partido (live pace).
 *
 * @returns {object|null} datos de alerta o null si el partido no está activo
 */
function calcGoalAlert(fixture, liveStats, homeStats, awayStats) {
  const timeInfo = matchTimeInfo(fixture.status, fixture.elapsed);
  if (!timeInfo) return null;

  const { period, remaining, total } = timeInfo;
  const elapsed     = fixture.elapsed || 1;
  const homeGoals   = fixture.homeGoals ?? 0;
  const awayGoals   = fixture.awayGoals ?? 0;
  const totalGoals  = homeGoals + awayGoals;
  const timeFrac    = remaining / 90; // fracción de 90 min restante

  // ── xG histórico ajustado a tiempo restante ──────────────────────────────
  const hFor  = parseFloat(homeStats?.golesAnotadosHome) || 1.2;
  const hAgt  = parseFloat(homeStats?.golesRecibidosHome) || 1.1;
  const aFor  = parseFloat(awayStats?.golesAnotadosAway) || 1.0;
  const aAgt  = parseFloat(awayStats?.golesRecibidosAway) || 1.0;

  const homeLambdaFull = ((hFor + aAgt) / 2);
  const awayLambdaFull = ((aFor + hAgt) / 2);
  const lambdaHistorical = (homeLambdaFull + awayLambdaFull) * timeFrac;

  // ── Ritmo real del partido (pace) ─────────────────────────────────────────
  const goalPaceFull = totalGoals > 0 ? (totalGoals / elapsed) * 90 : 0;
  const lambdaLivePace = goalPaceFull * timeFrac;

  // ── Lambda combinada: 60% histórico + 40% pace real ───────────────────────
  // Si no hay goles aún, confiamos más en el histórico
  const liveWeight = totalGoals > 0 ? 0.40 : 0.15;
  const lambdaCombined = lambdaHistorical * (1 - liveWeight) + lambdaLivePace * liveWeight;

  // ── Bonuses contextuales ──────────────────────────────────────────────────
  let bonus = 0;

  // Presión de tiros: muchos tiros con pocos goles → presión acumulada
  if (liveStats) {
    const teams = Object.values(liveStats);
    if (teams.length >= 2) {
      const shotsOnH = parseInt(teams[0]?.['Shots on Goal'] || 0);
      const shotsOnA = parseInt(teams[1]?.['Shots on Goal'] || 0);
      const shotsTotal = parseInt(teams[0]?.['Total Shots'] || 0) + parseInt(teams[1]?.['Total Shots'] || 0);
      const shotsOnTarget = shotsOnH + shotsOnA;
      // Si hay ≥8 tiros a puerta y pocos goles → +5% probabilidad
      if (shotsOnTarget >= 8 && shotsOnTarget / (totalGoals + 1) > 4) bonus += 0.05;
      // Si hay ≥14 tiros totales → más presión ofensiva
      if (shotsTotal >= 14) bonus += 0.03;
    }
  }

  // Equipo perdedor por 1 gol → empuja más (desesperación)
  if (Math.abs(homeGoals - awayGoals) === 1 && elapsed >= 55) bonus += 0.04;

  // Último cuarto 0-0 → urgencia máxima
  if (totalGoals === 0 && elapsed >= 67) bonus += 0.06;

  // P(al menos 1 gol más) = 1 - P(0 goles) = 1 - e^(-lambda)
  const pRaw     = 1 - Math.exp(-lambdaCombined);
  const pGoal    = Math.min(pRaw + bonus, 0.94);
  const impliedOdds = pGoal > 0 ? +(1 / pGoal).toFixed(2) : 99;

  // Solo alertamos si las odds estimadas superan 1.45 (P < 69%)
  if (impliedOdds < 1.45) return null;

  const { market, impliedOdds: mktOdds, overLine } = selectGoalMarket(
    homeGoals, awayGoals, pGoal, elapsed, period
  );

  // Score de oportunidad (0-100): combina prob + odds + tiempo restante
  // Ponderamos más los partidos con odds entre 1.50-2.20 (zona de valor real)
  const oddsBonus = (mktOdds >= 1.50 && mktOdds <= 2.50) ? 15 : (mktOdds > 2.50 ? 5 : -10);
  const alertScore = Math.min(pGoal * 70 + (remaining / 45) * 15 + oddsBonus, 100);

  // Describe por qué es una oportunidad
  const reasons = [];
  if (totalGoals === 0 && elapsed >= 60)  reasons.push(`0-0 en min ${elapsed}, tiempo aprieta`);
  if (Math.abs(homeGoals - awayGoals) === 1 && elapsed >= 55) reasons.push('equipo perdedor presiona');
  if (bonus > 0.07) reasons.push('alta presión ofensiva (tiros a puerta)');
  if (lambdaCombined > 0.8) reasons.push(`xG restante alto (${lambdaCombined.toFixed(2)} goles esperados)`);
  if (reasons.length === 0) reasons.push(`${(pGoal*100).toFixed(0)}% prob de gol en ${remaining} min restantes`);

  return {
    fixtureId:   fixture.fixtureId,
    local:       fixture.homeTeam,
    visitante:   fixture.awayTeam,
    liga:        fixture.leagueName,
    country:     fixture.country,
    marcador:    `${homeGoals}-${awayGoals}`,
    minuto:      elapsed,
    period,
    remaining,
    pGoal:       +(pGoal * 100).toFixed(1),
    impliedOdds: mktOdds,
    market,
    overLine,
    xGRestante:  +lambdaCombined.toFixed(2),
    alertScore:  +alertScore.toFixed(1),
    razon:       reasons.join(' + '),
  };
}

const ALERTA_GOL_SYSTEM = `Eres un tipster especializado en apuestas en vivo (in-play). Te llegan datos calculados matemáticamente de partidos en curso con probabilidades de gol reales.

FORMATO OBLIGATORIO para cada alerta:
⚡ *ALERTA DE GOL #[N]*
⚽ [Local] [marcador] [Visitante] | 🕐 Min [XX] ([período])
🏆 [Liga] — [País]
━━━━━━━━━━━━━━━━━━━
🎯 Mercado: *[mercado recomendado]*
📊 Prob. de gol: *[X]%*
💰 Cuota estimada: *~[X.XX]*
⏱️ Actúa antes del min: *[min_límite]*
📈 Por qué: [razón en 1 línea]
🏆 Stake: *[X]/10*
━━━━━━━━━━━━━━━━━━━

CRITERIO DE STAKE PARA ALERTA EN VIVO:
- Stake 8: prob > 72% + cuota > 1.55
- Stake 7: prob 62-72% + cuota > 1.50
- Stake 6: prob 55-62% + cuota > 1.48
- Stake 1-5: NUNCA publicar — si la prob no llega a 55%, omite la alerta completamente
- No publicar si cuota estimada < 1.45

CRITERIO DEL MINUTO LÍMITE:
- Siempre da un minuto concreto antes del que vale apostar
- En 1T: si es min 25, actúa antes del min 35
- En 2T: si es min 65, actúa antes del min 75
- Nunca más allá del min 85

Al final de todas las alertas, añade:
⚠️ _Las cuotas en vivo cambian rápidamente. Verifica la cuota real antes de apostar._

Responde en español. No menciones APIs ni fuentes de datos.`;

// ─── Anthropic helpers ────────────────────────────────────────────────────────

function isOverloadedError(err) {
  return err.status === 529 || err.status === 429 ||
    (err.message && (err.message.includes('overloaded') || err.message.includes('529')));
}

async function claudeWithRetry(params, retries = 4) {
  for (let i = 0; i < retries; i++) {
    try {
      return await anthropic.messages.create(params);
    } catch (err) {
      if (isOverloadedError(err) && i < retries - 1) {
        const wait = [3000, 7000, 15000][i] || 15000;
        console.log(`Anthropic ${err.status} — reintentando en ${wait / 1000}s (${i + 1}/${retries})`);
        await new Promise(r => setTimeout(r, wait));
      } else throw err;
    }
  }
}

async function haiku(systemPrompt, userMessage) {
  const msg = await claudeWithRetry({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 800,
    system: systemPrompt,
    messages: [{ role: 'user', content: userMessage }],
  });
  return msg.content[0].text;
}

async function sonnet(systemPrompt, userMessage) {
  try {
    const msg = await claudeWithRetry({
      model: 'claude-sonnet-4-6',
      max_tokens: 4000,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }],
    });
    return msg.content[0].text;
  } catch (err) {
    if (isOverloadedError(err)) {
      console.log('Sonnet sobrecargado — fallback a Haiku');
      const msg = await claudeWithRetry({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 4000,
        system: systemPrompt + '\n\nSé conciso pero mantén formato y calidad.',
        messages: [{ role: 'user', content: userMessage }],
      });
      return msg.content[0].text;
    }
    throw err;
  }
}

// ─── System prompts ───────────────────────────────────────────────────────────

const TIPSTER_SYSTEM = `Eres el mejor tipster profesional del mundo especializado en mercados de VALOR REAL.

PICKS ABSOLUTAMENTE PROHIBIDAS - NUNCA las des:
- Gana el favorito obvio a cuota menor de 1.80 (gana Bayern, gana Madrid, gana City, gana Barcelona, gana PSG etc)
- Over 2.5 o Over 3.5 de equipos muy ofensivos (Madrid, Barcelona, Bayern, City, PSG) en casa vs rivales débiles — todo el mundo lo sabe, no hay valor
- 1X2 simple a cuota menor de 1.75 - no es tipster, es obvio
- Picks que cualquier persona sin conocimiento daría
- BTTS No cuando un equipo ya marcó 2+ goles en el HT
- PICKS YA RESUELTOS: si el mercado ya se cumplió (ej: BTTS cuando ya hay goles de ambos), NO lo incluyas como pick apostable — omítelo completamente
- MÁXIMO 2 PICKS POR PARTIDO — nunca más de 2 mercados distintos sobre el mismo partido. Si tienes 3 ideas para un partido, elige las 2 mejores y descarta la tercera. Esto es innegociable.
- BTTS en derbis o clásicos de alta tensión táctica (Milan vs Inter, Real Madrid vs Atlético, Arsenal vs Tottenham, Celtic vs Rangers, etc.) — estos partidos se cierran defensivamente, el BTTS falla sistemáticamente
- Asian Handicap de -1 o mayor (-1, -1.5, -2) — falla con frecuencia. Máximo permitido: AH -0.5, y solo si el equipo promedia más de 2.0 goles en su contexto
- BTTS cuando un equipo tiene más del 35% de partidos sin marcar en su contexto (casa o fuera)

MERCADOS DONDE ESTÁ EL VALOR REAL:
1. HT/FT combos específicos
2. Corners Over/Under
3. Tarjetas Over/Under
4. BTTS cuando ambos marcan en más del 68% de sus partidos (umbral estricto)
5. Over 3.5 goles cuando ambos tienen promedio goleador alto
6. DNB (Draw No Bet)
7. Asian Handicap -0.5 máximo
8. HT Over 0.5 o 1.5
9. Gana el visitante cuando el local tiene malos registros en casa

PROCESO DE ANÁLISIS OBLIGATORIO:
Para BTTS: % local marcó en casa + % visitante marcó fuera + % BTTS en H2H. Los 3 deben superar 68% (umbral estricto). Usa probBTTS_Combinada: debe superar 65%. Si uno no llega, NO es pick.
Para Corners: promedio local en casa + visitante fuera. Recomienda Over si total supera línea en +1.5.
Para HT: % local gana 1T en casa. Solo si supera 60%.
Para Tarjetas: suma promedios. Solo si supera línea en +1.
Para Over/Under goles: usa probOver25 y probOver35 del modelo. Si probOver25 > 65% con EV positivo, considera pick.
Para DNB: usa probDNB_Local o probDNB_Visitante. Solo si supera 72% para stake 7+.
Para AH: solo -0.5. Solo si prob de victoria supera 70%. Nunca -1 ni -1.5.

INSTRUCCIONES PARA USAR LAS PROBABILIDADES CALCULADAS:
Si el JSON de datos incluye el campo "probabilidadesCalculadas", DEBES usarlo como base:
- xGLocal / xGVisitante: goles esperados. Si xG local > 1.8 y away < 0.9, el local domina claramente.
- probBTTS_Combinada: combinación de Poisson + H2H. Más fiable que solo H2H. Debe superar 65%.
- expectedValue_vs_CuotasReferencia: si el EV de un mercado es negativo, NO lo recomiendes. Busca mercados con EV > +5%.
- Las probabilidades son calculadas matemáticamente — úsalas para CALIBRAR el stake.

INSTRUCCIONES PARA MOMENTUM EN VIVO:
Si el JSON incluye "momentumEnVivo", úsalo para detectar oportunidades en tiempo real:
- Si domina un equipo (score > 15) pero el marcador no lo refleja aún, considera apuesta al próximo gol de ese equipo.
- Si está equilibrado, prioriza mercados de corners o tarjetas sobre resultado.
- proyeccionCorners.projected > 10: considera Over 9.5 corners si confidence es "alta".
- proyeccionTarjetas.projected > 4: considera Over 3.5 tarjetas si confidence es "alta".

TRADUCCIÓN OBLIGATORIA DE TÉRMINOS TÉCNICOS — SIEMPRE en español:
- failedToScore → "partidos sin marcar"
- cleanSheet / cleanSheets → "portería a cero"
- golesAnotadosHome → "goles anotados en casa"
- golesAnotadosAway → "goles anotados fuera"
- golesRecibidosHome → "goles recibidos en casa"
- golesRecibidosAway → "goles recibidos fuera"
- forma → "forma reciente"
- BTTS → "ambos marcan" (puedes usar BTTS como abreviatura pero explícalo)
- DNB → "sin empate" o "apuesta sin empate"
- Over/Under → "más de / menos de"
- HT → "primer tiempo" o "al descanso"
- FT → "al final del partido"
NUNCA escribas nombres de campos técnicos en inglés en la respuesta al usuario.

Si hay 2+ goles de diferencia en el marcador:
- PROHIBIDO: resultado final (gana X, DNB, 1X2)
- PROHIBIDO: Over goles totales si ya hay 3+ goles y queda poco
- PERMITIDO: Corners Over/Under, Tarjetas Over, BTTS, Next Goal del perdedor, Over goles 2T si va 2-0 al HT

CRITERIO DE STAKE ESTRICTO — umbrales elevados tras análisis de resultados reales:
10/10: +80% probabilidad, cuota mín 1.85, EV > +10%
9/10:  +75% probabilidad, cuota mín 1.75, EV > +8%
8/10:  +70% probabilidad, cuota mín 1.65, EV > +5%
7/10:  +68% probabilidad, cuota mín 1.60, EV > +5%
6/10:  +63% probabilidad, cuota mín 1.50, EV > +3%
1-5:   NUNCA publicar

REGLA DE ORO — calidad sobre cantidad:
Un pick de stake 8-9 bien analizado vale más que tres picks de stake 7 mediocres.
Si dudas entre stake 7 y stake 6, usa stake 6. Sé conservador.
Prefiere 1-2 picks excelentes antes que 4-5 picks mediocres.

Si no hay picks con STAKE 6+: "⛔ Sin picks de valor en este partido. Mejor no apostar."

FORMATO OBLIGATORIO — sigue este formato exacto, sin variaciones:

🌍 [País] — [Liga]
⚽ [Local] vs [Visitante] | ⏰ [Hora Colombia]
━━━━━━━━━━━━━━━━━━━

📊 *ESTADÍSTICAS CLAVE*
▸ [Local] anota en casa: *X.X* por partido
▸ [Visitante] anota fuera: *X.X* por partido
▸ [Local] recibe en casa: *X.X* por partido
▸ Ambos marcan en H2H: *X de 10* partidos
▸ Forma reciente [Local]: *GGPGE*
▸ Forma reciente [Visitante]: *PGEGG*

🎯 *PICK [N]: [Mercado en español]*
┌ Selección: [Qué apostar exactamente]
├ Razonamiento: [Explicación con el dato específico que lo justifica]
├ Probabilidad: [X]%
├ 🏆 Stake: *[X]/10*
├ 💡 Cuota mínima: *[X.XX]*
└ ⚠️ Riesgo: [1 línea máximo]

━━━━━━━━━━━━━━━━━━━
🔥 PICK ESTRELLA DEL DÍA [Solo si stake 8+/10]
━━━━━━━━━━━━━━━━━━━

REGLAS DE FORMATO:
- Usa *texto* para negritas (Telegram Markdown)
- NUNCA uses # ni ## ni ### (headers markdown de escritorio — no funcionan en Telegram)
- NUNCA uses | columnas | ni tablas HTML
- NUNCA menciones fuentes de datos, APIs, plataformas ni herramientas
- NUNCA escribas disclaimers como "el análisis se basa en estadísticas de la temporada X" ni "el fútbol puede cambiar"
- NUNCA muestres valores técnicos internos como xGLocal, xGVisitante, lambdaRem, EV%, score de momentum — estos son solo para tu análisis interno, NO para el usuario
- NUNCA muestres conteos de pases en los primeros minutos (son irrelevantes y confusos)
- La forma reciente MÁXIMO 6 caracteres (ej: GGGPPE). Nunca más de 6.
- Si no hay picks válidos: escribe solo "⛔ Sin picks de valor hoy en este partido. Mejor no apostar."

Responde en español. NUNCA inventes estadísticas. Usa SOLO los datos que recibes.`;

const PICKS_HOY_SYSTEM = `${TIPSTER_SYSTEM}

INSTRUCCIÓN ESPECIAL PARA PICKS DEL DÍA:
Emite EXACTAMENTE 3 picks individuales (partidos diferentes) + 1 APUESTA COMBINADA al final.

━━━━━━━━━━━━━━━━━━━
🎰 *COMBINADA DEL DÍA*
Mínimo 3 selecciones, máximo 5. Mercados y partidos distintos.

▸ [Local] vs [Visitante] → *[mercado]* | Cuota: *X.XX*
▸ [Local] vs [Visitante] → *[mercado]* | Cuota: *X.XX*
▸ [Local] vs [Visitante] → *[mercado]* | Cuota: *X.XX*

🏆 Stake combinada: *[X]/10*
💡 Cuota combinada estimada: *~X.XX*
━━━━━━━━━━━━━━━━━━━`;

const INPLAY_SYSTEM = `${TIPSTER_SYSTEM}

INSTRUCCIÓN ESPECIAL IN-PLAY:
Analiza el marcador, minuto y estadísticas en tiempo real.
Indica el tiempo restante estimado y cuándo actuar.

ANÁLISIS DE MOMENTUM (campo "momentumEnVivo"):
- score > 15: el local domina → favorece apuestas al local (siguiente gol, AH)
- score < -15: el visitante domina → favorece apuestas al visitante
- score entre -15 y 15: partido equilibrado → enfócate en corners y tarjetas
- intensity > 30: dominio muy claro → stake más alto permitido

PROYECCIONES EN TIEMPO REAL:
- proyeccionCorners.projected: si > 10.5 con confidence "alta" → considera Over 9.5/10.5
- proyeccionCorners.remaining: cuántos corners faltan (para saber si vale apostar ahora)
- proyeccionTarjetas.projected: si > 4.5 con confidence "alta" → considera Over 3.5/4.5
- Solo usa proyecciones con confidence "alta" (min 30 minutos jugados) para picks de stake 7+

FORMATO ADICIONAL IN-PLAY:
⏰ Actúa antes del min: [XX]
📈 Ritmo corners: [proyeccionCorners.pace] corners/90min → proyectados [proyeccionCorners.projected] al final
📊 Momentum: [momentumEnVivo.label] (score: [momentumEnVivo.score])`;

// ─── Intent detection ─────────────────────────────────────────────────────────

const INTENT_SYSTEM = `Eres un clasificador de intenciones para un bot tipster de fútbol. Responde ÚNICAMENTE con JSON válido.

Intenciones disponibles:
- "picks_hoy": picks generales del día en todas las ligas
- "picks_liga": picks del día de una liga específica
- "partido_especifico": análisis de un equipo o partido, con o sin pregunta de mercado específico
- "en_vivo": partidos en vivo, con o sin filtro de liga o mercado
- "alerta_gol": alerta de probabilidad de gol en vivo — detecta partidos con mayor prob de gol próximo y buena cuota. Se activa con palabras como "alerta gol", "alerta de gol", "probabilidad gol", "donde puede haber gol", "gol en vivo", "partido con gol", "cuota gol"
- "estadisticas": rendimiento/historial de picks que el bot ha emitido
- "chat_general": saludos, preguntas generales de fútbol, conversación
- "ver_planes": usuario pregunta por precios, planes, suscripción, VIP, PRO
- "rachas": buscar equipos con rachas activas (5+ partidos consecutivos con una estadística). Se activa con palabras como "rachas", "racha", "racha de", "equipos que llevan", "equipos con racha"

Estructura JSON SIEMPRE completa:
{
  "intencion": "picks_hoy|picks_liga|partido_especifico|en_vivo|alerta_gol|estadisticas|chat_general|ver_planes|rachas",
  "equipo": "nombre del equipo mencionado o null",
  "liga": "nombre de la liga mencionada o null",
  "pregunta_especifica": "la pregunta exacta del usuario",
  "mercado": "goles|goles_1T|corners|tarjetas|BTTS|resultado|resultado_1T|handicap|null",
  "tiempo": "1T|2T|FT|null",
  "contexto": "en_vivo|proximo_partido|hoy|null",
  "period": "hoy|ayer|semana|total o null",
  "venue": "home|away|all"
}

Ejemplos:
- "picks de hoy" → {"intencion":"picks_hoy","equipo":null,"liga":null,"pregunta_especifica":"picks de hoy","mercado":null,"tiempo":null,"contexto":"hoy","period":null}
- "partidos bundesliga hoy" → {"intencion":"picks_liga","equipo":null,"liga":"bundesliga","pregunta_especifica":"partidos bundesliga hoy","mercado":null,"tiempo":null,"contexto":"hoy","period":null}
- "bundesliga en vivo" → {"intencion":"en_vivo","equipo":null,"liga":"bundesliga","pregunta_especifica":"bundesliga en vivo","mercado":null,"tiempo":null,"contexto":"en_vivo","period":null}
- "analiza el Real Madrid" → {"intencion":"partido_especifico","equipo":"Real Madrid","liga":null,"pregunta_especifica":"analiza el Real Madrid","mercado":null,"tiempo":null,"contexto":"proximo_partido","period":null}
- "probabilidad que el Real Madrid gane el 1T hoy" → {"intencion":"partido_especifico","equipo":"Real Madrid","liga":null,"pregunta_especifica":"probabilidad que el Real Madrid gane el 1T hoy","mercado":"resultado_1T","tiempo":"1T","contexto":"hoy","period":null}
- "cuantos corners suelen meter el PSG" → {"intencion":"partido_especifico","equipo":"PSG","liga":null,"pregunta_especifica":"cuantos corners suelen meter el PSG","mercado":"corners","tiempo":"FT","contexto":"proximo_partido","period":null}
- "que hay en vivo" → {"intencion":"en_vivo","equipo":null,"liga":null,"pregunta_especifica":"que hay en vivo","mercado":null,"tiempo":null,"contexto":"en_vivo","period":null}
- "en vivo hay partidos con muchos goles en el 1T" → {"intencion":"en_vivo","equipo":null,"liga":null,"pregunta_especifica":"hay partidos en vivo con muchos goles en el 1T","mercado":"goles_1T","tiempo":"1T","contexto":"en_vivo","period":null}
- "cuantos picks acerté hoy" → {"intencion":"estadisticas","equipo":null,"liga":null,"pregunta_especifica":"cuantos picks acerté hoy","mercado":null,"tiempo":null,"contexto":null,"period":"hoy"}
- "rendimiento de esta semana" → {"intencion":"estadisticas","equipo":null,"liga":null,"pregunta_especifica":"rendimiento de esta semana","mercado":null,"tiempo":null,"contexto":null,"period":"semana"}
- "% de aciertos de ayer" → {"intencion":"estadisticas","equipo":null,"liga":null,"pregunta_especifica":"% de aciertos de ayer","mercado":null,"tiempo":null,"contexto":null,"period":"ayer"}
- "historial total" → {"intencion":"estadisticas","equipo":null,"liga":null,"pregunta_especifica":"historial total","mercado":null,"tiempo":null,"contexto":null,"period":"total"}
- "hola" → {"intencion":"chat_general","equipo":null,"liga":null,"pregunta_especifica":"hola","mercado":null,"tiempo":null,"contexto":null,"period":null}
- "ver planes" → {"intencion":"ver_planes","equipo":null,"liga":null,"pregunta_especifica":"ver planes","mercado":null,"tiempo":null,"contexto":null,"period":null}
- "quiero PRO" → {"intencion":"ver_planes","equipo":null,"liga":null,"pregunta_especifica":"quiero PRO","mercado":null,"tiempo":null,"contexto":null,"period":null}
- "precios" → {"intencion":"ver_planes","equipo":null,"liga":null,"pregunta_especifica":"precios","mercado":null,"tiempo":null,"contexto":null,"period":null,"venue":"all"}
- "alerta gol" → {"intencion":"alerta_gol","equipo":null,"liga":null,"pregunta_especifica":"alerta gol","mercado":"goles","tiempo":null,"contexto":"en_vivo","period":null,"venue":"all"}
- "gol en vivo" → {"intencion":"alerta_gol","equipo":null,"liga":null,"pregunta_especifica":"gol en vivo","mercado":"goles","tiempo":null,"contexto":"en_vivo","period":null,"venue":"all"}
- "donde puede haber gol" → {"intencion":"alerta_gol","equipo":null,"liga":null,"pregunta_especifica":"donde puede haber gol","mercado":"goles","tiempo":null,"contexto":"en_vivo","period":null,"venue":"all"}
- "probabilidad de gol" → {"intencion":"alerta_gol","equipo":null,"liga":null,"pregunta_especifica":"probabilidad de gol","mercado":"goles","tiempo":null,"contexto":"en_vivo","period":null,"venue":"all"}
- "partido con gol ahora" → {"intencion":"alerta_gol","equipo":null,"liga":null,"pregunta_especifica":"partido con gol ahora","mercado":"goles","tiempo":null,"contexto":"en_vivo","period":null,"venue":"all"}
- "IMPORTANTE: si el usuario menciona 'gol' junto con contexto en vivo o inmediato, usar alerta_gol, NO en_vivo"
- "rachas Premier League" → {"intencion":"rachas","equipo":null,"liga":"Premier League","pregunta_especifica":"rachas Premier League","mercado":null,"tiempo":null,"contexto":null,"period":null,"venue":"all"}
- "rachas Real Madrid" → {"intencion":"rachas","equipo":"Real Madrid","liga":null,"pregunta_especifica":"rachas Real Madrid","mercado":null,"tiempo":null,"contexto":null,"period":null,"venue":"all"}
- "rachas en casa Serie A" → {"intencion":"rachas","equipo":null,"liga":"Serie A","pregunta_especifica":"rachas en casa Serie A","mercado":null,"tiempo":null,"contexto":null,"period":null,"venue":"home"}
- "rachas de visita Atletico Madrid" → {"intencion":"rachas","equipo":"Atletico Madrid","liga":null,"pregunta_especifica":"rachas de visita Atletico Madrid","mercado":null,"tiempo":null,"contexto":null,"period":null,"venue":"away"}
- "equipos con racha de goles Bundesliga" → {"intencion":"rachas","equipo":null,"liga":"Bundesliga","pregunta_especifica":"equipos con racha de goles Bundesliga","mercado":"goles","tiempo":null,"contexto":null,"period":null,"venue":"all"}`;

async function detectIntent(message) {
  const raw = await haiku(INTENT_SYSTEM, message);
  try {
    const m = raw.match(/\{[\s\S]*?\}/);
    return m ? JSON.parse(m[0]) : { intencion: 'chat_general', pregunta_especifica: message };
  } catch {
    return { intencion: 'chat_general', pregunta_especifica: message };
  }
}

// ─── Pick Tracking ────────────────────────────────────────────────────────────

const PICKS_FILE = path.join(__dirname, 'picks.json');

function loadPicks() {
  try { return JSON.parse(fs.readFileSync(PICKS_FILE, 'utf8')); }
  catch { return []; }
}

function persistPicks(picks) {
  fs.writeFileSync(PICKS_FILE, JSON.stringify(picks, null, 2));
}

const EXTRACT_PICKS_SYSTEM = `Eres un extractor de picks de apuestas deportivas. Dado un texto de análisis de tipster, extrae TODOS los picks concretos emitidos.
Para cada pick devuelve un objeto JSON con estos campos:
- local: nombre del equipo local (string)
- visitante: nombre del equipo visitante (string)
- mercado: tipo de mercado. Usa UNO de: BTTS_YES, BTTS_NO, OVER_GOALS, UNDER_GOALS, OVER_CORNERS, UNDER_CORNERS, OVER_CARDS, UNDER_CARDS, HOME_WIN, AWAY_WIN, DRAW, AH_HOME, AH_AWAY, HT_OVER, HT_RESULT, DNB_HOME, DNB_AWAY, OTHER
- seleccion: descripción exacta del pick tal como aparece en el texto (ej: "Over 2.5 goles FT", "BTTS Yes", "Atlético Madrid -1 AH")
- linea: número de la línea si aplica (ej: 2.5 para Over 2.5, 7.5 para corners Over 7.5, null si no aplica)
- handicap: valor del handicap si es AH (ej: -1, +0.5, null si no es AH)
- cuota: cuota estimada como número (null si no se menciona)
- stake: stake como número del 1 al 10 (null si no se menciona)
- esCombinada: true si es parte de una apuesta combinada, false si es individual

Responde SOLO con un JSON array. Si no hay picks claros, responde [].`;

async function extractPicksFromText(analysisText, matchesCtx) {
  const contextStr = matchesCtx.map(m => `fixtureId=${m.fixtureId} | ${m.local} vs ${m.visitante} (${m.liga})`).join('\n');
  const raw = await haiku(
    EXTRACT_PICKS_SYSTEM,
    `Contexto de partidos analizados:\n${contextStr}\n\nTexto del tipster:\n${analysisText}`
  );
  try {
    const jsonMatch = raw.match(/\[[\s\S]*\]/);
    return jsonMatch ? JSON.parse(jsonMatch[0]) : [];
  } catch { return []; }
}

async function recordPicks(analysisText, matchesCtx) {
  if (!analysisText || !matchesCtx.length) return;
  try {
    const extracted = await extractPicksFromText(analysisText, matchesCtx);
    if (!extracted.length) { console.log('📝 No se extrajeron picks estructurados'); return; }

    const existing = loadPicks();
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });

    const newPicks = extracted.map(p => {
      // Match pick to a fixture from context
      const matched = matchesCtx.find(m =>
        m.local.toLowerCase().includes((p.local || '').toLowerCase().split(' ')[0]) ||
        m.visitante.toLowerCase().includes((p.visitante || '').toLowerCase().split(' ')[0]) ||
        (p.local || '').toLowerCase().includes(m.local.toLowerCase().split(' ')[0])
      );
      return {
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        emitidoAt: new Date().toISOString(),
        fecha: today,
        fixtureId: matched?.fixtureId || null,
        fechaPartido: matched?.fechaPartido || null,
        liga: matched?.liga || p.liga || null,
        local: p.local,
        visitante: p.visitante,
        mercado: p.mercado,
        seleccion: p.seleccion,
        linea: p.linea ?? null,
        handicap: p.handicap ?? null,
        cuota: p.cuota ?? null,
        stake: p.stake ?? null,
        esCombinada: p.esCombinada || false,
        resultado: null,
        scoresFinal: null,
      };
    });

    persistPicks([...existing, ...newPicks]);
    console.log(`📝 ${newPicks.length} picks guardados en picks.json`);
  } catch (e) {
    console.error('recordPicks error:', e.message);
  }
}

async function evaluatePickResult(pick, fixture, stats) {
  const goalsHome = fixture.goals?.home ?? 0;
  const goalsAway = fixture.goals?.away ?? 0;
  const htHome    = fixture.score?.halftime?.home ?? null;
  const htAway    = fixture.score?.halftime?.away ?? null;
  const total     = goalsHome + goalsAway;
  const sel       = (pick.seleccion || '').toLowerCase();
  const linea     = pick.linea;

  // BTTS
  if (pick.mercado === 'BTTS_YES') return (goalsHome > 0 && goalsAway > 0) ? 'W' : 'L';
  if (pick.mercado === 'BTTS_NO')  return (goalsHome === 0 || goalsAway === 0) ? 'W' : 'L';

  // Over/Under goals
  if (pick.mercado === 'OVER_GOALS'  && linea != null) return total > linea ? 'W' : 'L';
  if (pick.mercado === 'UNDER_GOALS' && linea != null) return total < linea ? 'W' : 'L';

  // HT Over
  if (pick.mercado === 'HT_OVER' && linea != null && htHome != null)
    return (htHome + htAway) > linea ? 'W' : 'L';

  // 1X2
  if (pick.mercado === 'HOME_WIN') return goalsHome > goalsAway ? 'W' : 'L';
  if (pick.mercado === 'AWAY_WIN') return goalsAway > goalsHome ? 'W' : 'L';
  if (pick.mercado === 'DRAW')     return goalsHome === goalsAway ? 'W' : 'L';

  // DNB
  if (pick.mercado === 'DNB_HOME') {
    if (goalsHome > goalsAway) return 'W';
    if (goalsHome === goalsAway) return 'V';
    return 'L';
  }
  if (pick.mercado === 'DNB_AWAY') {
    if (goalsAway > goalsHome) return 'W';
    if (goalsHome === goalsAway) return 'V';
    return 'L';
  }

  // Asian Handicap
  if ((pick.mercado === 'AH_HOME' || pick.mercado === 'AH_AWAY') && pick.handicap != null) {
    const h = parseFloat(pick.handicap);
    const adjHome = goalsHome + (pick.mercado === 'AH_HOME' ? h : -h);
    const adjAway = goalsAway + (pick.mercado === 'AH_AWAY' ? h : -h);
    if (adjHome === adjAway) return 'V';
    if (pick.mercado === 'AH_HOME') return adjHome > adjAway ? 'W' : 'L';
    return adjAway > adjHome ? 'W' : 'L';
  }

  // Corners / Cards — need stats
  if (stats && (pick.mercado === 'OVER_CORNERS' || pick.mercado === 'UNDER_CORNERS') && linea != null) {
    const cornersHome = Object.values(stats)[0]?.['Corner Kicks'] ?? null;
    const cornersAway = Object.values(stats)[1]?.['Corner Kicks'] ?? null;
    if (cornersHome != null && cornersAway != null) {
      const totalCorners = cornersHome + cornersAway;
      if (pick.mercado === 'OVER_CORNERS')  return totalCorners > linea ? 'W' : 'L';
      if (pick.mercado === 'UNDER_CORNERS') return totalCorners < linea ? 'W' : 'L';
    }
  }
  if (stats && (pick.mercado === 'OVER_CARDS' || pick.mercado === 'UNDER_CARDS') && linea != null) {
    const cardsHome = (Object.values(stats)[0]?.['Yellow Cards'] ?? 0) + (Object.values(stats)[0]?.['Red Cards'] ?? 0);
    const cardsAway = (Object.values(stats)[1]?.['Yellow Cards'] ?? 0) + (Object.values(stats)[1]?.['Red Cards'] ?? 0);
    const totalCards = cardsHome + cardsAway;
    if (pick.mercado === 'OVER_CARDS')  return totalCards > linea ? 'W' : 'L';
    if (pick.mercado === 'UNDER_CARDS') return totalCards < linea ? 'W' : 'L';
  }

  return '?'; // can't determine automatically
}

async function evaluatePendingPicks() {
  const picks = loadPicks();
  const pending = picks.filter(p => p.resultado === null && p.fixtureId);
  if (!pending.length) return picks;

  const fixtureIds = [...new Set(pending.map(p => p.fixtureId))];
  const fixtureMap = {};

  await Promise.allSettled(fixtureIds.map(async (fid) => {
    const { data } = await API.get('/fixtures', { params: { id: fid } });
    const f = data.response?.[0];
    if (f && ['FT', 'AET', 'PEN'].includes(f.fixture.status.short)) {
      const stats = await getFixtureStatistics(fid).catch(() => null);
      fixtureMap[fid] = { fixture: f, stats };
    }
  }));

  for (const pick of picks) {
    if (pick.resultado !== null || !pick.fixtureId) continue;
    const entry = fixtureMap[pick.fixtureId];
    if (!entry) continue;
    pick.resultado = await evaluatePickResult(pick, entry.fixture, entry.stats);
    pick.scoresFinal = { home: entry.fixture.goals?.home, away: entry.fixture.goals?.away };
    console.log(`📊 Pick evaluado: ${pick.local} vs ${pick.visitante} — ${pick.seleccion} → ${pick.resultado}`);
  }

  persistPicks(picks);
  return picks;
}

async function handleEstadisticas(chatId, period = 'hoy') {
  await bot.sendMessage(chatId, '📊 Evaluando resultados de tus picks...');

  const allPicks = await evaluatePendingPicks();
  const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });

  const ayer = new Date();
  ayer.setDate(ayer.getDate() - 1);
  const ayerStr = ayer.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });

  const filtered = period === 'semana'
    ? allPicks.filter(p => {
        const d = new Date(p.fecha);
        const now = new Date();
        return (now - d) <= 7 * 24 * 60 * 60 * 1000;
      })
    : period === 'total'
      ? allPicks
      : period === 'ayer'
        ? allPicks.filter(p => p.fecha === ayerStr)
        : allPicks.filter(p => p.fecha === today);

  const label = period === 'semana' ? 'ESTA SEMANA' : period === 'total' ? 'HISTORIAL TOTAL' : period === 'ayer' ? `AYER (${ayerStr})` : `HOY (${today})`;

  if (!filtered.length) {
    const periodoLabel = period === 'hoy' ? 'de hoy' : period === 'ayer' ? 'de ayer' : 'en el período seleccionado';
    return bot.sendMessage(chatId, `😔 No hay picks registrados ${periodoLabel}.`);
  }

  const individual  = filtered.filter(p => !p.esCombinada);
  const evaluados   = filtered.filter(p => ['W', 'L'].includes(p.resultado));
  const wins        = evaluados.filter(p => p.resultado === 'W').length;
  const losses      = evaluados.filter(p => p.resultado === 'L').length;
  const voids       = filtered.filter(p => p.resultado === 'V').length;
  const pendientes  = filtered.filter(p => p.resultado === null || p.resultado === '?').length;
  const pct         = evaluados.length ? Math.round((wins / evaluados.length) * 100) : null;

  let text = `📊 *RENDIMIENTO — ${label}*\n`;
  text += `━━━━━━━━━━━━━━━━━━━\n`;
  text += `✅ Ganados:    ${wins}\n`;
  text += `❌ Perdidos:   ${losses}\n`;
  if (voids)      text += `↩️ Void/Nulos:  ${voids}\n`;
  if (pendientes) text += `⏳ Pendientes: ${pendientes}\n`;
  text += `━━━━━━━━━━━━━━━━━━━\n`;
  if (pct !== null) {
    const icon = pct >= 60 ? '🔥' : pct >= 40 ? '📈' : '📉';
    text += `${icon} *Aciertos: ${pct}% (${wins}/${evaluados.length})*\n\n`;
  }

  text += `*Detalle de picks:*\n`;
  for (const p of filtered) {
    const icon = p.resultado === 'W' ? '✅' : p.resultado === 'L' ? '❌' : p.resultado === 'V' ? '↩️' : '⏳';
    const score = p.scoresFinal ? ` *(${p.scoresFinal.home}-${p.scoresFinal.away})*` : '';
    const combo = p.esCombinada ? ' 🔗' : '';
    text += `${icon}${combo} ${p.local} vs ${p.visitante}${score}\n`;
    text += `   └ ${p.seleccion}`;
    if (p.cuota) text += ` @ ${p.cuota}`;
    if (p.stake) text += ` | STAKE ${p.stake}/10`;
    text += '\n';
  }

  await sendLong(chatId, text, { parse_mode: 'Markdown' });
}

// ─── Format helpers ───────────────────────────────────────────────────────────

function formatHour(isoDate) {
  if (!isoDate) return '??:??';
  return new Date(isoDate).toLocaleTimeString('es-CO', {
    hour: '2-digit', minute: '2-digit', timeZone: 'America/Bogota',
  });
}

function todayDate() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
}

// ─── Telegram send helper ─────────────────────────────────────────────────────

const TG_LIMIT = 4000;

function normalizeMd(text) {
  // Telegram legacy Markdown uses *bold*, not **bold**
  return text.replace(/\*\*(.+?)\*\*/g, '*$1*');
}

async function sendLong(chatId, text, options = {}) {
  if (options.parse_mode === 'Markdown') text = normalizeMd(text);
  if (text.length <= TG_LIMIT) return bot.sendMessage(chatId, text, options);
  const paragraphs = text.split(/\n\n+/);
  let chunk = '';
  for (const para of paragraphs) {
    const addition = (chunk ? '\n\n' : '') + para;
    if (chunk.length + addition.length > TG_LIMIT) {
      await bot.sendMessage(chatId, chunk, options);
      chunk = para;
    } else {
      chunk += addition;
    }
  }
  if (chunk) await bot.sendMessage(chatId, chunk, options);
}

// ─── Business flows ───────────────────────────────────────────────────────────

async function handlePicksHoy(chatId, forceRefresh = false) {
  const today = todayDate();

  // ── Caché: devuelve picks ya generados hoy sin volver a consultar ─────────
  if (!forceRefresh) {
    const cached = getPicksCache('all');
    if (cached) {
      console.log(`📦 Cache hit — picks del día ya generados a las ${cached.generadoAt}`);
      await bot.sendMessage(
        chatId,
        `📦 _Picks ya generados hoy (${cached.generadoAt.slice(11, 16)} hora UTC). Mostrando análisis guardado:_`,
        { parse_mode: 'Markdown' }
      );
      return sendLong(chatId, `📅 *PICKS DEL DÍA — ${today}*\n\n${cached.picksText}`, { parse_mode: 'Markdown' });
    }
  }

  await bot.sendMessage(chatId, '🔍 Consultando nuestra base de datos estadística...');
  const fixtures = await getFixturesByDate(today);

  if (fixtures.length === 0) {
    return bot.sendMessage(chatId, `😔 No hay partidos en las ligas monitoreadas hoy (${today}).`);
  }

  const selected = [...fixtures]
    .sort((a, b) => (LEAGUE_PRIORITY[b.leagueId] || 0) - (LEAGUE_PRIORITY[a.leagueId] || 0))
    .slice(0, 8);

  console.log(`✅ Seleccionados ${selected.length} partidos para análisis`);
  await bot.sendMessage(chatId, `📊 ${fixtures.length} partidos identificados. Recopilando estadísticas de equipos...`);

  // Fetch team stats in batches of 4 to avoid rate limit
  const statsPairs = selected.flatMap(f => [
    getTeamStats(f.homeId, f.leagueId),
    getTeamStats(f.awayId, f.leagueId),
  ]);
  const statsResults = [];
  for (let i = 0; i < statsPairs.length; i += 4) {
    const batch = await Promise.allSettled(statsPairs.slice(i, i + 4));
    statsResults.push(...batch);
    if (i + 4 < statsPairs.length) await new Promise(r => setTimeout(r, 6000));
  }

  const enriched = selected.map((f, i) => {
    const homeStats = statsResults[i * 2].status === 'fulfilled' ? statsResults[i * 2].value : null;
    const awayStats = statsResults[i * 2 + 1].status === 'fulfilled' ? statsResults[i * 2 + 1].value : null;
    const probBlock = buildProbBlock(homeStats, awayStats, []);
    return {
      fixtureId:      f.fixtureId,
      liga:           f.leagueName,
      local:          f.homeTeam,
      visitante:      f.awayTeam,
      hora:           formatHour(f.date),
      fechaPartido:   f.date,
      statsLocal:     homeStats,
      statsVisitante: awayStats,
      ...(probBlock && { probabilidadesCalculadas: probBlock }),
    };
  });

  await bot.sendMessage(chatId, `🧠 Calculando picks de valor...`);

  const picksText = await sonnet(
    PICKS_HOY_SYSTEM,
    `Partidos del día ${today} (hora Colombia). DATOS REALES DE API-FOOTBALL:\n\n${JSON.stringify(enriched, null, 2)}\n\nEmite EXACTAMENTE 3 picks individuales + 1 combinada basadas SOLO en estos datos reales. Usa las probabilidadesCalculadas para validar cada pick — solo recomienda si el EV es positivo o cercano a 0 y la prob supera el umbral de stake.`
  );

  // Guardar en caché para evitar re-análisis y picks contradictorios
  setPicksCache('all', picksText, enriched.map(f => f.fixtureId));

  await sendLong(chatId, `📅 *PICKS DEL DÍA — ${today}*\n\n${picksText}`, { parse_mode: 'Markdown' });
  recordPicks(picksText, enriched.map(f => ({ fixtureId: f.fixtureId, local: f.local, visitante: f.visitante, liga: f.liga, fechaPartido: f.fechaPartido }))).catch(e => console.error('recordPicks:', e.message));
}

async function handlePicksLiga(chatId, leagueName, forceRefresh = false) {
  const leagueId = findLeagueId(leagueName);
  const leagueInfo = leagueId ? LEAGUE_MAP[leagueId] : null;
  const displayName = leagueInfo?.name || leagueName;
  const cacheScope  = `liga_${leagueId || leagueName}`;

  // ── Caché por liga ────────────────────────────────────────────────────────
  if (!forceRefresh) {
    const cached = getPicksCache(cacheScope);
    if (cached) {
      console.log(`📦 Cache hit — picks ${displayName} ya generados a las ${cached.generadoAt}`);
      await bot.sendMessage(
        chatId,
        `📦 _Picks de ${displayName} ya generados hoy (${cached.generadoAt.slice(11, 16)} UTC). Mostrando análisis guardado:_`,
        { parse_mode: 'Markdown' }
      );
      return sendLong(chatId, `📅 *${displayName} — ${todayDate()}*\n\n${cached.picksText}`, { parse_mode: 'Markdown' });
    }
  }

  await bot.sendMessage(chatId, `🔍 Consultando nuestra base de datos — ${displayName}...`);

  const today = todayDate();
  const all = await fetchFixturesByDate(today);

  let fixtures = leagueId
    ? all.filter(f => f.league.id === leagueId).map(parseFixture)
    : all.filter(f => {
        const n = (f.league.name || '').toLowerCase();
        return n.includes(leagueName.toLowerCase());
      }).map(parseFixture);

  console.log(`📊 Partidos encontrados para ${displayName}: ${fixtures.length}`);

  if (fixtures.length === 0) {
    return bot.sendMessage(chatId, `😔 No hay partidos de *${displayName}* programados para hoy.`, { parse_mode: 'Markdown' });
  }

  await bot.sendMessage(chatId, `📊 ${fixtures.length} partido(s) de *${displayName}* encontrados. Recopilando estadísticas de equipos...`, { parse_mode: 'Markdown' });

  // Fetch team stats in batches of 4 to avoid rate limit
  const statsPairs = fixtures.flatMap(f => [
    getTeamStats(f.homeId, f.leagueId),
    getTeamStats(f.awayId, f.leagueId),
  ]);
  const statsResults = [];
  for (let i = 0; i < statsPairs.length; i += 4) {
    const batch = await Promise.allSettled(statsPairs.slice(i, i + 4));
    statsResults.push(...batch);
    if (i + 4 < statsPairs.length) await new Promise(r => setTimeout(r, 6000));
  }

  const enriched = fixtures.map((f, i) => {
    const homeStats = statsResults[i * 2]?.status === 'fulfilled' ? statsResults[i * 2].value : null;
    const awayStats = statsResults[i * 2 + 1]?.status === 'fulfilled' ? statsResults[i * 2 + 1].value : null;
    const probBlock = buildProbBlock(homeStats, awayStats, []);
    return {
      fixtureId:      f.fixtureId,
      liga:           f.leagueName,
      local:          f.homeTeam,
      visitante:      f.awayTeam,
      hora:           formatHour(f.date),
      fechaPartido:   f.date,
      statsLocal:     homeStats,
      statsVisitante: awayStats,
      ...(probBlock && { probabilidadesCalculadas: probBlock }),
    };
  });

  await bot.sendMessage(chatId, `🧠 Calculando picks de valor...`);

  const picksText = await sonnet(
    PICKS_HOY_SYSTEM,
    `Partidos de ${displayName} del día ${today}. DATOS REALES DE API:\n\n${JSON.stringify(enriched, null, 2)}\n\nAnaliza y emite picks de valor basadas SOLO en estos datos reales. Usa las probabilidadesCalculadas para validar cada pick — solo recomienda si el EV es positivo o cercano a 0 y la prob supera el umbral de stake.`
  );

  // Guardar en caché para evitar re-análisis y picks contradictorios
  setPicksCache(cacheScope, picksText, enriched.map(f => f.fixtureId));

  await sendLong(chatId, `📅 *${displayName} — ${today}*\n\n${picksText}`, { parse_mode: 'Markdown' });
  recordPicks(picksText, enriched.map(f => ({ fixtureId: f.fixtureId, local: f.local, visitante: f.visitante, liga: f.liga, fechaPartido: f.fechaPartido }))).catch(e => console.error('recordPicks:', e.message));
}

async function handlePartido(chatId, teamName, countryHint = '') {
  await bot.sendMessage(chatId, `🔍 Buscando *${teamName}* en nuestra base de datos...`, { parse_mode: 'Markdown' });

  const teamData = await searchTeam(teamName, countryHint);
  if (!teamData) {
    return bot.sendMessage(chatId, `❌ No encontré el equipo "${teamName}" en nuestra base de datos.`);
  }

  const teamId   = teamData.team.id;
  const teamFull = teamData.team.name;
  await bot.sendMessage(chatId, `✅ *${teamFull}* encontrado. Analizando próximo partido...`, { parse_mode: 'Markdown' });

  const nextRaw = await findNextFixtureByDate(teamId, 14);
  if (!nextRaw) {
    return bot.sendMessage(chatId, `😔 No encontré próximos partidos para *${teamFull}* en los próximos 14 días.`, { parse_mode: 'Markdown' });
  }

  const homeId   = nextRaw.teams.home.id;
  const awayId   = nextRaw.teams.away.id;
  const leagueId = nextRaw.league.id;
  const homeTeam = nextRaw.teams.home.name;
  const awayTeam = nextRaw.teams.away.name;
  const isLive   = ['1H','HT','2H','ET','P'].includes(nextRaw.fixture?.status?.short);

  await bot.sendMessage(
    chatId,
    `⚽ ${isLive ? '🔴 EN VIVO: ' : 'Próximo: '}*${homeTeam} vs ${awayTeam}*\n🏆 ${nextRaw.league.name} | ⏰ ${formatHour(nextRaw.fixture.date)}\n\n📊 Consultando historial y estadísticas${isLive ? ' en tiempo real' : ''}...`,
    { parse_mode: 'Markdown' }
  );

  const requests = [
    getH2H(homeId, awayId),
    getTeamStats(homeId, leagueId),
    getTeamStats(awayId, leagueId),
  ];
  if (isLive) requests.push(getFixtureStatistics(nextRaw.fixture.id));

  const [h2hRes, homeStatsRes, awayStatsRes, liveStatsRes] = await Promise.allSettled(requests);

  const h2hData      = h2hRes.status === 'fulfilled'      ? h2hRes.value      : [];
  const homeStatsData= homeStatsRes.status === 'fulfilled' ? homeStatsRes.value : null;
  const awayStatsData= awayStatsRes.status === 'fulfilled' ? awayStatsRes.value : null;
  const liveStatsData= (isLive && liveStatsRes?.status === 'fulfilled') ? liveStatsRes.value : null;

  // Calcular probabilidades con modelo de Poisson
  const probBlock = buildProbBlock(homeStatsData, awayStatsData, h2hData);

  // Momentum y proyecciones en vivo
  const momentum   = isLive ? calcLiveMomentum(liveStatsData, homeTeam, awayTeam) : null;
  const elapsed    = nextRaw.fixture?.status?.elapsed || 0;
  const homeCorners= liveStatsData ? (Object.values(liveStatsData)[0]?.['Corner Kicks'] ?? 0) : 0;
  const awayCorners= liveStatsData ? (Object.values(liveStatsData)[1]?.['Corner Kicks'] ?? 0) : 0;
  const cornersProj= isLive && elapsed > 0
    ? calcLiveProjection(homeCorners + awayCorners, elapsed)
    : null;
  const homeCards  = liveStatsData
    ? ((Object.values(liveStatsData)[0]?.['Yellow Cards'] ?? 0) + (Object.values(liveStatsData)[0]?.['Red Cards'] ?? 0))
    : 0;
  const awayCards  = liveStatsData
    ? ((Object.values(liveStatsData)[1]?.['Yellow Cards'] ?? 0) + (Object.values(liveStatsData)[1]?.['Red Cards'] ?? 0))
    : 0;
  const cardsProj  = isLive && elapsed > 0
    ? calcLiveProjection(homeCards + awayCards, elapsed)
    : null;

  const analysisData = {
    partido: {
      liga:      nextRaw.league.name,
      pais:      nextRaw.league.country,
      fecha:     nextRaw.fixture.date.split('T')[0],
      hora:      formatHour(nextRaw.fixture.date),
      local:     homeTeam,
      visitante: awayTeam,
      enVivo:    isLive,
      minuto:    elapsed || null,
      marcador:  isLive ? `${nextRaw.goals?.home ?? 0}-${nextRaw.goals?.away ?? 0}` : null,
    },
    h2h:            h2hData,
    bttsEnH2H:      h2hData.filter(m => m.btts).length,
    statsLocal:     homeStatsData,
    statsVisitante: awayStatsData,
    estadisticasVivo: liveStatsData,
    ...(probBlock   && { probabilidadesCalculadas: probBlock }),
    ...(momentum    && { momentumEnVivo: momentum }),
    ...(cornersProj && { proyeccionCorners: cornersProj }),
    ...(cardsProj   && { proyeccionTarjetas: cardsProj }),
  };

  await bot.sendMessage(chatId, '⚡ Procesando análisis profesional...');
  const system = isLive ? INPLAY_SYSTEM : TIPSTER_SYSTEM;
  const season = LEAGUE_SEASONS[leagueId] || 2025;
  const analysis = await sonnet(
    system,
    `Analiza este partido con DATOS REALES de API-Football (temporada ${season}):\n\n${JSON.stringify(analysisData, null, 2)}`
  );
  await sendLong(chatId, `🎯 *${homeTeam} vs ${awayTeam}*\n\n${analysis}`, { parse_mode: 'Markdown' });
  recordPicks(analysis, [{ fixtureId: nextRaw.fixture.id, local: homeTeam, visitante: awayTeam, liga: nextRaw.league.name, fechaPartido: nextRaw.fixture.date }]).catch(e => console.error('recordPicks:', e.message));
}

async function handleVivo(chatId, leagueId = null, leagueName = null) {
  const displayName = leagueName || 'todas las ligas';
  await bot.sendMessage(chatId, `📡 Obteniendo datos en tiempo real (${displayName})...`);

  const liveFixtures = await getLiveFixtures(leagueId);
  console.log(`📊 Partidos en vivo encontrados: ${liveFixtures.length}`);

  if (liveFixtures.length === 0) {
    const msg = leagueName
      ? `😔 No hay partidos de *${leagueName}* en vivo ahora mismo.`
      : '😔 No hay partidos en curso en las ligas monitoreadas ahora mismo.';
    return bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
  }

  await bot.sendMessage(chatId, `⏳ *${liveFixtures.length}* partido(s) en vivo detectados. Recopilando estadísticas...`, { parse_mode: 'Markdown' });

  // Get live stats for up to 4 matches (API call limit)
  const toAnalyze = liveFixtures.slice(0, 4);
  const statsResults = await Promise.allSettled(
    toAnalyze.map(f => getFixtureStatistics(f.fixtureId))
  );

  const enriched = toAnalyze.map((f, i) => {
    const liveStats = statsResults[i].status === 'fulfilled' ? statsResults[i].value : null;
    const elapsed   = f.elapsed || 0;

    // Momentum en vivo
    const momentum = calcLiveMomentum(liveStats, f.homeTeam, f.awayTeam);

    // Proyección de corners al ritmo actual
    const homeCorners = liveStats ? (Object.values(liveStats)[0]?.['Corner Kicks'] ?? 0) : 0;
    const awayCorners = liveStats ? (Object.values(liveStats)[1]?.['Corner Kicks'] ?? 0) : 0;
    const cornersProj = elapsed > 0
      ? calcLiveProjection(homeCorners + awayCorners, elapsed)
      : null;

    // Proyección de tarjetas
    const homeCards = liveStats
      ? ((Object.values(liveStats)[0]?.['Yellow Cards'] ?? 0) + (Object.values(liveStats)[0]?.['Red Cards'] ?? 0))
      : 0;
    const awayCards = liveStats
      ? ((Object.values(liveStats)[1]?.['Yellow Cards'] ?? 0) + (Object.values(liveStats)[1]?.['Red Cards'] ?? 0))
      : 0;
    const cardsProj = elapsed > 0
      ? calcLiveProjection(homeCards + awayCards, elapsed)
      : null;

    return {
      ...f,
      marcador: `${f.homeGoals ?? 0}-${f.awayGoals ?? 0}`,
      estadisticasVivo: liveStats,
      ...(momentum    && { momentumEnVivo: momentum }),
      ...(cornersProj && { proyeccionCorners: cornersProj }),
      ...(cardsProj   && { proyeccionTarjetas: cardsProj }),
    };
  });

  await bot.sendMessage(chatId, '🎯 Identificando picks de valor...');
  const analysis = await sonnet(
    INPLAY_SYSTEM,
    `DATOS REALES EN VIVO de API-Football:\n\n${JSON.stringify(enriched, null, 2)}\n\nAnaliza y da picks de valor in-play para los mejores partidos.`
  );
  await sendLong(chatId, `🔴 *PICKS EN VIVO${leagueName ? ' — ' + leagueName : ''}*\n\n${analysis}`, { parse_mode: 'Markdown' });
  recordPicks(analysis, enriched.map(f => ({ fixtureId: f.fixtureId, local: f.homeTeam, visitante: f.awayTeam, liga: f.leagueName, fechaPartido: f.date }))).catch(e => console.error('recordPicks:', e.message));
}

// ─── Alerta de Gol ────────────────────────────────────────────────────────────

async function handleAlertaGol(chatId) {
  await bot.sendMessage(chatId, '⚡ Escaneando partidos en vivo en busca de oportunidades de gol...');

  // 1. Obtener todos los partidos en vivo
  const liveRaw = await fetchLiveRaw();
  const liveActive = liveRaw.filter(f =>
    ['1H', '2H', 'ET'].includes(f.fixture.status.short) &&
    LEAGUE_IDS.has(f.league.id)
  );

  if (liveActive.length === 0) {
    return bot.sendMessage(chatId, '😔 No hay partidos activos ahora mismo en las ligas monitoreadas.');
  }

  await bot.sendMessage(chatId, `🔍 *${liveActive.length}* partido(s) activo(s). Calculando probabilidades de gol...`, { parse_mode: 'Markdown' });

  // 2. Obtener stats en vivo + históricas en paralelo (máx 6 partidos)
  const candidates = liveActive.slice(0, 6);
  const [liveStatsResults, homeStatsResults, awayStatsResults] = await Promise.all([
    Promise.allSettled(candidates.map(f => getFixtureStatistics(f.fixture.id))),
    Promise.allSettled(candidates.map(f => getTeamStats(f.teams.home.id, f.league.id))),
    Promise.allSettled(candidates.map(f => getTeamStats(f.teams.away.id, f.league.id))),
  ]);

  // 3. Calcular alerta de gol para cada partido
  const alerts = [];
  for (let i = 0; i < candidates.length; i++) {
    const f = candidates[i];
    const parsed = parseFixture(f);
    const liveStats  = liveStatsResults[i].status  === 'fulfilled' ? liveStatsResults[i].value  : null;
    const homeStats  = homeStatsResults[i].status  === 'fulfilled' ? homeStatsResults[i].value  : null;
    const awayStats  = awayStatsResults[i].status  === 'fulfilled' ? awayStatsResults[i].value  : null;

    const alert = calcGoalAlert(parsed, liveStats, homeStats, awayStats);
    if (alert && alert.impliedOdds >= 1.45) alerts.push(alert);
  }

  if (alerts.length === 0) {
    return bot.sendMessage(
      chatId,
      '⛔ No hay partidos en vivo con probabilidad de gol suficiente para recomendar (cuota < 1.45 o partidos finalizando).\n\nInténtalo más tarde.'
    );
  }

  // 4. Ordenar por alertScore desc y tomar top 3
  const top = alerts
    .sort((a, b) => b.alertScore - a.alertScore)
    .slice(0, 3);

  console.log(`⚡ ${top.length} alertas de gol generadas:`, top.map(a => `${a.local} vs ${a.visitante} (${a.pGoal}% @ ~${a.impliedOdds})`).join(', '));

  // 5. Claude formatea las alertas (Haiku = rápido y barato para tiempo real)
  const analysis = await haiku(
    ALERTA_GOL_SYSTEM,
    `Alertas de gol calculadas matemáticamente:\n\n${JSON.stringify(top, null, 2)}\n\nFormatea las ${top.length} alerta(s) usando el formato obligatorio. Ordénalas de mayor a menor alertScore.`
  );

  await sendLong(chatId, `⚡ *ALERTAS DE GOL EN VIVO*\n\n${analysis}`, { parse_mode: 'Markdown' });
}

async function handleEspecifica(chatId, intent) {
  const { equipo, pregunta_especifica, mercado, tiempo, contexto } = intent;

  if (!equipo) {
    return bot.sendMessage(chatId, '¿De qué equipo te interesa la estadística o análisis?');
  }

  await bot.sendMessage(chatId, `🔍 Analizando tu pregunta...`);

  const teamData = await searchTeam(equipo, intent.liga || '');
  if (!teamData) {
    return bot.sendMessage(chatId, `❌ No encontré el equipo "${equipo}" en nuestra base de datos.`);
  }

  const teamId   = teamData.team.id;
  const teamFull = teamData.team.name;

  const nextRaw = await findNextFixtureByDate(teamId, 14);
  if (!nextRaw) {
    return bot.sendMessage(chatId, `😔 No encontré próximos partidos para *${teamFull}* en los próximos 14 días.`, { parse_mode: 'Markdown' });
  }

  const homeId   = nextRaw.teams.home.id;
  const awayId   = nextRaw.teams.away.id;
  const leagueId = nextRaw.league.id;
  const homeTeam = nextRaw.teams.home.name;
  const awayTeam = nextRaw.teams.away.name;
  const isHome   = homeId === teamId;

  await bot.sendMessage(chatId, `📊 Recopilando datos históricos...`);

  const [h2hRes, homeStatsRes, awayStatsRes] = await Promise.allSettled([
    getH2H(homeId, awayId),
    getTeamStats(homeId, leagueId),
    getTeamStats(awayId, leagueId),
  ]);

  const h2hData2       = h2hRes.status === 'fulfilled' ? h2hRes.value : [];
  const homeStatsData2 = homeStatsRes.status === 'fulfilled' ? homeStatsRes.value : null;
  const awayStatsData2 = awayStatsRes.status === 'fulfilled' ? awayStatsRes.value : null;
  const probBlock2     = buildProbBlock(homeStatsData2, awayStatsData2, h2hData2);

  const analysisData = {
    partido: {
      liga:      nextRaw.league.name,
      fecha:     nextRaw.fixture.date.split('T')[0],
      hora:      formatHour(nextRaw.fixture.date),
      local:     homeTeam,
      visitante: awayTeam,
    },
    equipoConsultado: teamFull,
    rolEnPartido: isHome ? 'LOCAL' : 'VISITANTE',
    h2h:          h2hData2,
    bttsEnH2H:    h2hData2.filter(m => m.btts).length,
    statsLocal:   homeStatsData2,
    statsVisitante: awayStatsData2,
    ...(probBlock2 && { probabilidadesCalculadas: probBlock2 }),
  };

  await bot.sendMessage(chatId, '⚡ Calculando probabilidad específica...');

  const specificPrompt = `El usuario pregunta ESPECÍFICAMENTE: "${pregunta_especifica}"
Mercado de interés: ${mercado || 'general'}
Tiempo: ${tiempo || 'FT'}
Contexto: ${contexto || 'proximo_partido'}
Equipo consultado: ${teamFull} (juega como ${isHome ? 'LOCAL' : 'VISITANTE'})

DATOS REALES:
${JSON.stringify(analysisData, null, 2)}

INSTRUCCIONES ESTRICTAS:
- Responde EXACTAMENTE lo que pregunta el usuario. NO hagas análisis completo si no lo pidió.
- Usa las probabilidadesCalculadas como base principal. El modelo Poisson ya calculó las probs matemáticamente.
- Si pregunta por BTTS: usa probBTTS_Combinada (Poisson + H2H). Muestra la fuente.
- Si pregunta por Over/Under goles: usa probOver25, probOver35, xGLocal, xGVisitante.
- Si pregunta por resultado: usa probLocalGana, probEmpate, probVisitanteGana, probDNB_Local/Visitante.
- Si pregunta por corners o tarjetas: usa promedios de stats históricos disponibles.
- Si el EV es negativo en el mercado preguntado, menciónalo ("la cuota de mercado no ofrece valor").
- Muestra los datos históricos relevantes para ESA pregunta concreta.
- Sé directo. Máximo 250 palabras.`;

  const analysis = await sonnet(TIPSTER_SYSTEM, specificPrompt);
  await sendLong(chatId, `🎯 *${teamFull}* — Consulta específica\n\n${analysis}`, { parse_mode: 'Markdown' });
  recordPicks(analysis, [{ fixtureId: nextRaw.fixture.id, local: homeTeam, visitante: awayTeam, liga: nextRaw.league.name, fechaPartido: nextRaw.fixture.date }]).catch(() => {});
}

// ─── Rachas ───────────────────────────────────────────────────────────────────

function calcTeamStreaks(fixtures, teamId) {
  const sorted = [...fixtures].sort((a, b) => new Date(a.date) - new Date(b.date));
  const matchStats = sorted.map(f => {
    const isHome   = f.homeId === teamId;
    const scored   = isHome ? f.goalsHome : f.goalsAway;
    const conceded = isHome ? f.goalsAway : f.goalsHome;
    const htScored   = isHome ? f.htHome : f.htAway;
    const htConceded = isHome ? f.htAway : f.htHome;
    return { scored, conceded, total: scored + conceded, htScored, htConceded };
  });

  const categories = [
    { key: 'marcó',      label: 'Marcó al menos 1 gol',       fn: m => m.scored > 0 },
    { key: 'no_marcó',   label: 'Sin marcar',                  fn: m => m.scored === 0 },
    { key: 'portería0',  label: 'Portería a cero',             fn: m => m.conceded === 0 },
    { key: 'ganó',       label: 'Victoria',                    fn: m => m.scored > m.conceded },
    { key: 'empató',     label: 'Empate',                      fn: m => m.scored === m.conceded },
    { key: 'perdió',     label: 'Derrota',                     fn: m => m.scored < m.conceded },
    { key: 'btts',       label: 'Ambos marcan (BTTS)',         fn: m => m.scored > 0 && m.conceded > 0 },
    { key: 'over15',     label: 'Over 1.5 goles totales',      fn: m => m.total >= 2 },
    { key: 'over25',     label: 'Over 2.5 goles totales',      fn: m => m.total >= 3 },
    { key: 'over35',     label: 'Over 3.5 goles totales',      fn: m => m.total >= 4 },
    { key: 'marcó1T',    label: 'Marcó en el 1er tiempo',      fn: m => m.htScored != null && m.htScored > 0 },
    { key: 'marcó2T',    label: 'Marcó en el 2do tiempo',      fn: m => m.htScored != null && (m.scored - m.htScored) > 0 },
    { key: 'recibió1T',  label: 'Recibió gol en el 1er tiempo',fn: m => m.htConceded != null && m.htConceded > 0 },
    { key: 'sinRecibir1T',label: 'Sin recibir en el 1er tiempo',fn: m => m.htConceded != null && m.htConceded === 0 },
  ];

  const streaks = {};
  for (const cat of categories) {
    let current = 0;
    for (const m of matchStats) {
      if (cat.fn(m)) current++;
      else current = 0;
    }
    streaks[cat.key] = { current, label: cat.label };
  }
  return streaks;
}

function calcCornerCardStreaks(fixtures, statsMap) {
  const sorted = [...fixtures].sort((a, b) => new Date(a.date) - new Date(b.date));
  const thresholds = [
    { key: 'corners8',  label: 'Partido con 8+ córners totales',   type: 'corners', min: 8 },
    { key: 'corners9',  label: 'Partido con 9+ córners totales',   type: 'corners', min: 9 },
    { key: 'corners10', label: 'Partido con 10+ córners totales',  type: 'corners', min: 10 },
    { key: 'corners11', label: 'Partido con 11+ córners totales',  type: 'corners', min: 11 },
    { key: 'cards3',    label: 'Partido con 3+ tarjetas totales',  type: 'cards',   min: 3 },
    { key: 'cards4',    label: 'Partido con 4+ tarjetas totales',  type: 'cards',   min: 4 },
  ];

  const streaks = {};
  for (const t of thresholds) {
    let current = 0;
    for (const f of sorted) {
      const stats = statsMap[f.fixtureId];
      if (!stats) { current = 0; continue; }
      let value = 0;
      for (const ts of Object.values(stats)) {
        if (t.type === 'corners') value += ts['Corner Kicks'] || 0;
        else value += (ts['Yellow Cards'] || 0) + (ts['Red Cards'] || 0);
      }
      if (value >= t.min) current++;
      else current = 0;
    }
    streaks[t.key] = { current, label: t.label };
  }
  return streaks;
}

async function handleRachas(chatId, intent) {
  const venueRaw = intent.venue || 'all';
  const venueParam = venueRaw === 'home' ? 'home' : venueRaw === 'away' ? 'away' : null;
  const venueLabel = { home: 'en casa', away: 'de visita', all: 'en total' }[venueRaw] || 'en total';
  const MIN_STREAK = 5;

  // ── Modo equipo específico ──────────────────────────────────────────────────
  if (intent.equipo) {
    await bot.sendMessage(chatId, `🔍 Buscando rachas de *${intent.equipo}*...`, { parse_mode: 'Markdown' });

    const teamData = await searchTeam(intent.equipo, intent.liga || '');
    if (!teamData) return bot.sendMessage(chatId, `😔 No encontré el equipo *${intent.equipo}*.`, { parse_mode: 'Markdown' });

    const teamId   = teamData.team.id;
    const teamName = teamData.team.name;

    const fixtures = await getTeamLastFixtures(teamId, 20, venueParam);
    if (fixtures.length < 3) return bot.sendMessage(chatId, `😔 No hay suficientes partidos recientes para *${teamName}*.`, { parse_mode: 'Markdown' });

    const baseStreaks = calcTeamStreaks(fixtures, teamId);

    // Corners/tarjetas — últimos 10 partidos con estadísticas
    const last10 = fixtures.slice(-10);
    await bot.sendMessage(chatId, `📊 Consultando estadísticas de córners y tarjetas...`);
    const statsMap = {};
    for (const f of last10) {
      const s = await getFixtureStatistics(f.fixtureId);
      if (s) statsMap[f.fixtureId] = s;
    }
    const extraStreaks = calcCornerCardStreaks(last10, statsMap);

    const allStreaks = { ...baseStreaks, ...extraStreaks };
    const active = Object.entries(allStreaks)
      .filter(([, v]) => v.current >= MIN_STREAK)
      .sort(([, a], [, b]) => b.current - a.current);

    if (active.length === 0) {
      const top3 = Object.entries(allStreaks)
        .sort(([, a], [, b]) => b.current - a.current)
        .slice(0, 3)
        .map(([, v]) => `${v.label}: ${v.current}`).join(', ');
      return bot.sendMessage(chatId,
        `📊 *${teamName}* no tiene rachas activas de ${MIN_STREAK}+ partidos ${venueLabel}.\n\nMejores rachas actuales: ${top3}`,
        { parse_mode: 'Markdown' }
      );
    }

    let text = `🔥 *RACHAS ACTIVAS — ${teamName}*\n`;
    text += `📍 ${venueLabel.charAt(0).toUpperCase() + venueLabel.slice(1)} | Últimos ${fixtures.length} partidos\n`;
    text += `━━━━━━━━━━━━━━━━━━━\n\n`;
    for (const [, val] of active) {
      text += `✅ *${val.label}*\n   ↳ ${val.current} partidos consecutivos\n\n`;
    }
    text += `━━━━━━━━━━━━━━━━━━━\n`;
    text += `⚠️ Solo muestra rachas de ${MIN_STREAK}+ partidos`;
    return sendLong(chatId, text, { parse_mode: 'Markdown' });
  }

  // ── Modo liga ──────────────────────────────────────────────────────────────
  const leagueId = intent.liga ? findLeagueId(intent.liga) : null;
  if (!leagueId) {
    return bot.sendMessage(chatId,
      '¿De qué equipo o liga quieres ver las rachas?\n\nEjemplos:\n• *rachas Real Madrid*\n• *rachas Premier League*\n• *rachas en casa Serie A*\n• *rachas de visita Bundesliga*',
      { parse_mode: 'Markdown' }
    );
  }

  const leagueName = LEAGUE_MAP[leagueId]?.name || intent.liga;
  await bot.sendMessage(chatId, `🔍 Analizando rachas en *${leagueName}* ${venueLabel}...`, { parse_mode: 'Markdown' });

  const teams = await getLeagueStandings(leagueId);
  if (teams.length === 0) return bot.sendMessage(chatId, `😔 No encontré la tabla de *${leagueName}*.`, { parse_mode: 'Markdown' });

  const topTeams = teams.slice(0, 16);
  await bot.sendMessage(chatId, `📊 Procesando ${topTeams.length} equipos...`);

  const teamsWithStreaks = [];
  for (let i = 0; i < topTeams.length; i += 4) {
    const batch = topTeams.slice(i, i + 4);
    const results = await Promise.all(batch.map(async t => {
      const fixtures = await getTeamLastFixtures(t.teamId, 12, venueParam);
      const streaks  = calcTeamStreaks(fixtures, t.teamId);
      return { ...t, streaks };
    }));
    teamsWithStreaks.push(...results);
  }

  // Agrupar por categoría
  const categoryMap = {};
  for (const team of teamsWithStreaks) {
    for (const [key, val] of Object.entries(team.streaks)) {
      if (val.current >= MIN_STREAK) {
        if (!categoryMap[key]) categoryMap[key] = { label: val.label, teams: [] };
        categoryMap[key].teams.push({ name: team.teamName, streak: val.current });
      }
    }
  }

  const sorted = Object.entries(categoryMap).sort(([, a], [, b]) => b.teams.length - a.teams.length);

  if (sorted.length === 0) {
    return bot.sendMessage(chatId,
      `📊 No hay equipos con rachas de ${MIN_STREAK}+ partidos en *${leagueName}* ${venueLabel}.`,
      { parse_mode: 'Markdown' }
    );
  }

  let text = `🔥 *RACHAS ${leagueName.toUpperCase()}*\n`;
  text += `📍 ${venueLabel.charAt(0).toUpperCase() + venueLabel.slice(1)} | Mín. ${MIN_STREAK} partidos consecutivos\n`;
  text += `━━━━━━━━━━━━━━━━━━━\n\n`;
  for (const [, cat] of sorted) {
    cat.teams.sort((a, b) => b.streak - a.streak);
    text += `📌 *${cat.label}*\n`;
    for (const t of cat.teams) text += `   ▸ ${t.name}: *${t.streak}* partidos\n`;
    text += '\n';
  }
  text += `━━━━━━━━━━━━━━━━━━━\n`;
  text += `📊 Basado en últimos 12 partidos ${venueLabel} por equipo`;
  return sendLong(chatId, text, { parse_mode: 'Markdown' });
}

async function handleChatGeneral(chatId, pregunta) {
  const CHAT_SYSTEM = `Eres TipsterAI Master PRO, el mejor asistente de apuestas deportivas.
Responde en español. Sé amigable, conciso y profesional.
No menciones tecnologías, APIs ni plataformas.
Si el usuario saluda, saluda de vuelta y menciona brevemente qué puedes hacer.
Si hace una pregunta general de fútbol o apuestas, responde con conocimiento experto.
Recuérdales que pueden pedir: picks del día, analizar un equipo, partidos en vivo, enviar imagen de un partido, o ver planes.
FORMATO: usa *texto* para negritas (un solo asterisco, estilo Telegram). Nunca uses **doble asterisco**.`;

  const response = await haiku(CHAT_SYSTEM, pregunta);
  await bot.sendMessage(chatId, normalizeMd(response), { parse_mode: 'Markdown' });
}

async function handleVerPlanes(chatId) {
  await bot.sendMessage(chatId,
    `🏆 *PLANES TIPSTERAI MASTER PRO*\n\n` +
    `━━━━━━━━━━━━━━━━━━━\n` +
    `🆓 *FREEMIUM* — Prueba gratuita\n` +
    `▸ 1 consulta gratis al día\n` +
    `▸ Válido por 3 días\n\n` +
    `⚡ *VIP*\n` +
    `▸ 10 consultas diarias\n` +
    `▸ Análisis completo de partidos y picks\n\n` +
    `🏆 *PRO*\n` +
    `▸ 50 consultas diarias\n` +
    `▸ Análisis de imágenes en vivo\n` +
    `▸ Acceso completo a todas las funciones\n` +
    `━━━━━━━━━━━━━━━━━━━\n\n` +
    `🔗 [Suscríbete aquí](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)`,
    { parse_mode: 'Markdown' }
  );
}

// ─── Vision (image analysis) ──────────────────────────────────────────────────

const VISION_EXTRACT_PROMPT = `Eres un experto en estadísticas de fútbol. Analiza esta imagen de un partido en vivo y extrae TODOS los datos visibles en formato JSON:
{"home_team":"string","away_team":"string","score_home":number,"score_away":number,"minute":number,"half":"1T o 2T o HT o ET","stats":{"possession_home":number o null,"possession_away":number o null,"shots_home":number o null,"shots_away":number o null,"shots_on_target_home":number o null,"shots_on_target_away":number o null,"corners_home":number o null,"corners_away":number o null,"fouls_home":number o null,"fouls_away":number o null,"yellow_cards_home":number o null,"yellow_cards_away":number o null,"red_cards_home":number o null,"red_cards_away":number o null,"dangerous_attacks_home":number o null,"dangerous_attacks_away":number o null,"xg_home":number o null,"xg_away":number o null}}
Si algún dato no es visible usa null. Responde SOLO el JSON.`;

async function handleImage(msg) {
  const chatId = msg.chat.id;
  try {
    await bot.sendMessage(chatId, '📸 Imagen recibida. Procesando estadísticas del partido...');

    const photos  = msg.photo;
    const fileId  = photos[photos.length - 1].file_id;
    const fileObj = await bot.getFile(fileId);
    const fileUrl = `https://api.telegram.org/file/bot${process.env.TELEGRAM_TOKEN}/${fileObj.file_path}`;

    const imgResp = await axios.get(fileUrl, { responseType: 'arraybuffer', timeout: 20000 });
    const base64  = Buffer.from(imgResp.data).toString('base64');
    const rawMime = imgResp.headers['content-type'] || '';
    let mime = 'image/jpeg';
    if (rawMime.includes('png')) mime = 'image/png';
    else if (rawMime.includes('gif')) mime = 'image/gif';
    else if (rawMime.includes('webp')) mime = 'image/webp';

    console.log(`🔍 Claude Vision: analizando imagen (${mime})`);
    const visionMsg = await claudeWithRetry({
      model: 'claude-opus-4-6',
      max_tokens: 1000,
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: mime, data: base64 } },
          { type: 'text',  text: VISION_EXTRACT_PROMPT },
        ],
      }],
    });

    let matchData = null;
    try {
      const jsonMatch = visionMsg.content[0].text.match(/\{[\s\S]*\}/);
      matchData = jsonMatch ? JSON.parse(jsonMatch[0]) : null;
    } catch { matchData = null; }

    if (!matchData?.home_team) {
      return sendLong(chatId, '❌ No pude leer estadísticas de fútbol en la imagen. Envía una captura de Flashscore, Sofascore u otra app de estadísticas.');
    }

    console.log(`📊 Vision extrajo: ${matchData.home_team} ${matchData.score_home}-${matchData.score_away} ${matchData.away_team} min${matchData.minute}`);
    await bot.sendMessage(chatId,
      `✅ Partido identificado: *${matchData.home_team}* ${matchData.score_home}-${matchData.score_away} *${matchData.away_team}* | Min ${matchData.minute ?? '?'}\n\n📊 Consultando historial de enfrentamientos...`,
      { parse_mode: 'Markdown' }
    );

    // Search API for H2H + stats
    let apiContext = null;
    try {
      const [homeRes, awayRes] = await Promise.allSettled([
        searchTeam(matchData.home_team),
        searchTeam(matchData.away_team),
      ]);
      const homeTeam = homeRes.status === 'fulfilled' ? homeRes.value : null;
      const awayTeam = awayRes.status === 'fulfilled' ? awayRes.value : null;

      if (homeTeam && awayTeam) {
        const homeId = homeTeam.team.id;
        const awayId = awayTeam.team.id;
        const [h2hRes, h2hRawRes] = await Promise.allSettled([
          getH2H(homeId, awayId),
          API.get('/fixtures/headtohead', { params: { h2h: `${homeId}-${awayId}`, last: 1 } }),
        ]);
        const h2h = h2hRes.status === 'fulfilled' ? h2hRes.value : [];
        let statsHome = null, statsAway = null;
        if (h2hRawRes.status === 'fulfilled') {
          const lastMatch = h2hRawRes.value.data.response?.[0];
          if (lastMatch) {
            const lid = lastMatch.league.id;
            const [sh, sa] = await Promise.allSettled([getTeamStats(homeId, lid), getTeamStats(awayId, lid)]);
            statsHome = sh.status === 'fulfilled' ? sh.value : null;
            statsAway = sa.status === 'fulfilled' ? sa.value : null;
          }
        }
        apiContext = { h2h, bttsEnH2H: h2h.filter(m => m.btts).length, statsLocal: statsHome, statsVisitante: statsAway };
        console.log(`📊 API encontró H2H: ${h2h.length} partidos`);
      }
    } catch (e) { console.log('API lookup falló:', e.message); }

    const contextNote = apiContext
      ? 'Datos históricos de API-Football disponibles.'
      : 'No se encontraron en API-Football. Analiza SOLO con datos de la imagen. Indica "Análisis basado solo en estadísticas visibles".';

    await bot.sendMessage(chatId, '⚡ Generando análisis in-play...');
    const analysis = await sonnet(
      INPLAY_SYSTEM,
      `DATOS DE LA IMAGEN (fuente principal):\n${JSON.stringify(matchData, null, 2)}\n\nDATOS HISTÓRICOS API:\n${apiContext ? JSON.stringify(apiContext, null, 2) : 'No disponibles'}\n\nNOTA: ${contextNote}`
    );
    await sendLong(chatId, analysis, { parse_mode: 'Markdown' });
    recordPicks(analysis, [{ fixtureId: null, local: matchData.home_team, visitante: matchData.away_team, liga: 'En vivo (imagen)', fechaPartido: new Date().toISOString() }]).catch(e => console.error('recordPicks:', e.message));

  } catch (err) {
    console.error('handleImage error:', err.message);
    await bot.sendMessage(chatId, `❌ Error al analizar imagen: ${err.message}`);
  }
}

// ─── Access control ───────────────────────────────────────────────────────────

function todayBogota() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
}

async function getAirtableUser(telegramId) {
  try {
    const base = getAirtableBase();
    const records = await base(AIRTABLE_TABLE)
      .select({ filterByFormula: `{telegram_id} = "${telegramId}"`, maxRecords: 1 })
      .firstPage();
    return records[0] || null;
  } catch (e) {
    console.error('getAirtableUser error:', e.message);
    return null;
  }
}

async function registerUser(telegramId, username) {
  const today = todayBogota();
  const trialExpira = new Date();
  trialExpira.setDate(trialExpira.getDate() + 3);
  const trialStr = trialExpira.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
  try {
    await upsertAirtableUser(telegramId, {
      plan:           'free',
      consultas_hoy:  0,
      fecha_registro: today,
      trial_expira:   trialStr,
      ultimo_reset:   today,
    });
    console.log(`👤 Usuario registrado: telegram_id=${telegramId} plan=free trial_expira=${trialStr}`);
  } catch (e) {
    console.error('registerUser error:', e.message);
  }
}

async function checkAndResetIfNeeded(record) {
  const today = todayBogota();
  if (record.fields.ultimo_reset !== today) {
    try {
      const base = getAirtableBase();
      await base(AIRTABLE_TABLE).update(record.id, { consultas_hoy: 0, ultimo_reset: today });
      record.fields.consultas_hoy = 0;
      record.fields.ultimo_reset  = today;
    } catch (e) {
      console.error('resetDaily error:', e.message);
    }
  }
  return record;
}

const ADMIN_IDS = new Set(['1079416271']);

async function checkAccess(chatId, telegramId, isImage = false) {
  console.log('CHECK ACCESS - telegramId:', telegramId);

  // Superadmin: acceso ilimitado siempre
  if (ADMIN_IDS.has(String(telegramId))) {
    console.log('CHECK ACCESS - superadmin, acceso ilimitado');
    return { allowed: true, plan: 'pro' };
  }

  let record = await getAirtableUser(telegramId);
  if (!record) {
    // No registrado — registrar como free y aplicar límites normalmente
    console.log('CHECK ACCESS - usuario no existe, registrando como free...');
    await registerUser(telegramId, String(telegramId));
    record = await getAirtableUser(telegramId);
    if (!record) {
      // Si Airtable falla al crear, bloquear por seguridad
      console.error('CHECK ACCESS - no se pudo registrar usuario, bloqueando');
      return { allowed: false };
    }
  }

  record = await checkAndResetIfNeeded(record);

  const plan       = record.fields.plan || 'free';
  const planConfig = PLANES[plan] || PLANES.free;
  const consultasHoy = Number(record.fields.consultas_hoy) || 0;
  const today      = todayBogota();

  console.log('CHECK ACCESS - plan:', plan);
  console.log('CHECK ACCESS - consultas_hoy:', consultasHoy, '/ limite:', planConfig.consultas_diarias);
  console.log('CHECK ACCESS - trial_expira:', record.fields.trial_expira, '| hoy:', today);

  // Imagen: solo PRO
  if (isImage && !planConfig.puede_imagen) {
    await bot.sendMessage(chatId,
      `📸 El análisis de imágenes en vivo está disponible solo en el plan *PRO*.\n\n🔗 [Ver planes y suscribirte](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)`,
      { parse_mode: 'Markdown' }
    );
    return { allowed: false };
  }

  // Free: verificar período de prueba
  if (plan === 'free' && record.fields.trial_expira) {
    if (today > record.fields.trial_expira) {
      await bot.sendMessage(chatId,
        `Tu período de prueba gratuito ha terminado 🏁\n\n` +
        `Esperamos que hayas disfrutado TipsterAI.\n` +
        `Para continuar con análisis profesionales:\n\n` +
        `⚡ *VIP*: 10 consultas/día\n` +
        `🏆 *PRO*: 50 consultas/día + análisis de imágenes en vivo\n\n` +
        `🔗 [Ver planes y suscribirte](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)`,
        { parse_mode: 'Markdown' }
      );
      return { allowed: false };
    }
  }

  // VIP / PRO: verificar que la suscripción de 30 días no haya expirado
  if ((plan === 'vip' || plan === 'pro') && record.fields.expires_at) {
    if (today > record.fields.expires_at) {
      // Degradar a free automáticamente
      try {
        const base = getAirtableBase();
        await base(AIRTABLE_TABLE).update(record.id, { plan: 'free' });
      } catch (e) { console.error('downgrade error:', e.message); }
      await bot.sendMessage(chatId,
        `⚠️ *Tu suscripción ${plan.toUpperCase()} ha expirado.*\n\n` +
        `Tu acceso ha cambiado al plan gratuito.\n\n` +
        `Para renovar:\n` +
        `⚡ *VIP*: 10 consultas/día\n` +
        `🏆 *PRO*: 50 consultas/día + análisis de imágenes\n\n` +
        `Escribe *"ver planes"* para más información.`,
        { parse_mode: 'Markdown' }
      );
      return { allowed: false };
    }
  }

  // Verificar límite diario
  if (consultasHoy >= planConfig.consultas_diarias) {
    let msg;
    if (plan === 'free') {
      msg =
        `Has usado tu consulta gratuita de hoy 🎯\n\n` +
        `Vuelve mañana por tu consulta diaria gratis, o accede a más análisis:\n\n` +
        `⚡ *VIP*: 10 consultas/día\n` +
        `🏆 *PRO*: 50 consultas/día + análisis de imágenes\n\n` +
        `🔗 [Ver planes y suscribirte](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)`;
    } else if (plan === 'vip') {
      msg =
        `Has alcanzado tus 10 consultas de hoy ⚡\n\n` +
        `Tus consultas se renuevan a medianoche.\n` +
        `¿Quieres más? Upgrade a PRO:\n\n` +
        `🏆 *PRO*: 50 consultas/día + análisis de imágenes en vivo\n\n` +
        `🔗 [Upgrade a PRO aquí](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)`;
    } else {
      msg = `Has alcanzado tus 50 consultas de hoy.\nTus consultas se renuevan a medianoche. ⏰`;
    }
    console.log('CHECK ACCESS - resultado: bloqueado por límite diario');
    await bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
    return { allowed: false };
  }

  console.log('CHECK ACCESS - resultado: allowed');
  return { allowed: true, plan, recordId: record.id };
}

async function incrementConsultas(telegramId) {
  try {
    const record = await getAirtableUser(telegramId);
    if (!record) return;
    const base = getAirtableBase();
    const current = Number(record.fields.consultas_hoy) || 0;
    await base(AIRTABLE_TABLE).update(record.id, { consultas_hoy: current + 1 });
  } catch (e) {
    console.error('incrementConsultas error:', e.message);
  }
}

// ─── Command: /start ──────────────────────────────────────────────────────────

bot.onText(/\/start/, async (msg) => {
  const chatId = msg.chat.id;
  const telegramId = String(msg.from.id);
  const username = msg.from.username || msg.from.first_name || '';

  // Register user if not already registered
  const existing = await getAirtableUser(telegramId).catch(() => null);
  if (!existing) {
    registerUser(telegramId, username).catch(e => console.error('register on start:', e.message));
  }

  bot.sendMessage(chatId, `⚽ *TipsterAi Master PRO — Servicio Exclusivo de Apuestas*

Bienvenido. Tengo acceso a estadísticas en tiempo real, historial de enfrentamientos y datos exclusivos de las principales ligas del mundo.

Dime lo que necesitas en lenguaje natural:

• *"picks de hoy"* → mis mejores picks del día con análisis completo
• *"bundesliga en vivo"* → partidos en curso con picks in-play
• *"analiza al Real Madrid"* → análisis táctico con historial y estadísticas
• *"que hay en vivo"* → todos los partidos en curso ahora mismo
• 📸 Envía una captura de un partido → análisis in-play inmediato

━━━━━━━━━━━━━━━━━━━
⚠️ Solo emito picks con STAKE 6+/10 y cuota mínima 1.50
🎯 Apuesta siempre con responsabilidad.`, { parse_mode: 'Markdown' });
});

// ─── Main message handler ─────────────────────────────────────────────────────

bot.on('message', async (msg) => {
  const chatId    = msg.chat.id;
  const telegramId = String(msg.from?.id || chatId);

  // Handle images
  if (msg.photo) {
    const access = await checkAccess(chatId, telegramId, true);
    if (!access.allowed) return;
    await handleImage(msg);
    incrementConsultas(telegramId).catch(() => {});
    return;
  }

  if (!msg.text || msg.text.startsWith('/')) return;

  const text = msg.text.trim();

  try {
    // ── Pre-detección por keywords (más rápida y fiable que el LLM) ──────────
    // Corre ANTES de detectIntent para comandos específicos que el LLM confunde
    function preDetectIntent(t) {
      const q = t.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');

      // Alerta de gol — debe ir ANTES de en_vivo para capturar "gol en vivo"
      if (/alerta.{0,6}gol|gol.{0,10}(en\s*vivo|vivo|live|ahora|ahora mismo)|probabilidad.{0,6}gol|donde.{0,10}gol|partido.{0,10}gol|next.{0,4}goal/.test(q)) {
        return { intencion: 'alerta_gol', pregunta_especifica: t };
      }

      // Picks del día general
      if (/^(picks?|apuestas?)\s*(de\s*)?(hoy|del\s*dia|para\s*hoy)/.test(q) || q === 'picks' || q === 'picks hoy') {
        return { intencion: 'picks_hoy', pregunta_especifica: t };
      }

      // Rachas
      if (/\brachas?\b/.test(q)) return null; // dejar al LLM (tiene contexto de liga/equipo)

      // En vivo general (sin "gol")
      if (/\ben\s*vivo\b|\blive\b|\bque\s*hay\s*(en\s*vivo|ahora)\b/.test(q) && !/gol/.test(q)) {
        return null; // dejar al LLM para capturar filtro de liga
      }

      return null; // sin pre-detección → usar detectIntent normalmente
    }

    const preDetected = preDetectIntent(text);
    const intent = preDetected || await detectIntent(text);
    const intencion = intent.intencion || intent.intent || 'chat_general';
    console.log(`[${new Date().toISOString()}] "${text}" → ${JSON.stringify(intent)}`);

    // ver_planes nunca consume cuota ni requiere acceso
    if (intencion === 'ver_planes') {
      return handleVerPlanes(chatId);
    }

    // Verificar acceso para TODOS los demás intents (incluyendo chat_general)
    // chat_general pasa el check pero NO consume cuota
    const access = await checkAccess(chatId, telegramId, false);
    if (!access.allowed) return;

    // chat_general: acceso verificado pero sin consumir cuota
    if (intencion === 'chat_general') {
      return handleChatGeneral(chatId, intent.pregunta_especifica || text);
    }

    // Detectar si el usuario quiere forzar un nuevo análisis
    const forceRefresh = /\b(actualizar|refresh|forzar|nuevo|recalcul|regenera)\b/i.test(text);

    switch (intencion) {
      case 'picks_hoy':
        await handlePicksHoy(chatId, forceRefresh);
        break;
      case 'picks_liga':
        await handlePicksLiga(chatId, intent.liga || text, forceRefresh);
        break;
      case 'alerta_gol':
        await handleAlertaGol(chatId);
        break;
      case 'partido_especifico':
        if (!intent.equipo) {
          await bot.sendMessage(chatId, '¿De qué equipo quieres el análisis?');
        } else if (intent.mercado || intent.tiempo) {
          // Specific market question
          await handleEspecifica(chatId, intent);
        } else {
          // Full match analysis
          await handlePartido(chatId, intent.equipo, intent.liga || '');
        }
        break;
      // Legacy compatibility
      case 'partido':
        if (!intent.team && !intent.equipo) {
          await bot.sendMessage(chatId, '¿De qué equipo quieres el análisis?');
        } else {
          await handlePartido(chatId, intent.team || intent.equipo, intent.country || intent.liga || '');
        }
        break;
      case 'en_vivo': {
        const lid = intent.liga ? findLeagueId(intent.liga) : null;
        const lname = lid ? (LEAGUE_MAP[lid]?.name || intent.liga) : intent.liga || null;
        await handleVivo(chatId, lid, lname);
        break;
      }
      // Legacy
      case 'vivo':
        await handleVivo(chatId);
        break;
      case 'vivo_liga': {
        const lid = findLeagueId(intent.league || '');
        const lname = lid ? (LEAGUE_MAP[lid]?.name || intent.league) : intent.league;
        await handleVivo(chatId, lid, lname);
        break;
      }
      case 'estadisticas':
        await handleEstadisticas(chatId, intent.period || 'hoy');
        break;
      case 'rachas':
        await handleRachas(chatId, intent);
        break;
      default:
        await handleChatGeneral(chatId, intent.pregunta_especifica || text);
        return; // don't count chat as quota
    }

    // Increment quota after successful handling
    incrementConsultas(telegramId).catch(() => {});

  } catch (err) {
    console.error('Handler error:', err.message);
    const userMsg = isOverloadedError(err)
      ? '⏳ La IA está saturada. Intenta de nuevo en unos segundos.'
      : `❌ Error: ${err.message}`;
    await bot.sendMessage(chatId, userMsg);
  }
});

// ─── Startup ──────────────────────────────────────────────────────────────────

bot.getMe().then(me => {
  console.log(`✅ @${me.username} conectado`);
}).catch(err => {
  console.error('❌ Error de conexión:', err.message);
  process.exit(1);
});

// Salida limpia — PM2 reinicia automáticamente
function exitBot(reason) {
  console.error(`💥 Reiniciando proceso: ${reason}`);
  process.exit(1);
}

// Cualquier error de polling → reinicio inmediato
bot.on('polling_error', err => exitBot(`polling_error: ${err.message}`));

// Errores no capturados → reinicio
process.on('uncaughtException',  err => exitBot(`uncaughtException: ${err.message}`));
process.on('unhandledRejection', err => exitBot(`unhandledRejection: ${err}`));

// Watchdog: cada 90 segundos verifica que el polling siga vivo
// Lógica: si getMe() falla UNA vez → reinicio inmediato (no esperar 3)
// Si getMe() funciona pero el polling lleva mucho tiempo sin recibir nada → reinicio
let lastUpdateReceived = Date.now();
bot.on('message',        () => { lastUpdateReceived = Date.now(); });
bot.on('callback_query', () => { lastUpdateReceived = Date.now(); });

setInterval(async () => {
  try {
    await bot.getMe();
    const minutesSilent = (Date.now() - lastUpdateReceived) / 60000;
    // Si llevamos más de 20 min sin ningún update Y getMe empieza a tener
    // fallos intermitentes, algo está mal. Pero getMe OK = polling OK aquí
    // solo logueamos para diagnóstico.
    console.log(`💓 OK ${new Date().toISOString()} | silencio: ${minutesSilent.toFixed(1)}min`);
  } catch (err) {
    // getMe falló → red rota → reinicio inmediato sin esperar contador
    exitBot(`keepalive getMe falló: ${err.message}`);
  }
}, 90 * 1000);


// ─── Whop Webhook Server ───────────────────────────────────────────────────────

const AIRTABLE_TABLE = process.env.AIRTABLE_TABLE || 'Users';
const WHOP_SECRET   = process.env.WHOP_WEBHOOK_SECRET;
const WEBHOOK_PORT  = 3000;

function getAirtableBase() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    throw new Error('AIRTABLE_API_KEY y AIRTABLE_BASE_ID requeridos en .env');
  }
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

function verifyWhopSignature(rawBody, signatureHeader) {
  if (!WHOP_SECRET || !signatureHeader) return false;
  // Whop sends: "t=<timestamp>,v1=<hmac>"
  const parts = Object.fromEntries(signatureHeader.split(',').map(p => p.split('=')));
  const timestamp = parts.t;
  const signature = parts.v1;
  if (!timestamp || !signature) return false;
  const expected = crypto
    .createHmac('sha256', WHOP_SECRET)
    .update(`${timestamp}.${rawBody}`)
    .digest('hex');
  return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected));
}

async function findAirtableUser(telegramId) {
  const base = getAirtableBase();
  const records = await base(AIRTABLE_TABLE)
    .select({ filterByFormula: `{telegram_id} = "${telegramId}"`, maxRecords: 1 })
    .firstPage();
  return records[0] || null;
}

async function upsertAirtableUser(telegramId, fields) {
  const base = getAirtableBase();
  const existing = await findAirtableUser(telegramId);
  if (existing) {
    await base(AIRTABLE_TABLE).update(existing.id, fields);
    console.log(`📋 Airtable actualizado: telegram_id=${telegramId}`, fields);
  } else {
    await base(AIRTABLE_TABLE).create({ telegram_id: String(telegramId), ...fields });
    console.log(`📋 Airtable creado: telegram_id=${telegramId}`, fields);
  }
}

function expiresAt30Days() {
  const d = new Date();
  d.setDate(d.getDate() + 30);
  return d.toISOString().split('T')[0];
}

const app = express();

// Parse raw body for signature verification before JSON parsing
app.use('/webhook/whop', express.raw({ type: 'application/json' }));
app.use(express.json());

app.post('/webhook/whop', async (req, res) => {
  const rawBody  = req.body.toString('utf8');
  const sigHeader = req.headers['whop-signature'];

  if (WHOP_SECRET && !verifyWhopSignature(rawBody, sigHeader)) {
    console.warn('⚠️  Whop webhook: firma inválida');
    return res.status(401).json({ error: 'Invalid signature' });
  }

  let payload;
  try { payload = JSON.parse(rawBody); }
  catch { return res.status(400).json({ error: 'Invalid JSON' }); }

  const event      = payload.event || payload.type;
  const data       = payload.data || payload;
  const telegramId = data?.user?.telegram_id || data?.metadata?.telegram_id || data?.telegram_id;

  console.log(`📨 Whop event: ${event} | telegram_id=${telegramId}`);

  try {
    if (event === 'membership.created' || event === 'membership.renewed') {
      const expires = expiresAt30Days();

      if (telegramId) {
        await upsertAirtableUser(telegramId, { plan: 'pro', expires_at: expires });

        await bot.sendMessage(telegramId,
          `🎉 *¡Bienvenido a TIPSTER PRO!*\n\n` +
          `Tu suscripción está activa hasta el *${expires}*.\n\n` +
          `Ahora tienes acceso completo a:\n` +
          `• 🎯 Picks del día con análisis estadístico real\n` +
          `• 📡 Picks en vivo con estadísticas en tiempo real\n` +
          `• 🔍 Análisis de cualquier equipo o partido\n` +
          `• 📊 Historial de aciertos y rendimiento\n` +
          `• 📸 Análisis de imágenes de partidos\n\n` +
          `Escríbeme cualquier cosa para empezar. ¡Buena suerte! ⚽`,
          { parse_mode: 'Markdown' }
        );
        console.log(`✅ Bienvenida PRO enviada a telegram_id=${telegramId}`);
      }
    }

    if (event === 'membership.deleted' || event === 'membership.expired') {
      if (telegramId) {
        await upsertAirtableUser(telegramId, { plan: 'free', expires_at: '' });

        await bot.sendMessage(telegramId,
          `⚠️ *Tu suscripción PRO ha finalizado.*\n\n` +
          `Tu acceso ha cambiado al plan gratuito.\n\n` +
          `Para renovar y seguir recibiendo picks profesionales, visita nuestro canal.`,
          { parse_mode: 'Markdown' }
        );
        console.log(`📤 Notificación expiración enviada a telegram_id=${telegramId}`);
      }
    }

    res.status(200).json({ received: true });
  } catch (err) {
    console.error('Webhook handler error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (_req, res) => res.json({ status: 'ok', ts: new Date().toISOString() }));

app.listen(WEBHOOK_PORT, () => {
  console.log(`🌐 Webhook server escuchando en puerto ${WEBHOOK_PORT}`);
  console.log(`   POST http://localhost:${WEBHOOK_PORT}/webhook/whop`);
});
