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

// ─── Highlightly ─────────────────────────────────────────────────────────────

const API = axios.create({
  baseURL: 'https://soccer.highlightly.net',
  headers: { 'x-rapidapi-key': process.env.HIGHLIGHTLY_KEY },
  timeout: 15000,
});

// Logging interceptor — every API call is logged
API.interceptors.request.use(req => {
  const params = new URLSearchParams(req.params || {}).toString();
  console.log(`🔍 API: ${req.baseURL}${req.url}${params ? '?' + params : ''}`);
  return req;
});
API.interceptors.response.use(res => {
  const n = Array.isArray(res.data)
    ? res.data.length
    : (res.data?.data?.length ?? res.data?.pagination?.totalCount ?? '?');
  console.log(`📊 Respuesta: results=${n}`);
  return res;
});

// ─── Highlightly helpers ──────────────────────────────────────────────────────

const HL_STATUS = {
  'Not started':     'NS',
  'First half':      '1H',
  'Half time':       'HT',
  'Second half':     '2H',
  'Extra time':      'ET',
  'Penalties':       'P',
  'Break time':      'BT',
  'Finished':        'FT',
  'Finished (AET)':  'AET',
  'Finished (PEN)':  'PEN',
  'Postponed':       'PST',
  'Cancelled':       'CANC',
  'Suspended':       'SUSP',
  'Awarded':         'AWD',
  'Walkover':        'WO',
  'Abandoned':       'ABD',
};

const LIVE_DESCS = new Set(['First half', 'Half time', 'Second half', 'Extra time', 'Penalties', 'Break time']);
const FINISHED_DESCS = new Set(['Finished', 'Finished (AET)', 'Finished (PEN)']);

function parseHLScore(scoreStr) {
  if (!scoreStr) return { home: null, away: null };
  const parts = scoreStr.split(' - ');
  return { home: parseInt(parts[0]) || 0, away: parseInt(parts[1]) || 0 };
}

// Convert Highlightly match → API-Football-compatible shape (for handlers that use .fixture/.teams/.goals)
function hlToApif(m) {
  const score = parseHLScore(m.state?.score?.current);
  return {
    fixture: {
      id: m.id,
      date: m.date,
      status: { short: HL_STATUS[m.state?.description] || 'NS', elapsed: m.state?.clock || null },
      referee: null,
      venue: null,
    },
    league: { id: m.league.id, name: m.league.name, country: m.country?.name || 'World' },
    teams: {
      home: { id: m.homeTeam.id, name: m.homeTeam.name },
      away: { id: m.awayTeam.id, name: m.awayTeam.name },
    },
    goals: { home: score.home, away: score.away },
    score: { halftime: { home: null, away: null } },
  };
}

// ─── League config ────────────────────────────────────────────────────────────

const LEAGUE_SEASONS = {
  // Top 5 Europe
  33973:2025, 119924:2025, 115669:2025, 67162:2025, 52695:2025,
  // European 2nd tier
  34824:2025, 120775:2025, 116520:2025, 68013:2025, 53546:2025,
  // European club competitions
  2486:2025, 3337:2025, 722432:2025,
  // Other European leagues
  75672:2025, 80778:2025, 153113:2025, 123328:2025, 168431:2025,
  176941:2025, 179494:2025, 186302:2025, 102053:2025, 90990:2025,
  96947:2025, 88437:2025, 244170:2025, 271402:2025,
  // International
  1635:2026, 8443:2024, 4188:2024, 5039:2024, 29718:2026, 14400:2025,
  // South American club competitions
  11847:2025, 10145:2025,
  // South American leagues
  61205:2025, 62056:2025, 109712:2025,
  228852:2025, 230554:2025,
  204173:2025, 205024:2025, 205875:2025,
  226299:2025, 206726:2025, 255233:2025,
  293528:2025, 213534:2025, 215236:2025,
  239915:2025, 1049216:2025,
  // CONCACAF
  216087:2025, 223746:2025, 224597:2025,
  // Asia / Africa
  262041:2025, 84182:2025, 249276:2025, 199067:2025,
  173537:2025, 304591:2025, 305442:2025,
};

const LEAGUE_IDS = new Set(Object.keys(LEAGUE_SEASONS).map(Number));

const LEAGUE_MAP = {
  1635:  { name:'World Cup',          country:'World'       },
  33973: { name:'Premier League',     country:'England'     },
  119924:{ name:'LaLiga',             country:'Spain'       },
  115669:{ name:'Serie A',            country:'Italy'       },
  67162: { name:'Bundesliga',         country:'Germany'     },
  52695: { name:'Ligue 1',            country:'France'      },
  2486:  { name:'Champions League',   country:'Europe'      },
  3337:  { name:'Europa League',      country:'Europe'      },
  722432:{ name:'Conference League',  country:'Europe'      },
  11847: { name:'Libertadores',       country:'South Am.'   },
  10145: { name:'Sudamericana',       country:'South Am.'   },
  61205: { name:'Brasileirao',        country:'Brazil'      },
  204173:{ name:'Liga BetPlay',       country:'Colombia'    },
  109712:{ name:'Liga Argentina',     country:'Argentina'   },
  223746:{ name:'Liga MX',            country:'Mexico'      },
  216087:{ name:'MLS',                country:'USA'         },
  75672: { name:'Eredivisie',         country:'Netherlands' },
  80778: { name:'Primeira Liga',      country:'Portugal'    },
  173537:{ name:'Super Lig',          country:'Turkey'      },
  262041:{ name:'Saudi Pro League',   country:'Saudi Arabia'},
  123328:{ name:'Jupiler Pro League', country:'Belgium'     },
  168431:{ name:'Super League 1',     country:'Greece'      },
  199067:{ name:'Egyptian Premier',   country:'Egypt'       },
  249276:{ name:'K League 1',         country:'South Korea' },
  84182: { name:'J1 League',          country:'Japan'       },
  153113:{ name:'Scottish Prem.',     country:'Scotland'    },
  4188:  { name:'UEFA Euro',          country:'Europe'      },
  5039:  { name:'Nations League',     country:'Europe'      },
  8443:  { name:'Copa America',       country:'South Am.'   },
  205024:{ name:'Primera B Colombia', country:'Colombia'    },
  34824: { name:'Championship',       country:'England'     },
  120775:{ name:'LaLiga2',            country:'Spain'       },
  116520:{ name:'Serie B',            country:'Italy'       },
  68013: { name:'2.Bundesliga',       country:'Germany'     },
  53546: { name:'Ligue 2',            country:'France'      },
  62056: { name:'Brasileirao B',      country:'Brazil'      },
  224597:{ name:'Liga Expansión MX',  country:'Mexico'      },
  88437: { name:'Eliteserien',        country:'Norway'      },
  271402:{ name:'Primera División',   country:'Cyprus'      },
  29718: { name:'Clasif. Mundial',    country:'South Am.'   },
  228852:{ name:'Apertura Uruguay',   country:'Uruguay'     },
  230554:{ name:'Clausura Uruguay',   country:'Uruguay'     },
  226299:{ name:'Liga Chilena',       country:'Chile'       },
  206726:{ name:'LigaPro Ecuador',    country:'Ecuador'     },
  255233:{ name:'Liga Venezuela',     country:'Venezuela'   },
  213534:{ name:'Apertura Paraguay',  country:'Paraguay'    },
  215236:{ name:'Clausura Paraguay',  country:'Paraguay'    },
  239915:{ name:'Liga 1 Perú',        country:'Peru'        },
  293528:{ name:'Liga Boliviana',     country:'Bolivia'     },
  205875:{ name:'Copa Colombia',      country:'Colombia'    },
  176941:{ name:'Swiss SL',           country:'Switzerland' },
  179494:{ name:'HNL Croatia',        country:'Croatia'     },
  186302:{ name:'Bundesliga AT',      country:'Austria'     },
  102053:{ name:'Superliga DK',       country:'Denmark'     },
  90990: { name:'Ekstraklasa',        country:'Poland'      },
  96947: { name:'Allsvenskan',        country:'Sweden'      },
  244170:{ name:'Super Liga',         country:'Serbia'      },
  304591:{ name:'League of Ireland',  country:'Ireland'     },
  305442:{ name:'First Division IE',  country:'Ireland'     },
  14400: { name:'CONCACAF CL',        country:'CONCACAF'    },
  1049216:{ name:'Copa de la Liga PE', country:'Peru'       },
};

// Maps user-written league names → league ID
const LEAGUE_NAME_TO_ID = {
  'fifa world cup':1635, 'world cup':1635, 'mundial':1635, 'copa del mundo':1635,
  'bundesliga':67162, '2.bundesliga':68013, 'segunda bundesliga':68013,
  'premier league':33973, 'premier':33973, 'epl':33973,
  'laliga':119924, 'la liga':119924, 'primera division':119924,
  'laliga2':120775, 'segunda division':120775,
  'serie a':115669, 'serie b':116520,
  'ligue 1':52695, 'ligue1':52695, 'ligue 2':53546, 'ligue2':53546,
  'champions league':2486, 'champions':2486, 'ucl':2486,
  'europa league':3337, 'europa':3337, 'uel':3337,
  'conference league':722432, 'conference':722432,
  'libertadores':11847, 'copa libertadores':11847,
  'sudamericana':10145, 'copa sudamericana':10145,
  'brasileirao':61205, 'serie a brasileira':61205, 'seriea brasileira':61205,
  'brasil':61205, 'brazil':61205, 'liga brasil':61205, 'liga brazil':61205,
  'serie a brasil':61205, 'seriea brasil':61205, 'série a brasil':61205,
  'brasileirao b':62056, 'serie b brasil':62056, 'serieb brasil':62056,
  'liga betplay':204173, 'primera a':204173, 'liga colombia':204173, 'betplay':204173,
  'liga colombia b':205024, 'primera b colombia':205024, 'torneo aguila':205024, 'torneo águila':205024,
  'liga argentina':109712, 'primera division argentina':109712,
  'liga mx':223746, 'ligamx':223746, 'ascenso mx':224597, 'liga expansion':224597,
  'mls':216087,
  'eredivisie':75672,
  'primeira liga':80778, 'liga nos':80778,
  'super lig':173537, 'superlig':173537, 'turquia':173537, 'turkey':173537, 'liga turca':173537, 'tff':173537, 'turkiye':173537,
  'saudi pro league':262041, 'saudi league':262041,
  'jupiler pro league':123328, 'jupiler':123328,
  'super league grecia':168431, 'super league':168431,
  'liga egipto':199067,
  'k league':249276, 'k-league':249276,
  'j league':84182, 'j1 league':84182,
  'scottish premier':153113, 'scottish premiership':153113,
  'championship':34824,
  'euro':4188, 'euro championship':4188, 'eurocopa':4188,
  'nations league':5039, 'uefa nations league':5039,
  'copa america':8443,
  'chipre':271402, 'primera division chipre':271402, 'primera división chipre':271402, 'cyprus':271402,
  'noruega':88437, 'eliteserien':88437, 'norway':88437,
  'clasificatorias':29718, 'eliminatorias':29718, 'clasif mundial':29718,
  'uruguay':228852, 'apertura uruguay':228852, 'clausura uruguay':230554,
  'chile':226299, 'liga chilena':226299,
  'ecuador':206726, 'ligapro':206726,
  'venezuela':255233, 'liga venezolana':255233,
  'paraguay':213534, 'apertura paraguay':213534, 'clausura paraguay':215236,
  'peru':239915, 'liga 1 peru':239915, 'liga1 peru':239915,
  'bolivia':293528, 'liga boliviana':293528,
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
  2486:100, 3337:95, 722432:90,
  33973:88, 119924:87, 115669:86, 67162:85, 52695:84,
  11847:80, 10145:78,
  1635:77, 8443:76, 4188:75, 5039:74,
  75672:70, 80778:69, 173537:68, 123328:67,
  61205:65, 223746:64, 109712:63, 204173:62, 216087:61,
  34824:55, 120775:54, 116520:53, 68013:52, 53546:51,
  205024:45, 62056:44, 224597:41,
  262041:38, 84182:37, 249276:36, 153113:35,
  271402:33, 88437:32, 168431:31, 199067:30,
  244170:29, 176941:28, 179494:27, 186302:26,
  102053:25, 90990:24, 96947:23, 29718:22,
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

// Parses a Highlightly match object → internal fixture format
function parseFixture(m) {
  const score = parseHLScore(m.state?.score?.current);
  return {
    fixtureId:  m.id,
    date:       m.date,
    status:     HL_STATUS[m.state?.description] || 'NS',
    elapsed:    m.state?.clock || null,
    leagueId:   m.league.id,
    leagueName: LEAGUE_MAP[m.league.id]?.name || m.league.name,
    country:    LEAGUE_MAP[m.league.id]?.country || m.country?.name || 'World',
    homeId:     m.homeTeam.id,
    awayId:     m.awayTeam.id,
    homeTeam:   m.homeTeam.name,
    awayTeam:   m.awayTeam.name,
    homeGoals:  score.home,
    awayGoals:  score.away,
    referee:    null,
    venue:      null,
  };
}

async function fetchFixturesByDate(date) {
  if (dateCache.has(date)) return dateCache.get(date);
  const PAGE = 100;
  const result = [];
  let offset = 0;
  let total = Infinity;
  while (result.length < total) {
    const { data } = await API.get('/matches', { params: { date, limit: PAGE, offset } });
    const page = data.data || [];
    result.push(...page);
    total = data.pagination?.totalCount ?? result.length;
    if (page.length < PAGE) break;
    offset += PAGE;
    if (result.length < total) await new Promise(r => setTimeout(r, 300));
  }
  dateCache.set(date, result);
  return result;
}

async function getFixturesByDate(date) {
  const all = await fetchFixturesByDate(date);
  return all.filter(m => LEAGUE_IDS.has(m.league?.id)).map(parseFixture);
}

async function fetchLiveRaw() {
  if (Date.now() - liveCache.ts < 30000 && liveCache.raw) return liveCache.raw;
  const today = new Date().toISOString().split('T')[0];
  // fetchFixturesByDate handles pagination and caching
  const allToday = await fetchFixturesByDate(today);
  const raw = allToday.filter(m => LIVE_DESCS.has(m.state?.description));
  liveCache = { raw, ts: Date.now() };
  return raw;
}

async function getLiveFixtures(leagueId = null) {
  const raw = await fetchLiveRaw();
  const filtered = leagueId
    ? raw.filter(m => m.league?.id === leagueId)
    : raw.filter(m => LEAGUE_IDS.has(m.league?.id));
  return filtered.map(parseFixture);
}

// Stat name mapping: Highlightly displayName → API-Football key (used by calcLiveMomentum etc.)
const HL_STAT_MAP = {
  'Shots on target':           'Shots on Goal',
  'Corners':                   'Corner Kicks',
  'Yellow cards':              'Yellow Cards',
  'Red cards':                 'Red Cards',
  'Shots within penalty area': 'Shots insidebox',
};

async function getFixtureStatistics(fixtureId) {
  const { data } = await API.get('/statistics/' + fixtureId);
  if (!data || data.length === 0) return null;
  const stats = {};
  data.forEach(teamData => {
    const key = teamData.team.name;
    const raw = {};
    teamData.statistics.forEach(s => { raw[s.displayName] = s.value; });

    const totalShots = (raw['Shots on target'] || 0) + (raw['Shots off target'] || 0) + (raw['Blocked shots'] || 0);
    const possession = raw['Possession']; // decimal e.g. 0.38

    stats[key] = { 'Total Shots': totalShots || null };
    if (possession != null) stats[key]['Ball Possession'] = `${Math.round(possession * 100)}%`;
    for (const [hlName, apifName] of Object.entries(HL_STAT_MAP)) {
      if (raw[hlName] != null) stats[key][apifName] = raw[hlName];
    }
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

// Traducciones español → nombre en inglés usado por la API (selecciones nacionales)
const TEAM_NAME_ES_EN = {
  // Corea
  'corea del sur': 'South Korea', 'korea del sur': 'South Korea', 'corea sur': 'South Korea',
  'corea del norte': 'North Korea', 'korea del norte': 'North Korea',
  // Europa del este
  'republica checa': 'Czech Republic', 'chequia': 'Czech Republic', 'eslovaquia': 'Slovakia',
  'eslovenia': 'Slovenia', 'croacia': 'Croatia', 'hungria': 'Hungary', 'rumania': 'Romania',
  'rumania': 'Romania', 'rusia': 'Russia', 'ucrania': 'Ukraine', 'polonia': 'Poland',
  'serbia': 'Serbia', 'albania': 'Albania', 'turquia': 'Turkey',
  // Europa occidental
  'alemania': 'Germany', 'francia': 'France', 'espana': 'Spain', 'belgica': 'Belgium',
  'holanda': 'Netherlands', 'paises bajos': 'Netherlands', 'suiza': 'Switzerland',
  'dinamarca': 'Denmark', 'suecia': 'Sweden', 'noruega': 'Norway', 'finlandia': 'Finland',
  'austria': 'Austria', 'grecia': 'Greece', 'escocia': 'Scotland', 'gales': 'Wales',
  'irlanda': 'Ireland', 'irlanda del norte': 'Northern Ireland',
  // Americas
  'brasil': 'Brazil', 'estados unidos': 'USA', 'eeuu': 'USA', 'mexico': 'Mexico',
  'peru': 'Peru', 'colombia': 'Colombia', 'ecuador': 'Ecuador', 'venezuela': 'Venezuela',
  'chile': 'Chile', 'paraguay': 'Paraguay', 'uruguay': 'Uruguay', 'bolivia': 'Bolivia',
  'costa rica': 'Costa Rica', 'el salvador': 'El Salvador', 'panama': 'Panama',
  'honduras': 'Honduras', 'guatemala': 'Guatemala', 'haiti': 'Haiti', 'jamaica': 'Jamaica',
  'canada': 'Canada',
  // Africa
  'marruecos': 'Morocco', 'argelia': 'Algeria', 'costa de marfil': 'Ivory Coast',
  'senegal': 'Senegal', 'camerun': 'Cameroon', 'ghana': 'Ghana', 'nigeria': 'Nigeria',
  'egipto': 'Egypt', 'tunez': 'Tunisia', 'sudafrica': 'South Africa',
  // Asia / Medio Oriente
  'japon': 'Japan', 'china': 'China', 'iran': 'Iran', 'irak': 'Iraq',
  'arabia saudita': 'Saudi Arabia', 'arabia saudi': 'Saudi Arabia',
  'emiratos arabes unidos': 'UAE', 'emiratos arabes': 'UAE', 'emiratos': 'UAE',
  'siria': 'Syria', 'jordania': 'Jordan', 'qatar': 'Qatar', 'kuwait': 'Kuwait',
  'tailandia': 'Thailand', 'vietnam': 'Vietnam', 'indonesia': 'Indonesia',
  'australia': 'Australia', 'nueva zelanda': 'New Zealand',
};

function translateTeamName(name) {
  const key = name.toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .trim();
  return TEAM_NAME_ES_EN[key] || name;
}

async function searchTeam(name, countryHint = '') {
  const apiName = translateTeamName(name);
  const { data } = await API.get('/teams', { params: { name: apiName, limit: 20 } });
  const results = data.data || [];
  if (results.length === 0) return null;

  const q = normalizeTeamName(apiName);
  const RESERVE = /\b(ii|b|reserve|reserva|sub|youth|juvenil|u\d{2}|amateur|filial)\b/i;
  const WOMEN   = /\b(women|femenin[ao]|ladies|femmes|damen|vrouwen|mujer|fem\.?)\b| W$/i;

  function score(t) {
    const tname = normalizeTeamName(t.name);
    let s = 0;
    if (tname === q) s += 100;
    else if (tname.endsWith(' ' + q) || tname.endsWith(q)) s += 80;
    else if (tname.startsWith(q + ' ') || tname.startsWith(q)) s += 50;
    else if (tname.includes(q)) s += 20;
    if (t.type === 'national') s += 15;
    if (RESERVE.test(t.name)) s -= 40;
    if (WOMEN.test(t.name))   s -= 60;
    return s;
  }
  const best = results.sort((a, b) => score(b) - score(a))[0];
  return best ? { team: { id: best.id, name: best.name } } : null;
}

async function findNextFixtureByDate(teamId, daysAhead = 14) {
  const LIVE_STATUSES = new Set(['First half', 'Half time', 'Second half', 'Extra time', 'Penalties', 'Break time']);
  const UPCOMING_STATUSES = new Set(['Not started', ...LIVE_STATUSES]);
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() + daysAhead);

  // 1. Partido en vivo hoy para este equipo
  try {
    const today = new Date().toISOString().split('T')[0];
    const allToday = await fetchFixturesByDate(today);
    const live = allToday.find(m =>
      (m.homeTeam.id === teamId || m.awayTeam.id === teamId) &&
      LIVE_STATUSES.has(m.state?.description)
    );
    if (live) return hlToApif(live);
  } catch {}

  // 2. Próximos partidos en casa
  try {
    const { data } = await API.get('/matches', { params: { homeTeamId: teamId, limit: 10 } });
    const next = (data.data || [])
      .filter(m => UPCOMING_STATUSES.has(m.state?.description) && new Date(m.date) <= cutoff)
      .sort((a, b) => new Date(a.date) - new Date(b.date))[0];
    if (next) return hlToApif(next);
  } catch {}

  // 3. Próximos partidos fuera
  try {
    const { data } = await API.get('/matches', { params: { awayTeamId: teamId, limit: 10 } });
    const next = (data.data || [])
      .filter(m => UPCOMING_STATUSES.has(m.state?.description) && new Date(m.date) <= cutoff)
      .sort((a, b) => new Date(a.date) - new Date(b.date))[0];
    if (next) return hlToApif(next);
  } catch {}

  return null;
}

async function getTeamLastFixtures(teamId, last = 15, venue = null) {
  const matches = [];
  if (venue !== 'away') {
    try {
      const { data } = await API.get('/matches', { params: { homeTeamId: teamId, limit: 50 } });
      (data.data || []).filter(m => FINISHED_DESCS.has(m.state?.description)).forEach(m => matches.push(m));
    } catch {}
  }
  if (venue !== 'home') {
    try {
      const { data } = await API.get('/matches', { params: { awayTeamId: teamId, limit: 50 } });
      (data.data || []).filter(m => FINISHED_DESCS.has(m.state?.description)).forEach(m => matches.push(m));
    } catch {}
  }

  const seen = new Set();
  return matches
    .filter(m => { if (seen.has(m.id)) return false; seen.add(m.id); return true; })
    .sort((a, b) => new Date(b.date) - new Date(a.date))
    .slice(0, last)
    .map(m => {
      const score = parseHLScore(m.state?.score?.current || '0 - 0');
      return {
        fixtureId:  m.id,
        date:       m.date.split('T')[0],
        homeTeam:   m.homeTeam.name,
        awayTeam:   m.awayTeam.name,
        homeId:     m.homeTeam.id,
        awayId:     m.awayTeam.id,
        leagueName: m.league.name,
        leagueId:   m.league.id,
        goalsHome:  score.home ?? 0,
        goalsAway:  score.away ?? 0,
        htHome:     null,
        htAway:     null,
      };
    });
}

async function getLeagueStandings(leagueId) {
  const season = LEAGUE_SEASONS[leagueId] || 2025;
  const { data } = await API.get('/standings', { params: { leagueId, season } });
  const groups = data.groups || [];
  const group = groups[0]?.standings || [];
  return group.map(s => ({ teamId: s.team.id, teamName: s.team.name, rank: s.position }));
}

// ── StatsHub — estadísticas de árbitros (tarjetas, fouls, BTC) ───────────────
// API pública: https://www.statshub.com/api/referees/list
// Se fetchea automáticamente al iniciar y se refresca cada 6 horas.

const STATSHUB_API = 'https://www.statshub.com/api/referees/list?page=1&limit=500&upcomingFixturesOnly=false&last20GamesOnly=true&leagueStatsOnly=false&sortField=next_game_timestamp&sortDirection=asc';

let statsHubReferees = []; // array de objetos parseados en memoria

function normalizeRefName(name) {
  return name
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z\s]/g, ' ')
    .split(/\s+/).filter(Boolean)
    .sort()
    .join(' ');
}

function parseStatsHubApiData(refs) {
  return refs.map(r => ({
    nombre:                  r.name,
    normalizado:             normalizeRefName(r.name),
    partidos:                parseInt(r.games) || null,
    amarillas_por_partido:   parseFloat(r.avg_yellow_cards_per_game) || null,
    rojas_por_partido:       parseFloat(r.avg_red_cards_per_game) || null,
    avg_tarjetas:            parseFloat(r.avg_cards_per_game) || null,
    tarjetas_1T:             parseFloat(r.avg_first_half_cards) || null,
    tarjetas_2T:             parseFloat(r.avg_second_half_cards) || null,
    penaltis_por_partido:    (r.total_penalties && r.games)
                               ? +(parseInt(r.total_penalties) / parseInt(r.games)).toFixed(2)
                               : null,
    faltas_por_partido:      parseFloat(r.avg_fouls_per_game) || null,
    pct_over35_tarjetas:     parseFloat(r.o35_cards_pct) || null,
    pct_over45_tarjetas:     parseFloat(r.o45_cards_pct) || null,
    pct_over55_tarjetas:     parseFloat(r.o55_cards_pct) || null,
    pct_btc:                 parseFloat(r.both_teams_card_pct) || null,
    pct_btc2:                parseFloat(r.both_teams_o15_card_pct) || null,
  }));
}

async function fetchStatsHubReferees() {
  try {
    const res = await axios.get(STATSHUB_API, { timeout: 15000 });
    const refs = res.data?.data || res.data || [];
    if (!Array.isArray(refs) || refs.length === 0) return;
    statsHubReferees = parseStatsHubApiData(refs);
    console.log(`✅ StatsHub: ${statsHubReferees.length} árbitros cargados`);
  } catch (e) {
    console.warn(`⚠️ StatsHub fetch error: ${e.message}`);
  }
}

function findStatsHubReferee(refereeName) {
  if (!refereeName || statsHubReferees.length === 0) return null;
  const needleTokens = new Set(normalizeRefName(refereeName).split(' '));
  let best = null, bestScore = 0;
  for (const ref of statsHubReferees) {
    const hayTokens = new Set(ref.normalizado.split(' '));
    const intersection = [...needleTokens].filter(t => hayTokens.has(t)).length;
    const union = new Set([...needleTokens, ...hayTokens]).size;
    const score = intersection / union;
    if (score > bestScore) { bestScore = score; best = ref; }
  }
  return bestScore >= 0.35 ? best : null;
}

// ── SofaScore — árbitro + forma reciente ─────────────────────────────────────
// SofaScore tiene promedios pre-calculados del árbitro (career stats / partidos).
// 2 llamadas máx por partido (eventos del día + detalle), con caché agresivo.

const sofaEventCache = new Map();

const SOFA_USER_AGENTS = [
  'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
];
let _sofaUaIndex = 0;

function _sofaHeaders() {
  const ua = SOFA_USER_AGENTS[_sofaUaIndex % SOFA_USER_AGENTS.length];
  return {
    'User-Agent': ua,
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.sofascore.com/',
    'Origin': 'https://www.sofascore.com',
    'Cache-Control': 'no-cache',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Connection': 'keep-alive',
  };
}

async function fetchSofaScoreEvents(date) {
  if (sofaEventCache.has(date)) return sofaEventCache.get(date);

  for (let attempt = 0; attempt < SOFA_USER_AGENTS.length; attempt++) {
    _sofaUaIndex = attempt;
    try {
      const { data } = await axios.get(
        `https://api.sofascore.com/api/v1/sport/football/scheduled-events/${date}`,
        { headers: _sofaHeaders(), timeout: 12000 }
      );
      const events = data.events || [];
      sofaEventCache.set(date, events);
      if (attempt > 0) console.log(`🔄 SofaScore OK con UA #${attempt}: ${events.length} eventos`);
      else console.log(`✅ SofaScore: ${events.length} eventos para ${date}`);
      return events;
    } catch (e) {
      const status = e.response?.status;
      console.warn(`⚠️ SofaScore intento ${attempt + 1}/4 (UA#${attempt}) → ${status || e.message}`);
      if (status !== 403 && status !== 429) break;
      await new Promise(r => setTimeout(r, 800 * (attempt + 1)));
    }
  }
  console.warn('❌ SofaScore bloqueado en todos los intentos');
  return [];
}

function sofaNormalize(str) {
  return (str || '').toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/\bfc\b|\baf\b|\bac\b|\bsc\b|\bsk\b|\bcf\b|\bfk\b|\bif\b|\bbk\b|\bcd\b|\bsd\b|\bud\b|\brcd\b|\bas\b|\bss\b|\bus\b|\bsv\b|\bbv\b/g, '')
    .replace(/[^a-z0-9]/g, '')
    .trim();
}

function sofaTeamMatch(apiName, sofaName) {
  const a = sofaNormalize(apiName);
  const b = sofaNormalize(sofaName);
  if (a === b) return true;
  const minLen = Math.min(a.length, b.length);
  if (minLen >= 4) {
    return a.startsWith(b.substring(0, Math.min(5, b.length))) ||
           b.startsWith(a.substring(0, Math.min(5, a.length)));
  }
  return false;
}

async function getSofaMatchContext(homeTeam, awayTeam, sofaEvents) {
  try {
    const event = sofaEvents.find(e =>
      sofaTeamMatch(homeTeam, e.homeTeam?.name) && sofaTeamMatch(awayTeam, e.awayTeam?.name)
    );
    if (!event) {
      console.log(`SofaScore: sin match para ${homeTeam} vs ${awayTeam}`);
      return null;
    }

    const [detailRes, homeFormRes, awayFormRes] = await Promise.allSettled([
      axios.get(`https://api.sofascore.com/api/v1/event/${event.id}`, { headers: _sofaHeaders(), timeout: 8000 }),
      axios.get(`https://api.sofascore.com/api/v1/team/${event.homeTeam.id}/last/0`, { headers: _sofaHeaders(), timeout: 8000 }),
      axios.get(`https://api.sofascore.com/api/v1/team/${event.awayTeam.id}/last/0`, { headers: _sofaHeaders(), timeout: 8000 }),
    ]);

    const result = { fuente: 'sofascore' };

    // Árbitro (nombre desde SofaScore) + stats desde StatsHub + rankings FIFA
    if (detailRes.status === 'fulfilled') {
      const ev  = detailRes.value.data?.event;
      const ref = ev?.referee;
      if (ref) {
        const sh = findStatsHubReferee(ref.name);
        result.arbitro = {
          nombre:   ref.name,
          partidos: sh?.partidos || null,
          ...(sh ? {
            amarillas_por_partido: sh.amarillas_por_partido,
            rojas_por_partido:     sh.rojas_por_partido,
            avg_tarjetas:          sh.avg_tarjetas,
            tarjetas_1T:           sh.tarjetas_1T,
            tarjetas_2T:           sh.tarjetas_2T,
            penaltis_por_partido:  sh.penaltis_por_partido,
            faltas_por_partido:    sh.faltas_por_partido,
            pct_over35_tarjetas:   sh.pct_over35_tarjetas,
            pct_over45_tarjetas:   sh.pct_over45_tarjetas,
            pct_over55_tarjetas:   sh.pct_over55_tarjetas,
            pct_btc:               sh.pct_btc,
            pct_btc2:              sh.pct_btc2,
            fuente_stats:          'statshub',
          } : { fuente_stats: 'sin_datos' }),
        };
        console.log(`🃏 Árbitro [${homeTeam}]: ${ref.name}${sh ? ` | StatsHub ✓ BTC=${sh.pct_btc}% +3.5=${sh.pct_over35_tarjetas}%` : ' | sin datos StatsHub'}`);
      }

      // Rankings FIFA (presentes en partidos internacionales)
      const rankH = ev?.homeTeam?.ranking ?? ev?.homeTeamScore?.ranking ?? null;
      const rankA = ev?.awayTeam?.ranking ?? ev?.awayTeamScore?.ranking ?? null;
      if (rankH != null || rankA != null) {
        result.rankingsFIFA = {
          local:     rankH ? `#${rankH}` : null,
          visitante: rankA ? `#${rankA}` : null,
        };
        console.log(`🌍 Rankings FIFA: local=#${rankH} visitante=#${rankA}`);
      }
    }

    // Forma reciente local
    if (homeFormRes.status === 'fulfilled') {
      const evs = homeFormRes.value.data?.events || [];
      const hid = event.homeTeam.id;
      result.formaLocal = evs.slice(0, 6).map(e => {
        const esL = e.homeTeam?.id === hid;
        const mis = esL ? e.homeScore?.current : e.awayScore?.current;
        const opp = esL ? e.awayScore?.current : e.homeScore?.current;
        if (mis == null || opp == null) return '?';
        return mis > opp ? 'G' : mis < opp ? 'P' : 'E';
      }).join('-');
    }

    // Forma reciente visitante
    if (awayFormRes.status === 'fulfilled') {
      const evs = awayFormRes.value.data?.events || [];
      const aid = event.awayTeam.id;
      result.formaVisitante = evs.slice(0, 6).map(e => {
        const esL = e.homeTeam?.id === aid;
        const mis = esL ? e.homeScore?.current : e.awayScore?.current;
        const opp = esL ? e.awayScore?.current : e.homeScore?.current;
        if (mis == null || opp == null) return '?';
        return mis > opp ? 'G' : mis < opp ? 'P' : 'E';
      }).join('-');
    }

    return result;
  } catch (e) {
    console.warn('getSofaMatchContext error:', e.message);
    return null;
  }
}
// ─────────────────────────────────────────────────────────────────────────────

async function getH2H(id1, id2) {
  const { data } = await API.get('/head-2-head', { params: { teamIdOne: id1, teamIdTwo: id2, limit: 10 } });
  const matches = Array.isArray(data) ? data : (data.data || []);
  return matches
    .filter(m => FINISHED_DESCS.has(m.state?.description))
    .map(m => {
      const score = parseHLScore(m.state?.score?.current || '0 - 0');
      return {
        date:      m.date.split('T')[0],
        home:      m.homeTeam.name,
        away:      m.awayTeam.name,
        golesHome: score.home ?? 0,
        golesAway: score.away ?? 0,
        btts:      (score.home ?? 0) > 0 && (score.away ?? 0) > 0,
      };
    });
}

// Intenta extraer un valor numérico de múltiples rutas posibles en un objeto.
function safeNum(obj, ...paths) {
  for (const path of paths) {
    const parts = path.split('.');
    let val = obj;
    for (const p of parts) val = val?.[p];
    if (val != null && !isNaN(parseFloat(val))) return parseFloat(val);
  }
  return null;
}

let _teamStatsLogged = false; // log de estructura raw solo una vez por sesión

async function getTeamStats(teamId, leagueId) {
  const season = LEAGUE_SEASONS[leagueId] || 2025;
  const fromDate = `${season - 1}-06-01`;
  const { data } = await API.get('/teams/statistics/' + teamId, { params: { fromDate } });
  if (!data || data.length === 0) return null;

  // Prefer the requested league; fall back to highest-game-count entry
  const leagueStat = data.find(s => s.leagueId === leagueId)
    || data.sort((a, b) => (b.total?.games?.played || 0) - (a.total?.games?.played || 0))[0];
  if (!leagueStat) return null;

  // Log raw structure ONCE so podemos ver qué campos existen en Highlightly
  if (!_teamStatsLogged) {
    _teamStatsLogged = true;
    const home0 = leagueStat.home || {};
    console.log('📊 [DEBUG] TeamStats home keys:', JSON.stringify(Object.keys(home0)));
    console.log('📊 [DEBUG] TeamStats home sample:', JSON.stringify(home0, null, 2).slice(0, 1500));
  }

  const { home, away, total } = leagueStat;
  const hP = home.games?.played || 1;
  const aP = away.games?.played || 1;
  const tP = total.games?.played || (hP + aP) || 1;

  // ── Goles (confirmados) ──────────────────────────────────────────────────────
  const golesAnotadosHome  = +(home.goals.scored   / hP).toFixed(2);
  const golesAnotadosAway  = +(away.goals.scored   / aP).toFixed(2);
  const golesRecibidosHome = +(home.goals.received / hP).toFixed(2);
  const golesRecibidosAway = +(away.goals.received / aP).toFixed(2);

  // ── Porterías a cero / partidos sin marcar ───────────────────────────────────
  const csH = safeNum(home, 'goals.cleanSheets', 'cleanSheets', 'goals.clean_sheets', 'clean_sheets');
  const csA = safeNum(away, 'goals.cleanSheets', 'cleanSheets', 'goals.clean_sheets', 'clean_sheets');
  const ftsH = safeNum(home, 'goals.failedToScore', 'failedToScore', 'goals.failed_to_score', 'failed_to_score');
  const ftsA = safeNum(away, 'goals.failedToScore', 'failedToScore', 'goals.failed_to_score', 'failed_to_score');

  // ── Corners ──────────────────────────────────────────────────────────────────
  const cornH = safeNum(home, 'corners.total', 'corners.count', 'corners', 'cornerKicks', 'corner_kicks');
  const cornA = safeNum(away, 'corners.total', 'corners.count', 'corners', 'cornerKicks', 'corner_kicks');
  const cornT = safeNum(total, 'corners.total', 'corners.count', 'corners', 'cornerKicks', 'corner_kicks');

  // ── Tarjetas ─────────────────────────────────────────────────────────────────
  const yelH = safeNum(home, 'cards.yellow', 'yellowCards', 'yellow_cards', 'cards.yellowCards');
  const yelA = safeNum(away, 'cards.yellow', 'yellowCards', 'yellow_cards', 'cards.yellowCards');
  const redH = safeNum(home, 'cards.red', 'redCards', 'red_cards', 'cards.redCards');
  const redA = safeNum(away, 'cards.red', 'redCards', 'red_cards', 'cards.redCards');

  // ── Tiros ────────────────────────────────────────────────────────────────────
  const shotsH    = safeNum(home, 'shots.total', 'shots', 'totalShots', 'shots.on_target');
  const shotsA    = safeNum(away, 'shots.total', 'shots', 'totalShots', 'shots.on_target');
  const shotsOnH  = safeNum(home, 'shots.on_target', 'shotsOnTarget', 'shots_on_target');
  const shotsOnA  = safeNum(away, 'shots.on_target', 'shotsOnTarget', 'shots_on_target');

  // ── Posesión ─────────────────────────────────────────────────────────────────
  const posH = safeNum(home, 'possession', 'ballPossession', 'ball_possession');
  const posA = safeNum(away, 'possession', 'ballPossession', 'ball_possession');

  return {
    liga:      leagueStat.leagueName,
    temporada: leagueStat.season,
    partidos:  { home: hP, away: aP, total: tP },

    golesAnotadosHome,
    golesAnotadosAway,
    golesRecibidosHome,
    golesRecibidosAway,

    cleanSheetsHome:   csH,
    cleanSheetsAway:   csA,
    failedToScoreHome: ftsH,
    failedToScoreAway: ftsA,

    ...(cornH != null && { cornersPerGameHome: +(cornH / hP).toFixed(2) }),
    ...(cornA != null && { cornersPerGameAway: +(cornA / aP).toFixed(2) }),
    ...(cornT != null && { cornersPerGame:     +(cornT / tP).toFixed(2) }),

    ...(yelH != null && { tarjetasAmHome: +(yelH / hP).toFixed(2) }),
    ...(yelA != null && { tarjetasAmAway: +(yelA / aP).toFixed(2) }),
    ...(redH != null && { tarjetasRoHome: +(redH / hP).toFixed(2) }),
    ...(redA != null && { tarjetasRoAway: +(redA / aP).toFixed(2) }),

    ...(shotsH   != null && { tirosHome:       +(shotsH  / hP).toFixed(2) }),
    ...(shotsA   != null && { tirosAway:       +(shotsA  / aP).toFixed(2) }),
    ...(shotsOnH != null && { tirosArcoHome:   +(shotsOnH / hP).toFixed(2) }),
    ...(shotsOnA != null && { tirosArcoAway:   +(shotsOnA / aP).toFixed(2) }),

    ...(posH != null && { posesionHome: posH > 1 ? posH : +(posH * 100).toFixed(1) }),
    ...(posA != null && { posesionAway: posA > 1 ? posA : +(posA * 100).toFixed(1) }),

    victorias: { total: total.games.wins,  home: home.games.wins,  away: away.games.wins  },
    empates:   { total: total.games.draws, home: home.games.draws, away: away.games.draws },
    derrotas:  { total: total.games.loses, home: home.games.loses, away: away.games.loses },
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
  // Línea que las casas típicamente ofrecen en vivo: actual + mitad de lo esperado restante + margen 0.5
  // Ej: 1 córner al min45 → remaining≈1 → lineaVivo≈1+0.5+0.5=2.5 (Over 1.5 o Under 2.5)
  const lineaVivo = Math.floor(current + remaining * 0.5) + 0.5;
  return {
    projected:  +projected.toFixed(1),
    remaining:  +remaining.toFixed(1),
    pace:       +(pace * 90).toFixed(1), // corners/90 equivalentes
    lineaVivo,                            // línea estimada que la casa ofrece ahora en vivo
    confidence,
  };
}

/**
 * Calcula qué líneas de goles totales tienen valor dado el marcador actual en vivo.
 * Evita recomendar líneas triviales (ya casi seguras o casi imposibles).
 *
 * @param {number} currentGoals  - Goles totales ya marcados en el partido
 * @param {number} elapsed       - Minutos transcurridos
 * @param {number} homeFor       - Promedio histórico goles anotados local
 * @param {number} awayFor       - Promedio histórico goles anotados visitante
 * @returns {object} líneas evaluadas con probabilidades y recomendación
 */
function calcLiveGoalLines(currentGoals, elapsed, homeFor = 1.3, awayFor = 1.0) {
  if (!elapsed || elapsed <= 0) return null;

  const minutesRemaining = Math.max(1, 90 - elapsed);
  const currentPace = currentGoals / elapsed;                     // goles/min actuales
  const historicalRate = (homeFor + awayFor) / 90;               // goles/min históricos

  // Blend: a más minutos jugados, más peso al ritmo actual del partido
  const blendWeight = Math.min(elapsed / 70, 0.65);
  const blendedRate = currentPace * blendWeight + historicalRate * (1 - blendWeight);
  const lambda = blendedRate * minutesRemaining;                  // goles esperados restantes

  // P(X >= k) con distribución Poisson para los goles restantes
  const probAtLeast = (k) => {
    if (lambda <= 0) return k <= 0 ? 1 : 0;
    let cumul = 0;
    for (let i = 0; i < k; i++) cumul += poissonPMF(lambda, i);
    return 1 - cumul;
  };

  const lines = [];
  // Evaluar Over/Under para +0.5, +1.5, +2.5, +3.5 goles restantes
  for (const extra of [0.5, 1.5, 2.5, 3.5]) {
    const totalLine = currentGoals + extra;
    const goalsNeeded = Math.ceil(extra); // cuántos goles más necesita la línea Over
    const pOver  = +(probAtLeast(goalsNeeded) * 100).toFixed(1);
    const pUnder = +(100 - pOver).toFixed(1);

    // Zona de valor: probabilidad entre 20% y 80% → odds entre 1.25 y 5.0
    const overHasValue  = pOver  >= 20 && pOver  <= 80;
    const underHasValue = pUnder >= 20 && pUnder <= 80;

    lines.push({
      linea:        `${totalLine}`,
      overProb:     pOver,
      underProb:    pUnder,
      overValor:    overHasValue,
      underValor:   underHasValue,
      nota:         pOver > 85
        ? `Over ${totalLine} casi garantizado — sin valor en cuota`
        : pOver < 10
        ? `Over ${totalLine} casi imposible — sin valor`
        : `Over ${totalLine} (${pOver}%) | Under ${totalLine} (${pUnder}%)`,
    });
  }

  const lineasConValor = lines.filter(l => l.overValor || l.underValor);

  // Probabilidades vivo ajustadas al marcador actual (para que el LLM no use las de pre-partido)
  const findLine = (extra) => lines.find(l => l.linea === `${currentGoals + extra}`);
  const l05 = findLine(0.5); const l15 = findLine(1.5);
  const l25 = findLine(2.5); const l35 = findLine(3.5);

  return {
    golesActuales:      currentGoals,
    minutosJugados:     elapsed,
    minutosRestantes:   minutesRemaining,
    golesEsperadosRest: +lambda.toFixed(2),
    proyeccionTotal:    +(currentGoals + lambda).toFixed(1),
    // Probs ajustadas al vivo — usar ESTAS, no las de probabilidadesCalculadas
    probsVivo: {
      [`over${currentGoals + 0.5}`]: l05 ? `${l05.overProb}%` : null,
      [`over${currentGoals + 1.5}`]: l15 ? `${l15.overProb}%` : null,
      [`over${currentGoals + 2.5}`]: l25 ? `${l25.overProb}%` : null,
      [`over${currentGoals + 3.5}`]: l35 ? `${l35.overProb}%` : null,
    },
    lineas:             lines,
    lineasConValor:     lineasConValor.length > 0
      ? lineasConValor
      : [{ nota: 'Sin líneas de goles con valor claro en este momento' }],
  };
}

/**
 * Calcula promedios de goles desde los últimos N partidos de todas las competiciones.
 * Se usa como fallback cuando los stats de liga tienen muestra pequeña.
 */
function computeAvgFromFixtures(teamId, fixtures) {
  if (!fixtures || fixtures.length === 0) return null;
  const home = fixtures.filter(f => f.homeId === teamId);
  const away = fixtures.filter(f => f.awayId === teamId);
  return {
    golesAnotadosHome:  home.length > 0 ? +(home.reduce((s, f) => s + f.goalsHome, 0) / home.length).toFixed(2) : null,
    golesRecibidosHome: home.length > 0 ? +(home.reduce((s, f) => s + f.goalsAway, 0) / home.length).toFixed(2) : null,
    golesAnotadosAway:  away.length > 0 ? +(away.reduce((s, f) => s + f.goalsAway, 0) / away.length).toFixed(2) : null,
    golesRecibidosAway: away.length > 0 ? +(away.reduce((s, f) => s + f.goalsHome, 0) / away.length).toFixed(2) : null,
    partidos: fixtures.length,
  };
}

/**
 * Enriquece los datos de un partido con probabilidades calculadas.
 * Se añade el bloque `probabilidades` al objeto de análisis.
 *
 * @param {object} homeStats    - Stats del equipo local (de getTeamStats)
 * @param {object} awayStats    - Stats del equipo visitante (de getTeamStats)
 * @param {Array}  h2h          - Array de H2H (de getH2H)
 * @param {object} homeFallback - Promedios calculados desde últimos 20 partidos (fallback)
 * @param {object} awayFallback - Promedios calculados desde últimos 20 partidos (fallback)
 * @returns {object} bloque de probabilidades para incluir en el prompt
 */
function buildProbBlock(homeStats, awayStats, h2h = [], homeFallback = null, awayFallback = null) {
  if (!homeStats && !homeFallback) return null;
  if (!awayStats && !awayFallback) return null;

  // Si la liga tiene menos de 5 partidos, usa los últimos 20 como base
  const homeGames = homeStats
    ? ((homeStats.victorias?.total || 0) + (homeStats.empates?.total || 0) + (homeStats.derrotas?.total || 0))
    : 0;
  const awayGames = awayStats
    ? ((awayStats.victorias?.total || 0) + (awayStats.empates?.total || 0) + (awayStats.derrotas?.total || 0))
    : 0;

  const useHomeFallback = homeGames < 5 && homeFallback;
  const useAwayFallback = awayGames < 5 && awayFallback;

  const hSrc = useHomeFallback ? homeFallback : homeStats;
  const aSrc = useAwayFallback ? awayFallback : awayStats;

  const hFor  = parseFloat(hSrc?.golesAnotadosHome) || 0;
  const hAgt  = parseFloat(hSrc?.golesRecibidosHome) || 0;
  const aFor  = parseFloat(aSrc?.golesAnotadosAway) || 0;
  const aAgt  = parseFloat(aSrc?.golesRecibidosAway) || 0;

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

  // ── Corners projection ───────────────────────────────────────────────────────
  // local usa sus corners en casa, visitante usa sus corners fuera
  const hCorners = hSrc?.cornersPerGameHome ?? hSrc?.cornersPerGame ?? null;
  const aCorners = aSrc?.cornersPerGameAway ?? aSrc?.cornersPerGame ?? null;
  const cornersProyectados = (hCorners != null && aCorners != null)
    ? +(hCorners + aCorners).toFixed(1) : null;

  // Probabilidades de corners via Poisson (lambda = cornersProyectados)
  let cornersBlock = null;
  if (cornersProyectados != null) {
    const lambda = cornersProyectados;
    const pOver75  = +(poissonCDF_above(lambda, 8)  * 100).toFixed(1);
    const pOver85  = +(poissonCDF_above(lambda, 9)  * 100).toFixed(1);
    const pOver95  = +(poissonCDF_above(lambda, 10) * 100).toFixed(1);
    const pOver105 = +(poissonCDF_above(lambda, 11) * 100).toFixed(1);
    cornersBlock = {
      cornersLocal_PG:    hCorners,
      cornersVisitante_PG: aCorners,
      cornersProyectados,
      probOver75:  `${pOver75}%`,
      probOver85:  `${pOver85}%`,
      probOver95:  `${pOver95}%`,
      probOver105: `${pOver105}%`,
      lineaRecomendada: cornersProyectados >= 10.5 ? 'Over 9.5' :
                        cornersProyectados >= 9.5  ? 'Over 8.5' :
                        cornersProyectados >= 8.5  ? 'Over 7.5' : 'Under 7.5',
    };
  }

  // ── Cards projection ─────────────────────────────────────────────────────────
  const hCards = (hSrc?.tarjetasAmHome ?? 0) + (hSrc?.tarjetasRoHome ?? 0);
  const aCards = (aSrc?.tarjetasAmAway ?? 0) + (aSrc?.tarjetasRoAway ?? 0);
  const cardsProyectadas = (hSrc?.tarjetasAmHome != null || aSrc?.tarjetasAmAway != null)
    ? +(hCards + aCards).toFixed(1) : null;

  let cardsBlock = null;
  if (cardsProyectadas != null) {
    const lc = cardsProyectadas;
    cardsBlock = {
      tarjetasLocal_PG:     hCards > 0 ? +hCards.toFixed(2) : null,
      tarjetasVisitante_PG: aCards > 0 ? +aCards.toFixed(2) : null,
      tarjetasProyectadas:  lc,
      probOver25: `${+(poissonCDF_above(lc, 3) * 100).toFixed(1)}%`,
      probOver35: `${+(poissonCDF_above(lc, 4) * 100).toFixed(1)}%`,
      probOver45: `${+(poissonCDF_above(lc, 5) * 100).toFixed(1)}%`,
    };
  }

  // ── Clean sheets summary ─────────────────────────────────────────────────────
  const csBlock = (hSrc?.cleanSheetsHome != null || aSrc?.cleanSheetsAway != null) ? {
    porteriasACeroLocal_enCasa:       hSrc?.cleanSheetsHome ?? null,
    partidos_sinMarcarLocal_enCasa:   hSrc?.failedToScoreHome ?? null,
    porteriasACeroVisitante_fuera:    aSrc?.cleanSheetsAway ?? null,
    partidos_sinMarcarVisitante_fuera: aSrc?.failedToScoreAway ?? null,
  } : null;

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
    ...(cornersBlock && { proyeccionCorners: cornersBlock }),
    ...(cardsBlock   && { proyeccionTarjetas: cardsBlock }),
    ...(csBlock      && { porteriasACero: csBlock }),
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
    const noScorer = homeGoals === 0 ? 'visitante' : 'local';
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
📊 Prob. de gol: *[X]%* | xG restante: *[X.X]*
💰 Cuota estimada: *~[X.XX]*
⏱️ Actúa antes del min: *[min_límite]*
📈 Por qué: [razón en 1 línea]
🏆 Stake: *[X]/10*
━━━━━━━━━━━━━━━━━━━

CRITERIO DE STAKE PARA ALERTA EN VIVO:
- Stake 8: prob > 72% + cuota > 1.55
- Stake 7: prob 62-72% + cuota > 1.50
- Stake 6: prob 55-62% + cuota > 1.48
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

⛔ PICKS ABSOLUTAMENTE PROHIBIDAS - NUNCA las des:
- Cualquier pick con stake 5 o menor — si no llega a 6/10, NO la publiques. Di "Sin picks de valor."
- Gana el favorito obvio a cuota menor de 1.80 (gana Bayern, gana Madrid, gana City etc)
- Over 2.5 en partidos del Real Madrid, Bayern, City, PSG - todo el mundo lo sabe
- 1X2 simple a cuota menor de 1.75 - no es tipster, es obvio
- Picks que cualquier persona sin conocimiento daría
- BTTS No cuando un equipo ya marcó 2+ goles en el HT

MERCADOS DONDE ESTÁ EL VALOR REAL:
1. HT/FT combos específicos
2. Corners Over/Under
3. Tarjetas Over/Under
4. BTTS cuando ambos marcan en más del 65% de sus partidos
5. Over 3.5 goles cuando ambos tienen promedio goleador alto
6. DNB (Draw No Bet)
7. Asian Handicap
8. HT Over 0.5 o 1.5
9. Gana el visitante cuando el local tiene malos registros en casa

PROCESO DE ANÁLISIS OBLIGATORIO:
Para BTTS: % local marcó en casa + % visitante marcó fuera + % BTTS en H2H. Solo si los 3 superan 55%. Usa también probBTTS_Combinada del modelo Poisson: si supera 62% es señal fuerte.
⛔ BTTS VETO DEFENSIVO: Antes de recomendar BTTS, verifica golesRecibidosHome del equipo local y golesRecibidosAway del visitante. Si cualquiera de las dos defensas recibe < 0.6 goles/partido → BTTS no tiene valor. Si "porteriasACero" existe en el JSON: cleanSheets > 50% de partidos jugados es veto definitivo. Prioriza siempre el dato defensivo sobre el ofensivo del atacante.
Para Corners pre-partido: Si "probabilidadesCalculadas.proyeccionCorners" existe en el JSON, úsalo — contiene cornersProyectados, probOver75/85/95/105 y lineaRecomendada calculados con Poisson real. Cita "cornersProyectados" y la probabilidad exacta del modelo. Si ese bloque NO existe en los datos, PROHIBIDO recomendar corners pre-partido (sin cornersPerGame no hay base matemática — jamás inventes ese número).
Para HT: % local gana 1T en casa. Solo si supera 60%.
Para Tarjetas: Prioridad 1 — estadisticasArbitro del JSON si fuente_stats='statshub': pct_over35_tarjetas, pct_over45_tarjetas, avg_tarjetas. Prioridad 2 — si "probabilidadesCalculadas.proyeccionTarjetas" existe: usa tarjetasProyectadas y probOver25/35/45. Ambos datos son complementarios.
  - pct_over35_tarjetas ≥ 70% → considera Over 3.5 tarjetas
  - pct_over45_tarjetas ≥ 60% → considera Over 4.5 tarjetas
  - pct_btc ≥ 80% → considera BTC (ambos equipos ven tarjeta)
  Si no hay datos StatsHub: usa proyeccionTarjetas.tarjetasProyectadas si existe. Solo recomienda si supera línea en +1.
Para Over/Under goles: usa probOver25 y probOver35 del modelo. Si probOver25 > 65% con EV positivo, considera pick.
Para DNB: usa probDNB_Local o probDNB_Visitante. Solo si supera 72% para stake 7+.

INSTRUCCIONES PARA USAR LAS PROBABILIDADES CALCULADAS:
Si el JSON de datos incluye el campo "probabilidadesCalculadas", DEBES usarlo como base:
- xGLocal / xGVisitante: goles esperados. Si xG local > 1.8 y away < 0.9, el local domina claramente.
- probBTTS_Combinada: combinación de Poisson + H2H. Más fiable que solo H2H.
- expectedValue_vs_CuotasReferencia: si el EV de un mercado es negativo, NO lo recomiendes aunque el porcentaje parezca bueno. Busca mercados con EV > +3%.
- Las probabilidades son calculadas matemáticamente — úsalas para CALIBRAR el stake: si la prob calculada dice 71% pero el análisis cualitativo sugiere 65%, usa 67% como consenso.

INSTRUCCIONES PARA MOMENTUM EN VIVO:
Si el JSON incluye "momentumEnVivo", úsalo para detectar oportunidades en tiempo real:
- Si domina un equipo (score > 15) pero el marcador no lo refleja aún, considera apuesta al próximo gol de ese equipo.
- Si está equilibrado, prioriza mercados de corners o tarjetas sobre resultado.
- proyeccionCorners.lineaVivo: usa esta línea (no 9.5 ni 10.5 de pre-partido) cuando el partido ya comenzó. La casa ajusta la línea al ritmo real del juego.
- proyeccionCorners.projected y remaining: te dicen si hay valor en Over o Under la lineaVivo.
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

CRITERIO DE STAKE ESTRICTO:
10/10: +80% probabilidad, cuota mín 1.85
9/10: +75% probabilidad, cuota mín 1.75
8/10: +70% probabilidad, cuota mín 1.65
7/10: +65% probabilidad, cuota mín 1.55
6/10: +60% probabilidad, cuota mín 1.50
1-5: NUNCA publicar

Si no hay picks con STAKE 6+: "No hay picks de valor en este partido. Mejor no apostar."

FORMATO OBLIGATORIO — sigue este formato exacto, sin variaciones:

🌍 [País] — [Liga]  ← SOLO el nombre de liga que viene en el JSON. NUNCA añadas "(Amistoso Internacional)" ni etiquetas inventadas. Si la liga es "FIFA World Cup", escríbela tal cual.
⚽ [Local] vs [Visitante] | ⏰ [Hora Colombia]
📍 [Estadio, Ciudad]  ← usa partido.estadio y partido.ciudad del JSON. Si son null escribe "No disponible".
🃏 Árbitro: [Nombre] — 🟨 [X.XX/pj] 🟥 [X.XX/pj] | Fouls: [X.XX/pj] | +3.5 tarj: [X]% | +4.5 tarj: [X]% | BTC: [X]%
← usa estadisticasArbitro del JSON. Campos: amarillas_por_partido, rojas_por_partido, faltas_por_partido, pct_over35_tarjetas, pct_over45_tarjetas, pct_btc. Si un campo es null, omítelo. Si fuente_stats='statshub', todos los campos extras vienen de StatsHub.
← Ejemplo con datos completos: "🃏 César Ramos — 🟨 4.14/pj 🟥 0.44/pj | Fouls: 24.0/pj | +3.5 tarj: 67% | +4.5 tarj: 47% | BTC: 77%"
← Ejemplo sin StatsHub: "🃏 Árbitro: John Smith — 🟨 3.20/pj 🟥 0.15/pj"
[Si rankingsFIFA existe en el JSON] 🌐 Ranking FIFA: [Local] [#N] vs [Visitante] [#N]  ← omite esta línea si rankingsFIFA no está en el JSON.
━━━━━━━━━━━━━━━━━━━

📊 *ESTADÍSTICAS CLAVE*
▸ [Local] anota en casa: *X.X* por partido
▸ [Visitante] anota fuera: *X.X* por partido
▸ [Local] recibe en casa: *X.X* por partido
▸ Ambos marcan en H2H: *X de 10* partidos
▸ Forma reciente [Local]: *GGPGE*  ← usa formaLocalSofa si existe, si no usa la de statsLocal
▸ Forma reciente [Visitante]: *PGEGG*  ← usa formaVisitanteSofa si existe, si no usa la de statsVisitante

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
- No uses tablas HTML ni markdown de escritorio (no | columnas |)
- No menciones fuentes de datos, plataformas, APIs ni herramientas
- El pie de página NUNCA debe decir de dónde vienen los datos
- Si no hay picks válidos: escribe solo "⛔ Sin picks de valor hoy en este partido. Mejor no apostar."

Responde en español. NUNCA inventes estadísticas. Usa SOLO los datos que recibes.

⛔ CONTEXTO PROHIBIDO — NUNCA inventes ni asumas:
- NUNCA digas que un equipo es "anfitrión" o "sede" de un torneo a menos que el JSON lo diga explícitamente. El Mundial 2026 es en USA/Canadá/México — Qatar es PARTICIPANTE, NO sede.
- NUNCA menciones tácticas, formaciones, entrenadores ni jugadores específicos a menos que los datos los incluyan.
- NUNCA inventes contexto histórico ("debut memorable", "presión máxima", "favorito histórico") sin datos concretos.
- Si los datos de "statsLocal" o "statsVisitante" tienen pocos partidos en la liga actual, usa "ultimosPartidosLocal" y "ultimosPartidosVisitante" para calcular promedios reales de los últimos 20 partidos entre todas las competiciones.`;

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

MERCADO DE CORNERS EN VIVO — CÓMO USAR LAS PROYECCIONES:
Las casas ajustan la línea de corners en tiempo real. "proyeccionCorners.lineaVivo" es la línea estimada que la casa está ofreciendo ahora mismo (calculada sobre el ritmo actual + córners ya marcados).
- USA "proyeccionCorners.lineaVivo" como la línea a evaluar (no inventes 9.5 si el ritmo no lo justifica).
- Ejemplo: 1 córner al min 45 → lineaVivo ≈ 1.5 o 2.5 (Over o Under según el contexto)
- Si lineaVivo es 2.5 y el contexto del partido (equipo perdiendo, pressing intenso) sugiere que habrá más de 3 córners, entonces tiene valor el Over 2.5.
- Si el partido fue tranquilo en córners, el Under también puede tener valor.
- NUNCA uses la línea de pre-partido (9.5, 10.5) si el ritmo del partido en vivo no la soporta.
- "projected" es la proyección TOTAL al 90' basada en el ritmo actual. "remaining" son los esperados en el tiempo que queda. Ambos te dicen si la línea en vivo tiene valor.

⛔ PROBABILIDADES EN VIVO — REGLA INQUEBRANTABLE:
El campo "probabilidadesCalculadas.modeloPoisson" (probOver25, probBTTS_Poisson, etc.) son probabilidades PRE-PARTIDO calculadas desde históricos. Ya viene marcado con _ADVERTENCIA_ en el JSON.
En partidos en vivo IGNORA esas probabilidades para picks de goles. Usa EXCLUSIVAMENTE:
- "lineasGolesVivo.probsVivo" → probabilidades ajustadas al marcador y minuto actual
- "lineasGolesVivo.lineasConValor" → líneas que tienen valor real ahora mismo
Ejemplo del error a evitar: si "probOver25" del Poisson histórico dice 67.7% pero "probsVivo.over2.5" dice 12%, la cifra correcta para el pick es 12% — y con 12% el pick no existe (stake < 6).

MERCADO DE GOLES EN VIVO — CÓMO USARLO:
1. Leer "lineasGolesVivo.golesEsperadosRest" → goles esperados en el tiempo que queda
2. Leer "lineasGolesVivo.probsVivo" → ya tienes las probabilidades correctas por línea
3. Evaluar "lineasGolesVivo.lineasConValor" → solo recomendar líneas listadas ahí (probabilidad 20-80%)
4. Si "lineasConValor" dice "Sin líneas de goles con valor" → NO fuerces pick de goles; enfócate en corners/tarjetas

⛔ STAKE MÍNIMO 6 — SIN EXCEPCIONES:
Picks con stake 5 o menor NO SE PUBLICAN. Nunca. Ni aunque sea "la única línea disponible" ni aunque la cuota sea interesante. Si no hay 3 picks con stake 6+, emite solo los que lleguen a 6+ aunque sean 1 o 2. NUNCA incluyas "Stake: 5/10".

⛔ MÁXIMO 1 PICK CON STAKE 9 o 10 POR SESIÓN:
Solo UN pick puede ser stake 9 o stake 10 en toda la sesión del día. Los demás tienen tope de stake 8. Para asignar stake 9 o 10 debes citar EXPLÍCITAMENTE la probabilidad del modelo Poisson que lo justifica (ej: "probBTTS_Combinada: 81%") — sin citarla, el máximo es 8. Un stake 10 mal justificado daña la credibilidad del tipster más que no darlo.

⛔ COHERENCIA NARRATIVA ENTRE PICKS:
Si el análisis describe un partido que se abrirá tácticamente (ambos equipos atacan, urgencia táctica, pressing), los picks deben ser coherentes con esa narrativa:
- Partido abierto/atacante → Over corners, Over tarjetas, Over goles (NO Under corners en el mismo análisis)
- Partido cerrado/defensivo → Under corners, Under tarjetas (NO Over goles en el mismo análisis)
Si un pick contradice la narrativa principal, descártalo. Coherencia > cantidad de picks.

⛔ PROYECCIÓN CORNERS — USA LOS DATOS EXACTOS:
El campo "proyeccionCorners" ya viene calculado con el ritmo real del partido. Cita los valores exactos: "projected", "remaining", "lineaVivo". NO ajustes ni inventes valores cualitativos ("factor de presión", "urgencia táctica") para justificar una proyección diferente. Si "proyeccionCorners.projected" = 2.0, la proyección es 2.0, no 3.6.

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

// Gate matemático de stake: compara el stake del LLM contra la prob real calculada.
// Si el LLM infló el stake, lo baja al máximo que los números soportan.
const STAKE_PROB_THRESHOLDS = { 10: 80, 9: 75, 8: 70, 7: 65, 6: 60 };
const STAKE_CUOTA_MIN       = { 10: 1.85, 9: 1.75, 8: 1.65, 7: 1.55, 6: 1.50 };

function validateStake(pick, probBlock) {
  const claimed = pick.stake;
  if (!claimed) return claimed;

  // Corners no tienen probabilidad calculada pre-partido — cap fijo a 7
  if (['CORNERS_OVER', 'CORNERS_UNDER', 'OVER_CORNERS', 'UNDER_CORNERS'].includes(pick.mercado)) {
    return Math.min(claimed, 7);
  }

  if (!probBlock) return claimed;

  const probs = probBlock.modeloPoisson || {};
  const overProb = pick.linea >= 3 ? parseFloat(probs.probOver35) : parseFloat(probs.probOver25);
  const marketProb = {
    BTTS_YES:    parseFloat(probs.probBTTS_Combinada),
    BTTS_NO:     100 - parseFloat(probs.probBTTS_Combinada),
    OVER_GOALS:  overProb,
    UNDER_GOALS: 100 - overProb,
    HOME_WIN:    parseFloat(probs.probLocalGana),
    AWAY_WIN:    parseFloat(probs.probVisitanteGana),
    DRAW:        parseFloat(probs.probEmpate),
  }[pick.mercado];

  if (!marketProb || isNaN(marketProb)) return claimed; // sin datos, confiar en LLM

  // Cuota mínima requerida para el stake reclamado
  const cuotaOk = !pick.cuota || pick.cuota >= (STAKE_CUOTA_MIN[claimed] || 0);

  // Buscar el stake más alto que los números realmente soportan
  for (const [s, threshold] of Object.entries(STAKE_PROB_THRESHOLDS).sort((a,b) => b[0]-a[0])) {
    const stk = parseInt(s);
    if (marketProb >= threshold && cuotaOk) return Math.min(claimed, stk);
    if (marketProb >= threshold) return Math.min(claimed, stk - 1); // prob ok pero cuota baja
  }
  return Math.min(claimed, 6); // mínimo siempre 6 si llegó hasta aquí
}

// Valida y corrige stakes ANTES de publicar el mensaje.
// Ejecuta el gate matemático + regla de máximo 1 stake 9+/10 por sesión.
async function applyStakeGate(picksText, enriched, matchesCtx) {
  try {
    const extracted = await extractPicksFromText(picksText, matchesCtx);
    if (!extracted.length) return picksText;

    let correctedText = picksText;
    let highStakeCount = 0;

    for (const p of extracted.filter(x => !x.esCombinada)) {
      const f = enriched.find(e =>
        e.local?.toLowerCase().includes((p.local || '').toLowerCase().split(' ')[0]) ||
        e.visitante?.toLowerCase().includes((p.visitante || '').toLowerCase().split(' ')[0])
      );

      let stakeValidado = validateStake(p, f?.probabilidadesCalculadas);

      // Máximo 1 pick con stake 9 o 10 por sesión
      if (stakeValidado >= 9) {
        if (highStakeCount > 0) {
          console.log(`⚠️ Stake cap: ${p.local} vs ${p.visitante} reducido ${stakeValidado}→8 (ya hay un pick 9+)`);
          stakeValidado = 8;
        } else {
          highStakeCount++;
        }
      }

      if (stakeValidado !== p.stake) {
        console.log(`⚠️ Pre-publish gate: ${p.local} vs ${p.visitante} (${p.mercado}): ${p.stake}→${stakeValidado}`);
        correctedText = correctedText.replace(`Stake: ${p.stake}/10`, `Stake: ${stakeValidado}/10`);
      }
    }

    return correctedText;
  } catch (e) {
    console.error('applyStakeGate:', e.message);
    return picksText; // fail-open: publicar sin corrección antes que crashear
  }
}

async function recordPicks(analysisText, matchesCtx, enrichedFixtures = []) {
  if (!analysisText || !matchesCtx.length) return;
  try {
    const extracted = await extractPicksFromText(analysisText, matchesCtx);
    if (!extracted.length) { console.log('📝 No se extrajeron picks estructurados'); return; }

    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });

    const newPicks = extracted.map(p => {
      const matched = matchesCtx.find(m =>
        m.local?.toLowerCase().includes((p.local || '').toLowerCase().split(' ')[0]) ||
        m.visitante?.toLowerCase().includes((p.visitante || '').toLowerCase().split(' ')[0]) ||
        (p.local || '').toLowerCase().includes((m.local || '').toLowerCase().split(' ')[0])
      );
      // Validar stake contra probabilidades reales si disponibles
      const enriched = enrichedFixtures.find(f => f.fixtureId === matched?.fixtureId);
      const stakeValidado = validateStake(p, enriched?.probabilidadesCalculadas);
      if (stakeValidado !== p.stake) {
        console.log(`⚠️ Stake ajustado: ${p.mercado} ${p.local} vs ${p.visitante}: ${p.stake} → ${stakeValidado}`);
      }
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
        stake: stakeValidado ?? p.stake ?? null,
        stake_valid: stakeValidado,
        esCombinada: p.esCombinada || false,
        resultado: null,
        scoresFinal: null,
      };
    });

    // Guardar en Airtable (persistente) y en picks.json (fallback local)
    persistPicks([...loadPicks(), ...newPicks]);
    await savePicksToAirtable(newPicks);
    console.log(`📝 ${newPicks.length} picks guardados (Airtable + local)`);
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
  // Leer de Airtable si disponible, fallback a picks.json local
  let picks;
  let fromAirtable = false;
  if (process.env.AIRTABLE_API_KEY) {
    picks = await getPicksFromAirtable('total');
    fromAirtable = true;
  } else {
    picks = loadPicks();
  }
  const pending = picks.filter(p => (p.resultado === null || p.resultado === '?') && p.fixtureId);
  if (!pending.length) return picks;

  const fixtureIds = [...new Set(pending.map(p => p.fixtureId))];
  const fixtureMap = {};

  await Promise.allSettled(fixtureIds.map(async (fid) => {
    const pick = pending.find(p => p.fixtureId === fid);
    if (!pick?.fechaPartido) return;
    const date = pick.fechaPartido.split('T')[0];
    try {
      const allMatches = await fetchFixturesByDate(date);
      const m = allMatches.find(match => match.id === fid);
      if (m && FINISHED_DESCS.has(m.state?.description)) {
        const f = hlToApif(m);
        const stats = await getFixtureStatistics(fid).catch(() => null);
        fixtureMap[fid] = { fixture: f, stats };
      }
    } catch {}
  }));

  for (const pick of picks) {
    if (!['?', null].includes(pick.resultado) || !pick.fixtureId) continue;
    const entry = fixtureMap[pick.fixtureId];
    if (!entry) continue;
    pick.resultado = await evaluatePickResult(pick, entry.fixture, entry.stats);
    pick.scoresFinal = { home: entry.fixture.goals?.home, away: entry.fixture.goals?.away };
    console.log(`📊 Pick evaluado: ${pick.local} vs ${pick.visitante} — ${pick.seleccion} → ${pick.resultado}`);
    if (fromAirtable) {
      await updatePickResultInAirtable(pick._airtableId, pick.resultado, pick.scoresFinal);
    }
  }

  if (!fromAirtable) persistPicks(picks);
  return picks;
}

async function handleEstadisticas(chatId, period = 'hoy') {
  await bot.sendMessage(chatId, '📊 Consultando base de datos de picks...');

  await evaluatePendingPicks(); // actualiza resultados pendientes
  const allPicks = process.env.AIRTABLE_API_KEY
    ? await getPicksFromAirtable(period)
    : (() => {
        const all = loadPicks();
        const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
        const ayer = new Date(); ayer.setDate(ayer.getDate()-1);
        const ayerStr = ayer.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
        const semana = new Date(); semana.setDate(semana.getDate()-7);
        if (period === 'hoy')   return all.filter(p => p.fecha === today);
        if (period === 'ayer')  return all.filter(p => p.fecha === ayerStr);
        if (period === 'semana') return all.filter(p => new Date(p.fecha) >= semana);
        return all;
      })();
  const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
  const ayer = new Date(); ayer.setDate(ayer.getDate() - 1);
  const ayerStr = ayer.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });

  // allPicks is already filtered by period (both Airtable and local paths pre-filter)
  const filtered = allPicks;

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
    if (i + 4 < statsPairs.length) await new Promise(r => setTimeout(r, 1000));
  }

  // SofaScore: 1 llamada para todos los eventos del día (en caché)
  const sofaEventsHoy = await fetchSofaScoreEvents(today).catch(() => []);

  const enrichedRaw = selected.map((f, i) => {
    const homeStats = statsResults[i * 2].status === 'fulfilled' ? statsResults[i * 2].value : null;
    const awayStats = statsResults[i * 2 + 1].status === 'fulfilled' ? statsResults[i * 2 + 1].value : null;
    const probBlock = buildProbBlock(homeStats, awayStats, []);
    return { f, homeStats, awayStats, probBlock };
  });

  // Obtener contexto SofaScore (árbitro, rankings, forma) para cada partido en paralelo
  const sofaContexts = await Promise.allSettled(
    enrichedRaw.map(({ f }) => getSofaMatchContext(f.homeTeam, f.awayTeam, sofaEventsHoy))
  );

  const enriched = enrichedRaw.map(({ f, homeStats, awayStats, probBlock }, i) => {
    const sofa = sofaContexts[i].status === 'fulfilled' ? sofaContexts[i].value : null;
    return {
      fixtureId:      f.fixtureId,
      liga:           f.leagueName,
      local:          f.homeTeam,
      visitante:      f.awayTeam,
      hora:           formatHour(f.date),
      fechaPartido:   f.date,
      estadio:        f.venue?.name  || null,
      ciudad:         f.venue?.city  || null,
      arbitro:        sofa?.arbitro?.nombre || null,
      statsLocal:     homeStats,
      statsVisitante: awayStats,
      ...(probBlock         && { probabilidadesCalculadas: probBlock }),
      ...(sofa?.arbitro     && { estadisticasArbitro: sofa.arbitro }),
      ...(sofa?.rankingsFIFA && { rankingsFIFA: sofa.rankingsFIFA }),
      ...(sofa?.formaLocal      && { formaLocalSofa: sofa.formaLocal }),
      ...(sofa?.formaVisitante  && { formaVisitanteSofa: sofa.formaVisitante }),
    };
  });

  await bot.sendMessage(chatId, `🧠 Calculando picks de valor...`);

  const winRates = process.env.AIRTABLE_API_KEY ? await getHistoricalWinRates().catch(() => null) : null;
  const winRatesCtx = winRates
    ? `\n\nCONTEXTO HISTÓRICO DE RENDIMIENTO (aprende de esto para calibrar stakes):\n${JSON.stringify(winRates, null, 2)}\n— Si un mercado tiene winRate < 45%, sé más conservador con el stake.\n— Si un mercado tiene winRate ≥ 60%, puedes ser más agresivo.`
    : '';

  const picksText = await sonnet(
    PICKS_HOY_SYSTEM,
    `Partidos del día ${today} (hora Colombia). DATOS REALES — HIGHLIGHTLY + SOFASCORE:\n\n${JSON.stringify(enriched, null, 2)}${winRatesCtx}\n\nEmite EXACTAMENTE 3 picks individuales + 1 combinada basadas SOLO en estos datos reales. Usa las probabilidadesCalculadas para validar cada pick — solo recomienda si el EV es positivo o cercano a 0 y la prob supera el umbral de stake.`
  );

  const matchesCtxHoy = enriched.map(f => ({ fixtureId: f.fixtureId, local: f.local, visitante: f.visitante, liga: f.liga, fechaPartido: f.fechaPartido }));
  const finalText = await applyStakeGate(picksText, enriched, matchesCtxHoy);

  // Guardar en caché para evitar re-análisis y picks contradictorios
  setPicksCache('all', finalText, enriched.map(f => f.fixtureId));

  await sendLong(chatId, `📅 *PICKS DEL DÍA — ${today}*\n\n${finalText}`, { parse_mode: 'Markdown' });
  recordPicks(finalText, matchesCtxHoy, enriched).catch(e => console.error('recordPicks:', e.message));
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
    if (i + 4 < statsPairs.length) await new Promise(r => setTimeout(r, 1000));
  }

  const sofaEventsLiga = await fetchSofaScoreEvents(today).catch(() => []);

  const enrichedRawLiga = fixtures.map((f, i) => {
    const homeStats = statsResults[i * 2]?.status === 'fulfilled' ? statsResults[i * 2].value : null;
    const awayStats = statsResults[i * 2 + 1]?.status === 'fulfilled' ? statsResults[i * 2 + 1].value : null;
    const probBlock = buildProbBlock(homeStats, awayStats, []);
    return { f, homeStats, awayStats, probBlock };
  });

  const sofaContextsLiga = await Promise.allSettled(
    enrichedRawLiga.map(({ f }) => getSofaMatchContext(f.homeTeam, f.awayTeam, sofaEventsLiga))
  );

  const enriched = enrichedRawLiga.map(({ f, homeStats, awayStats, probBlock }, i) => {
    const sofa = sofaContextsLiga[i].status === 'fulfilled' ? sofaContextsLiga[i].value : null;
    return {
      fixtureId:      f.fixtureId,
      liga:           f.leagueName,
      local:          f.homeTeam,
      visitante:      f.awayTeam,
      hora:           formatHour(f.date),
      fechaPartido:   f.date,
      estadio:        f.venue?.name || null,
      ciudad:         f.venue?.city || null,
      arbitro:        sofa?.arbitro?.nombre || f.referee || null,
      statsLocal:     homeStats,
      statsVisitante: awayStats,
      ...(probBlock             && { probabilidadesCalculadas: probBlock }),
      ...(sofa?.arbitro         && { estadisticasArbitro: sofa.arbitro }),
      ...(sofa?.rankingsFIFA    && { rankingsFIFA: sofa.rankingsFIFA }),
      ...(sofa?.formaLocal      && { formaLocalSofa: sofa.formaLocal }),
      ...(sofa?.formaVisitante  && { formaVisitanteSofa: sofa.formaVisitante }),
    };
  });

  await bot.sendMessage(chatId, `🧠 Calculando picks de valor...`);

  const winRates = process.env.AIRTABLE_API_KEY ? await getHistoricalWinRates().catch(() => null) : null;
  const winRatesCtx = winRates
    ? `\n\nCONTEXTO HISTÓRICO DE RENDIMIENTO (aprende de esto para calibrar stakes):\n${JSON.stringify(winRates, null, 2)}\n— Si un mercado tiene winRate < 45%, sé más conservador con el stake.\n— Si un mercado tiene winRate ≥ 60%, puedes ser más agresivo.`
    : '';

  const picksText = await sonnet(
    PICKS_HOY_SYSTEM,
    `Partidos de ${displayName} del día ${today}. DATOS REALES DE API + SOFASCORE:\n\n${JSON.stringify(enriched, null, 2)}${winRatesCtx}\n\nAnaliza y emite picks de valor basadas SOLO en estos datos reales. Usa las probabilidadesCalculadas para validar cada pick — solo recomienda si el EV es positivo o cercano a 0 y la prob supera el umbral de stake.`
  );

  const matchesCtxLiga = enriched.map(f => ({ fixtureId: f.fixtureId, local: f.local, visitante: f.visitante, liga: f.liga, fechaPartido: f.fechaPartido }));
  const finalTextLiga = await applyStakeGate(picksText, enriched, matchesCtxLiga);

  // Guardar en caché para evitar re-análisis y picks contradictorios
  setPicksCache(cacheScope, finalTextLiga, enriched.map(f => f.fixtureId));

  await sendLong(chatId, `📅 *${displayName} — ${today}*\n\n${finalTextLiga}`, { parse_mode: 'Markdown' });
  recordPicks(finalTextLiga, matchesCtxLiga, enriched).catch(e => console.error('recordPicks:', e.message));
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
    getTeamLastFixtures(homeId, 20),
    getTeamLastFixtures(awayId, 20),
  ];
  if (isLive) requests.push(getFixtureStatistics(nextRaw.fixture.id));

  const [h2hRes, homeStatsRes, awayStatsRes, homeFixturesRes, awayFixturesRes, liveStatsRes] = await Promise.allSettled(requests);

  const h2hData          = h2hRes.status === 'fulfilled'          ? h2hRes.value          : [];
  const homeStatsData    = homeStatsRes.status === 'fulfilled'    ? homeStatsRes.value    : null;
  const awayStatsData    = awayStatsRes.status === 'fulfilled'    ? awayStatsRes.value    : null;
  const homeLastFixtures = homeFixturesRes.status === 'fulfilled' ? homeFixturesRes.value : [];
  const awayLastFixtures = awayFixturesRes.status === 'fulfilled' ? awayFixturesRes.value : [];
  const liveStatsData    = (isLive && liveStatsRes?.status === 'fulfilled') ? liveStatsRes.value : null;

  // Calcular probabilidades con modelo de Poisson (fallback a últimos 20 si liga tiene < 5 partidos)
  const homeFallback = computeAvgFromFixtures(homeId, homeLastFixtures);
  const awayFallback = computeAvgFromFixtures(awayId, awayLastFixtures);
  const probBlock = buildProbBlock(homeStatsData, awayStatsData, h2hData, homeFallback, awayFallback);

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

  // Líneas de goles con valor real dado el marcador actual (solo en vivo)
  const currentGoalsTotal = isLive
    ? (nextRaw.goals?.home ?? 0) + (nextRaw.goals?.away ?? 0)
    : 0;
  const goalLinesProj = isLive && elapsed > 0
    ? calcLiveGoalLines(currentGoalsTotal, elapsed)
    : null;

  // Árbitro stats desde SofaScore (pre-calculados)
  const fixtureDate = nextRaw.fixture.date.split('T')[0];
  const sofaEvents  = await fetchSofaScoreEvents(fixtureDate).catch(() => []);
  const sofaCtx     = await getSofaMatchContext(homeTeam, awayTeam, sofaEvents).catch(() => null);

  const analysisData = {
    partido: {
      liga:      nextRaw.league.name,
      pais:      nextRaw.league.country,
      fecha:     fixtureDate,
      hora:      formatHour(nextRaw.fixture.date),
      local:     homeTeam,
      visitante: awayTeam,
      estadio:   nextRaw.fixture.venue?.name  || null,
      ciudad:    nextRaw.fixture.venue?.city  || null,
      arbitro:   sofaCtx?.arbitro?.nombre || nextRaw.fixture.referee || null,
      enVivo:    isLive,
      minuto:    elapsed || null,
      marcador:  isLive ? `${nextRaw.goals?.home ?? 0}-${nextRaw.goals?.away ?? 0}` : null,
    },
    ...(sofaCtx?.arbitro         && { estadisticasArbitro: sofaCtx.arbitro }),
    ...(sofaCtx?.rankingsFIFA    && { rankingsFIFA: sofaCtx.rankingsFIFA }),
    ...(sofaCtx?.formaLocal      && { formaLocalSofa: sofaCtx.formaLocal }),
    ...(sofaCtx?.formaVisitante  && { formaVisitanteSofa: sofaCtx.formaVisitante }),
    h2h:            h2hData,
    bttsEnH2H:      h2hData.filter(m => m.btts).length,
    statsLocal:     homeStatsData,
    statsVisitante: awayStatsData,
    ultimosPartidosLocal:     homeLastFixtures,
    ultimosPartidosVisitante: awayLastFixtures,
    estadisticasVivo: liveStatsData,
    ...(probBlock && {
      probabilidadesCalculadas: isLive
        ? { ...probBlock, _ADVERTENCIA_: 'DATOS PRE-PARTIDO basados en históricos. En vivo NO uses probOver25 ni probBTTS_Poisson para picks de goles — usa SOLO lineasGolesVivo.lineasConValor que ya tiene las probabilidades ajustadas al marcador y minuto actual.' }
        : probBlock,
    }),
    ...(momentum      && { momentumEnVivo: momentum }),
    ...(cornersProj   && { proyeccionCorners: cornersProj }),
    ...(cardsProj     && { proyeccionTarjetas: cardsProj }),
    ...(goalLinesProj && { lineasGolesVivo: goalLinesProj }),
  };

  await bot.sendMessage(chatId, '⚡ Procesando análisis profesional...');
  const system = isLive ? INPLAY_SYSTEM : TIPSTER_SYSTEM;
  const season = LEAGUE_SEASONS[leagueId] || 2025;
  const analysis = await sonnet(
    system,
    `Analiza este partido con DATOS REALES (temporada ${season}):\n\n${JSON.stringify(analysisData, null, 2)}`
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

  const sofaEventsVivo = await fetchSofaScoreEvents(todayDate()).catch(() => []);
  const sofaContextsVivo = await Promise.allSettled(
    toAnalyze.map(f => getSofaMatchContext(f.homeTeam, f.awayTeam, sofaEventsVivo))
  );

  const enriched = toAnalyze.map((f, i) => {
    const liveStats = statsResults[i].status === 'fulfilled' ? statsResults[i].value : null;
    const sofa      = sofaContextsVivo[i].status === 'fulfilled' ? sofaContextsVivo[i].value : null;
    const elapsed   = f.elapsed || 0;

    const momentum = calcLiveMomentum(liveStats, f.homeTeam, f.awayTeam);

    const homeCorners = liveStats ? (Object.values(liveStats)[0]?.['Corner Kicks'] ?? 0) : 0;
    const awayCorners = liveStats ? (Object.values(liveStats)[1]?.['Corner Kicks'] ?? 0) : 0;
    const cornersProj = elapsed > 0 ? calcLiveProjection(homeCorners + awayCorners, elapsed) : null;

    const homeCards = liveStats
      ? ((Object.values(liveStats)[0]?.['Yellow Cards'] ?? 0) + (Object.values(liveStats)[0]?.['Red Cards'] ?? 0))
      : 0;
    const awayCards = liveStats
      ? ((Object.values(liveStats)[1]?.['Yellow Cards'] ?? 0) + (Object.values(liveStats)[1]?.['Red Cards'] ?? 0))
      : 0;
    const cardsProj = elapsed > 0 ? calcLiveProjection(homeCards + awayCards, elapsed) : null;

    const currentGoals  = (f.homeGoals ?? 0) + (f.awayGoals ?? 0);
    const goalLinesProj = elapsed > 0 ? calcLiveGoalLines(currentGoals, elapsed) : null;

    return {
      ...f,
      marcador:         `${f.homeGoals ?? 0}-${f.awayGoals ?? 0}`,
      estadio:          f.venue?.name || null,
      ciudad:           f.venue?.city || null,
      arbitro:          sofa?.arbitro?.nombre || f.referee || null,
      estadisticasVivo: liveStats,
      ...(sofa?.arbitro      && { estadisticasArbitro: sofa.arbitro }),
      ...(sofa?.rankingsFIFA && { rankingsFIFA: sofa.rankingsFIFA }),
      ...(momentum      && { momentumEnVivo: momentum }),
      ...(cornersProj   && { proyeccionCorners: cornersProj }),
      ...(cardsProj     && { proyeccionTarjetas: cardsProj }),
      ...(goalLinesProj && { lineasGolesVivo: goalLinesProj }),
    };
  });

  await bot.sendMessage(chatId, '🎯 Identificando picks de valor...');
  const analysis = await sonnet(
    INPLAY_SYSTEM,
    `DATOS REALES EN VIVO — Highlightly + SofaScore:\n\n${JSON.stringify(enriched, null, 2)}\n\nAnaliza y da picks de valor in-play para los mejores partidos.`
  );
  await sendLong(chatId, `🔴 *PICKS EN VIVO${leagueName ? ' — ' + leagueName : ''}*\n\n${analysis}`, { parse_mode: 'Markdown' });
  recordPicks(analysis, enriched.map(f => ({ fixtureId: f.fixtureId, local: f.homeTeam, visitante: f.awayTeam, liga: f.leagueName, fechaPartido: f.date }))).catch(e => console.error('recordPicks:', e.message));
}

// ─── Alerta de Gol ────────────────────────────────────────────────────────────

async function handleAlertaGol(chatId) {
  await bot.sendMessage(chatId, '⚡ Escaneando partidos en vivo en busca de oportunidades de gol...');

  // 1. Obtener todos los partidos en vivo (formato Highlightly raw)
  const liveRaw = await fetchLiveRaw();
  const liveActive = liveRaw.filter(f =>
    ['First half', 'Second half', 'Extra time'].includes(f.state?.description) &&
    LEAGUE_IDS.has(f.league?.id)
  );

  if (liveActive.length === 0) {
    return bot.sendMessage(chatId, '😔 No hay partidos activos ahora mismo en las ligas monitoreadas.');
  }

  await bot.sendMessage(chatId, `🔍 *${liveActive.length}* partido(s) activo(s). Calculando probabilidades de gol...`, { parse_mode: 'Markdown' });

  // 2. Obtener stats en vivo + históricas en paralelo (máx 6 partidos)
  const candidates = liveActive.slice(0, 6);
  const [liveStatsResults, homeStatsResults, awayStatsResults] = await Promise.all([
    Promise.allSettled(candidates.map(f => getFixtureStatistics(f.id))),
    Promise.allSettled(candidates.map(f => getTeamStats(f.homeTeam.id, f.league.id))),
    Promise.allSettled(candidates.map(f => getTeamStats(f.awayTeam.id, f.league.id))),
  ]);

  // 3. Calcular alerta de gol para cada partido
  const alerts = [];
  for (let i = 0; i < candidates.length; i++) {
    const f = candidates[i];
    const parsed = parseFixture(f); // parseFixture handles Highlightly format
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

  const fixtureDate2 = nextRaw.fixture.date.split('T')[0];
  const [h2hRes, homeStatsRes, awayStatsRes, sofaEventsRes2] = await Promise.allSettled([
    getH2H(homeId, awayId),
    getTeamStats(homeId, leagueId),
    getTeamStats(awayId, leagueId),
    fetchSofaScoreEvents(fixtureDate2),
  ]);

  const h2hData2       = h2hRes.status === 'fulfilled' ? h2hRes.value : [];
  const homeStatsData2 = homeStatsRes.status === 'fulfilled' ? homeStatsRes.value : null;
  const awayStatsData2 = awayStatsRes.status === 'fulfilled' ? awayStatsRes.value : null;
  const sofaEvs2       = sofaEventsRes2.status === 'fulfilled' ? sofaEventsRes2.value : [];
  const sofaCtx2       = await getSofaMatchContext(homeTeam, awayTeam, sofaEvs2).catch(() => null);
  const probBlock2     = buildProbBlock(homeStatsData2, awayStatsData2, h2hData2);

  const analysisData = {
    partido: {
      liga:      nextRaw.league.name,
      pais:      nextRaw.league.country,
      fecha:     fixtureDate2,
      hora:      formatHour(nextRaw.fixture.date),
      local:     homeTeam,
      visitante: awayTeam,
      estadio:   nextRaw.fixture.venue?.name || null,
      ciudad:    nextRaw.fixture.venue?.city || null,
      arbitro:   sofaCtx2?.arbitro?.nombre || nextRaw.fixture.referee || null,
    },
    ...(sofaCtx2?.arbitro      && { estadisticasArbitro: sofaCtx2.arbitro }),
    ...(sofaCtx2?.rankingsFIFA && { rankingsFIFA: sofaCtx2.rankingsFIFA }),
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
      ? 'Datos históricos disponibles.'
      : 'No se encontraron datos externos. Analiza SOLO con datos de la imagen. Indica "Análisis basado solo en estadísticas visibles".';

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

// ── Admin: debug estructura raw de stats de Highlightly ──────────────────────
// Uso: /debugstats Real Madrid   (solo funciona para ADMIN_IDS)
bot.onText(/^\/debugstats(?:\s+(.+))?$/i, async (msg, match) => {
  const chatId     = msg.chat.id;
  const telegramId = String(msg.from?.id || chatId);
  if (!ADMIN_IDS.has(telegramId)) return;

  const teamQuery = (match[1] || '').trim();
  if (!teamQuery) {
    return bot.sendMessage(chatId, '⚠️ Uso: /debugstats [nombre del equipo]\nEjemplo: /debugstats Real Madrid');
  }

  await bot.sendMessage(chatId, `🔍 Buscando "${teamQuery}" en Highlightly...`);

  try {
    const teamData = await searchTeam(teamQuery);
    if (!teamData) {
      return bot.sendMessage(chatId, `❌ No encontré el equipo "${teamQuery}".`);
    }

    const teamId = teamData.team.id;
    await bot.sendMessage(chatId, `✅ Equipo: *${teamData.team.name}* (ID: ${teamId})\n🔍 Obteniendo stats...`, { parse_mode: 'Markdown' });

    const { data } = await API.get('/teams/statistics/' + teamId, { params: { fromDate: '2024-06-01' } });
    const first = Array.isArray(data) ? data[0] : data;

    if (!first) return bot.sendMessage(chatId, '❌ Sin datos de stats para ese equipo.');

    const home = first.home || {};
    const result = [
      `📊 *Estructura raw — ${teamData.team.name}*`,
      `Liga: ${first.leagueName || 'N/D'} | Temporada: ${first.season || 'N/D'}`,
      ``,
      `*Claves en home:* \`${Object.keys(home).join(', ')}\``,
      ``,
      `*home.games:* \`${JSON.stringify(home.games || {})}\``,
      `*home.goals:* \`${JSON.stringify(home.goals || {})}\``,
      `*home.corners:* \`${JSON.stringify(home.corners ?? 'N/A')}\``,
      `*home.cards:* \`${JSON.stringify(home.cards ?? 'N/A')}\``,
      `*home.shots:* \`${JSON.stringify(home.shots ?? 'N/A')}\``,
      `*home.possession:* \`${JSON.stringify(home.possession ?? 'N/A')}\``,
    ].join('\n');

    await sendLong(chatId, result, { parse_mode: 'Markdown' });
  } catch (e) {
    bot.sendMessage(chatId, `❌ Error: ${e.message}`);
  }
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

// ── Airtable Picks — tabla persistente de picks ───────────────────────────────
// Se crea automáticamente si no existe. Sobrevive deploys (a diferencia de picks.json).

const PICKS_TABLE = 'Picks';

async function ensurePicksTable() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return;
  try {
    const res = await axios.get(
      `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
      { headers: { Authorization: `Bearer ${apiKey}` }, timeout: 10000 }
    );
    if (res.data.tables.some(t => t.name === PICKS_TABLE)) {
      console.log('✅ Airtable Picks table ya existe');
      return;
    }
    await axios.post(
      `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
      {
        name: PICKS_TABLE,
        fields: [
          { name: 'pick_id',      type: 'singleLineText' },
          { name: 'emitidoAt',    type: 'singleLineText' },
          { name: 'fecha',        type: 'singleLineText' },
          { name: 'liga',         type: 'singleLineText' },
          { name: 'local',        type: 'singleLineText' },
          { name: 'visitante',    type: 'singleLineText' },
          { name: 'mercado',      type: 'singleLineText' },
          { name: 'seleccion',    type: 'singleLineText' },
          { name: 'linea',        type: 'number', options: { precision: 2 } },
          { name: 'cuota',        type: 'number', options: { precision: 2 } },
          { name: 'stake',        type: 'number', options: { precision: 0 } },
          { name: 'stake_valid',  type: 'number', options: { precision: 0 } },
          { name: 'prob_calc',    type: 'number', options: { precision: 1 } },
          { name: 'resultado',    type: 'singleLineText' },
          { name: 'esCombinada',  type: 'checkbox', options: { color: 'yellowBright', icon: 'check' } },
          { name: 'fixtureId',    type: 'number', options: { precision: 0 } },
          { name: 'scoresFinal',  type: 'singleLineText' },
        ],
      },
      { headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' }, timeout: 15000 }
    );
    console.log('✅ Airtable Picks table creada automáticamente');
  } catch (e) {
    console.warn('⚠️ ensurePicksTable:', e.response?.data?.error?.message || e.message);
  }
}

function pickToAirtableFields(p) {
  return {
    pick_id:     p.id,
    emitidoAt:   p.emitidoAt || new Date().toISOString(),
    fecha:       p.fecha || '',
    liga:        p.liga || '',
    local:       p.local || '',
    visitante:   p.visitante || '',
    mercado:     p.mercado || '',
    seleccion:   p.seleccion || '',
    linea:       p.linea ?? null,
    cuota:       p.cuota ?? null,
    stake:       p.stake ?? null,
    stake_valid: p.stake_valid ?? p.stake ?? null,
    prob_calc:   p.prob_calc ?? null,
    resultado:   p.resultado || '?',
    esCombinada: p.esCombinada || false,
    fixtureId:   p.fixtureId ?? null,
    scoresFinal: p.scoresFinal ? JSON.stringify(p.scoresFinal) : '',
  };
}

async function savePicksToAirtable(picks) {
  if (!picks.length || !process.env.AIRTABLE_API_KEY) return;
  const base = getAirtableBase();
  // Airtable acepta max 10 records por llamada
  for (let i = 0; i < picks.length; i += 10) {
    const batch = picks.slice(i, i + 10).map(p => ({ fields: pickToAirtableFields(p) }));
    await base(PICKS_TABLE).create(batch).catch(e => console.error('savePicksToAirtable batch:', e.message));
  }
}

async function getPicksFromAirtable(period = 'total') {
  if (!process.env.AIRTABLE_API_KEY) return [];
  try {
    const base = getAirtableBase();
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
    const ayer  = new Date(); ayer.setDate(ayer.getDate() - 1);
    const ayerStr = ayer.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
    const semana  = new Date(); semana.setDate(semana.getDate() - 7);
    const semanaStr = semana.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });

    let formula = '';
    if (period === 'hoy')   formula = `{fecha} = '${today}'`;
    else if (period === 'ayer')   formula = `{fecha} = '${ayerStr}'`;
    else if (period === 'semana') formula = `{fecha} >= '${semanaStr}'`;
    else if (period === 'mes')    {
      const mes = new Date(); mes.setDate(mes.getDate() - 30);
      formula = `{fecha} >= '${mes.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' })}'`;
    }

    const records = await base(PICKS_TABLE).select({
      ...(formula && { filterByFormula: formula }),
      sort: [{ field: 'emitidoAt', direction: 'desc' }],
      maxRecords: 1000,
    }).all();
    return records.map(r => ({ _airtableId: r.id, ...r.fields }));
  } catch (e) {
    console.error('getPicksFromAirtable:', e.message);
    return [];
  }
}

async function updatePickResultInAirtable(airtableId, resultado, scoresFinal) {
  if (!process.env.AIRTABLE_API_KEY || !airtableId) return;
  try {
    const base = getAirtableBase();
    await base(PICKS_TABLE).update(airtableId, {
      resultado,
      scoresFinal: scoresFinal ? JSON.stringify(scoresFinal) : '',
    });
  } catch (e) {
    console.error('updatePickResultInAirtable:', e.message);
  }
}

async function getHistoricalWinRates() {
  try {
    const picks = await getPicksFromAirtable('total');
    const resolved = picks.filter(p => ['W', 'L', 'V', 'P'].includes(p.resultado));
    if (!resolved.length) return null;

    const byMercado = {};
    for (const p of resolved) {
      const m = p.mercado || 'OTHER';
      if (!byMercado[m]) byMercado[m] = { w: 0, total: 0 };
      byMercado[m].total++;
      if (['W', 'V'].includes(p.resultado)) byMercado[m].w++;
    }

    const wins = resolved.filter(p => ['W', 'V'].includes(p.resultado)).length;
    return {
      total: resolved.length,
      winRate: +((wins / resolved.length) * 100).toFixed(1),
      porMercado: Object.fromEntries(
        Object.entries(byMercado)
          .filter(([, v]) => v.total >= 5)
          .map(([k, v]) => [k, { picks: v.total, winRate: +((v.w / v.total) * 100).toFixed(1) }])
      ),
    };
  } catch (e) {
    console.error('getHistoricalWinRates:', e.message);
    return null;
  }
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

// ── Debug: inspeccionar raw de estadísticas de un equipo en Highlightly ──────
// GET /admin/debug-team-stats/:teamId   Header: X-Admin-Key
// Devuelve la estructura raw del endpoint /teams/statistics para ver qué campos existen.
app.get('/admin/debug-team-stats/:teamId', async (req, res) => {
  const key = req.headers['x-admin-key'];
  if (process.env.ADMIN_KEY && key !== process.env.ADMIN_KEY) {
    return res.status(401).json({ error: 'No autorizado' });
  }
  try {
    const { data } = await API.get('/teams/statistics/' + req.params.teamId, {
      params: { fromDate: '2024-06-01' },
    });
    const first = Array.isArray(data) ? data[0] : data;
    res.json({
      leagueName: first?.leagueName,
      home_keys:  Object.keys(first?.home || {}),
      away_keys:  Object.keys(first?.away || {}),
      total_keys: Object.keys(first?.total || {}),
      home_sample: first?.home,
      raw_count: Array.isArray(data) ? data.length : 1,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── StatsHub — refresh manual de árbitros ────────────────────────────────────
// Fuerza un re-fetch inmediato desde la API. Protegido con X-Admin-Key.
app.post('/webhook/arbitros', async (req, res) => {
  const key = req.headers['x-admin-key'];
  if (process.env.ADMIN_KEY && key !== process.env.ADMIN_KEY) {
    return res.status(401).json({ error: 'No autorizado' });
  }
  await fetchStatsHubReferees();
  res.json({ ok: true, arbitros: statsHubReferees.length });
});

app.listen(WEBHOOK_PORT, () => {
  console.log(`🌐 Webhook server escuchando en puerto ${WEBHOOK_PORT}`);
  console.log(`   POST http://localhost:${WEBHOOK_PORT}/webhook/whop`);
});

// Auto-fetch árbitros de StatsHub al arrancar y cada 6 horas
fetchStatsHubReferees();
setInterval(fetchStatsHubReferees, 6 * 60 * 60 * 1000);
if (process.env.AIRTABLE_API_KEY) ensurePicksTable().catch(e => console.error('ensurePicksTable:', e.message));
