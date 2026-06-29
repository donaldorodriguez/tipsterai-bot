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

const bot = new TelegramBot(process.env.TELEGRAM_TOKEN, {
  polling: {
    interval: 300,
    params: {
      timeout: 10,
      allowed_updates: ['message', 'callback_query', 'edited_message', 'inline_query'],
    },
  },
});
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ─── Highlightly API ──────────────────────────────────────────────────────────

const API = axios.create({
  baseURL: 'https://soccer.highlightly.net',
  headers: { 'x-rapidapi-key': process.env.HIGHLIGHTLY_KEY },
  timeout: 15000,
});

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

const LIVE_DESCS     = new Set(['First half', 'Half time', 'Second half', 'Extra time', 'Penalties', 'Break time']);
const FINISHED_DESCS = new Set(['Finished', 'Finished (AET)', 'Finished (PEN)']);

function parseHLScore(scoreStr) {
  if (!scoreStr) return { home: null, away: null };
  const parts = scoreStr.split(' - ');
  return { home: parseInt(parts[0]) || 0, away: parseInt(parts[1]) || 0 };
}

// Convert Highlightly match → API-Football-compatible shape
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
    league: { id: m.league.id, name: m.league.name, country: m.country?.name || 'World', round: m.league?.round || null },
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
  1049216:{ name:'Copa de la Liga PE',country:'Peru'        },
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
  'saudi pro league':262041, 'saudi league':262041, 'arabia saudita':262041, 'saudi':262041,
  'jupiler pro league':123328, 'jupiler':123328,
  'super league grecia':168431, 'super league':168431,
  'liga egipto':199067, 'egypt':199067, 'egipto':199067,
  'k league':249276, 'k-league':249276, 'k league 1':249276, 'corea':249276, 'south korea':249276,
  'j league':84182, 'j1 league':84182,
  'scottish premier':153113, 'scottish premiership':153113, 'scotland':153113,
  'championship':34824,
  'euro':4188, 'euro championship':4188, 'eurocopa':4188,
  'nations league':5039, 'uefa nations league':5039,
  'copa america':8443,
  'chipre':271402, 'primera division chipre':271402, 'primera división chipre':271402, 'cyprus':271402,
  'noruega':88437, 'eliteserien':88437, 'norway':88437,
  'clasificatorias':29718, 'eliminatorias':29718, 'clasif mundial':29718,
  'suecia':96947, 'sweden':96947, 'allsvenskan':96947,
  'dinamarca':102053, 'denmark':102053, 'superliga dinamarca':102053, 'danish superliga':102053,
  'polonia':90990, 'poland':90990, 'ekstraklasa':90990,
  'serbia':244170, 'super liga serbia':244170,
  'suiza':176941, 'switzerland':176941, 'swiss super league':176941,
  'croacia':179494, 'croatia':179494, 'hnl':179494,
  'austria':186302, 'austria bundesliga':186302,
  'irlanda':304591, 'ireland':304591, 'liga irlanda':304591, 'league of ireland':304591,
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

// Ligas excluidas de picks automáticos (picks de hoy, picks en vivo)
// El bot sigue respondiendo si el usuario pregunta por estas ligas específicamente.
const PICKS_EXCLUDE_LEAGUES = new Set([
  205024,  // Primera B Colombia (datos insuficientes)
  224597,  // Liga Expansión MX (datos insuficientes)
  1049216, // Copa de la Liga Perú (datos insuficientes)
]);

// Buffer de cuota que se suma a la cuota real al mostrarla al usuario.
// Representa el margen de seguridad: si la cuota cayó por debajo de la real
// cuando el usuario intenta apostar, el pick ya no tiene valor → no entra.
// Internamente siempre usamos la cuota real para cálculos de EV.
const ODDS_DISPLAY_BUFFER = 0.15;

// Tasas históricas base de Over 2.5 y BTTS por liga
// Fuente: estadísticas 2023-2025, usadas para calibrar probabilidades
// Base rates históricas por liga (temporadas 2023-2025, promedio ponderado)
// over25: % partidos con más de 2.5 goles | btts: % BTTS Sí
// cards: promedio de tarjetas amarillas por partido (ambos equipos)
// corners: promedio de corners totales por partido
const LEAGUE_BASE_RATES = {
  // ── Ligas top europeas ────────────────────────────────────────────────────
  33973: { over25: 56, btts: 61, cards: 3.8, corners: 10.2, name: 'Premier League' },
  119924:{ over25: 52, btts: 55, cards: 4.9, corners:  9.8, name: 'LaLiga' },
  115669:{ over25: 54, btts: 57, cards: 4.6, corners:  9.6, name: 'Serie A' },
  67162: { over25: 62, btts: 60, cards: 3.6, corners: 10.1, name: 'Bundesliga' },
  52695: { over25: 53, btts: 54, cards: 4.2, corners:  9.7, name: 'Ligue 1' },
  // ── Copas europeas ─────────────────────────────────────────────────────────
  2486:  { over25: 61, btts: 63, cards: 3.5, corners: 10.5, name: 'Champions League' },
  3337:  { over25: 58, btts: 59, cards: 3.7, corners: 10.0, name: 'Europa League' },
  722432:{ over25: 57, btts: 58, cards: 3.6, corners:  9.8, name: 'Conference League' },
  // ── Otras europeas ─────────────────────────────────────────────────────────
  75672: { over25: 57, btts: 61, cards: 3.4, corners: 10.4, name: 'Eredivisie' },
  80778: { over25: 50, btts: 53, cards: 4.3, corners:  9.2, name: 'Primeira Liga' },
  123328:{ over25: 58, btts: 60, cards: 3.5, corners: 10.0, name: 'Jupiler Pro League' },
  173537:{ over25: 59, btts: 60, cards: 5.2, corners:  9.9, name: 'Super Lig' },
  176941:{ over25: 57, btts: 58, cards: 3.8, corners:  9.7, name: 'Swiss SL' },
  153113:{ over25: 55, btts: 57, cards: 3.9, corners:  9.5, name: 'Scottish Prem.' },
  102053:{ over25: 55, btts: 57, cards: 3.7, corners:  9.6, name: 'Superliga DK' },
  34824: { over25: 55, btts: 59, cards: 3.9, corners: 10.3, name: 'Championship' },
  120775:{ over25: 50, btts: 53, cards: 4.8, corners:  9.5, name: 'LaLiga2' },
  116520:{ over25: 51, btts: 53, cards: 4.5, corners:  9.3, name: 'Serie B' },
  68013: { over25: 58, btts: 57, cards: 3.8, corners: 10.0, name: '2. Bundesliga' },
  53546: { over25: 52, btts: 54, cards: 4.0, corners:  9.4, name: 'Ligue 2' },
  // ── Sudamérica ─────────────────────────────────────────────────────────────
  10145: { over25: 53, btts: 54, cards: 4.7, corners:  9.1, name: 'Copa Sudamericana' },
  11847: { over25: 54, btts: 55, cards: 4.8, corners:  9.3, name: 'Copa Libertadores' },
  61205: { over25: 55, btts: 58, cards: 4.5, corners:  9.0, name: 'Brasileirao' },
  223746:{ over25: 54, btts: 56, cards: 4.3, corners:  9.2, name: 'Liga MX' },
  204173:{ over25: 53, btts: 55, cards: 4.9, corners:  8.8, name: 'Liga BetPlay' },
  216087:{ over25: 54, btts: 57, cards: 3.8, corners: 10.1, name: 'MLS' },
  109712:{ over25: 48, btts: 50, cards: 5.1, corners:  8.7, name: 'Liga Argentina' },
  // ── Asia / Oriente Medio ──────────────────────────────────────────────────
  262041:{ over25: 57, btts: 60, cards: 3.9, corners:  9.8, name: 'Saudi Pro League' },
  84182: { over25: 55, btts: 58, cards: 3.2, corners: 10.2, name: 'J1 League' },
  249276:{ over25: 53, btts: 56, cards: 3.5, corners:  9.6, name: 'K League 1' },
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
  vip15: {
    nombre: 'VIP 15 días',
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

// ─── Wompi payment links ──────────────────────────────────────────────────────

const WOMPI_LINKS = {
  vip15: 'https://checkout.wompi.co/l/3ZBvRL',  // $59.900 COP — VIP 15 días
  vip30: 'https://checkout.wompi.co/l/LWfu76',  // $99.900 COP — VIP 30 días
  pro30: 'https://checkout.wompi.co/l/7t9pfC',  // $179.900 COP — PRO 30 días
};

// Genera link personalizado con referencia TELEGRAMID_plan
function wompiLink(baseLink, telegramId, plan) {
  return `${baseLink}?reference=${telegramId}_${plan}`;
}

// ─── Cache ────────────────────────────────────────────────────────────────────

const dateCache = new Map();
let liveCache = { raw: null, ts: 0 };

// Selecciones de equipo pendientes (desambiguación con botones)
const pendingTeamSelection = new Map(); // chatId → { candidates, intent, action }

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
  if (entry.fecha !== today) return null;
  const ageMs = Date.now() - new Date(entry.generadoAt).getTime();
  // Caché corto (45 min) si no hubo picks del motor — para reintentar cuando lleguen las cuotas
  // Caché normal (3 horas) si hubo picks reales del motor
  const maxAge = entry.noPicksEngine ? 45 * 60 * 1000 : 3 * 60 * 60 * 1000;
  if (ageMs > maxAge) return null;
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
    fecha:         today,
    generadoAt:    new Date().toISOString(),
    picksText,
    fixtureIds:    fixtureIds || [],
    noPicksEngine: (fixtureIds || []).length === 0, // true cuando no hubo picks del motor
  };
  savePicksCache(cache);
}

// ─── API helpers ──────────────────────────────────────────────────────────────

// Parses a Highlightly match object → internal fixture format
function parseFixture(m) {
  const score = parseHLScore(m.state?.score?.current);
  // Debug: log venue/referee raw data for WC matches
  if (m.league?.id === 1635) {
    console.log(`🏟️ WC raw[${m.id}] ${m.homeTeam?.name} vs ${m.awayTeam?.name} | venue=${JSON.stringify(m.venue)} | referee=${JSON.stringify(m.referee)} | keys=${Object.keys(m).join(',')}`);
  }
  return {
    fixtureId:  m.id,
    date:       m.date,
    status:     HL_STATUS[m.state?.description] || 'NS',
    elapsed:    m.state?.clock || null,
    leagueId:   m.league.id,
    leagueName: LEAGUE_MAP[m.league.id]?.name || m.league.name,
    country:    LEAGUE_MAP[m.league.id]?.country || m.country?.name || 'World',
    round:      m.league?.round || null,
    homeId:     m.homeTeam.id,
    awayId:     m.awayTeam.id,
    homeTeam:   m.homeTeam.name,
    awayTeam:   m.awayTeam.name,
    homeGoals:  score.home,
    awayGoals:  score.away,
    referee:    m.referee?.name || m.refereeName || m.officials?.[0]?.name || null,
    venue:      m.venue?.name  || m.stadium?.name || m.ground?.name || m.location?.name || null,
    city:       m.venue?.city  || m.stadium?.city || m.ground?.city || m.location?.city || m.city || null,
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

const FINISHED_STATUSES = new Set(['FT', 'AET', 'PEN', 'AWD', 'WO']);

async function getFixturesByDate(date) {
  // Colombia is UTC-5: games at 7PM+ Bogotá fall on the next UTC date.
  // Fetch both UTC dates and filter by Bogotá date.
  const nextUtcDate = (() => {
    const d = new Date(date + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + 1);
    return d.toISOString().split('T')[0];
  })();
  const [allToday, allTomorrow] = await Promise.all([
    fetchFixturesByDate(date),
    fetchFixturesByDate(nextUtcDate),
  ]);
  const all = [...allToday, ...allTomorrow];
  const seen = new Set();
  const unique = all.filter(m => {
    if (seen.has(m.id)) return false;
    seen.add(m.id);
    return true;
  });
  return unique
    .filter(m => {
      if (!LEAGUE_IDS.has(m.league?.id)) return false;
      const st = HL_STATUS[m.state?.description] || 'NS';
      if (FINISHED_STATUSES.has(st)) return false;
      const fixtureDate = new Date(m.date).toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
      return fixtureDate === date;
    })
    .map(parseFixture);
}

async function fetchLiveRaw() {
  if (Date.now() - liveCache.ts < 30000 && liveCache.raw) return liveCache.raw;
  const today = new Date().toISOString().split('T')[0];
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

// Stat name mapping: Highlightly displayName → internal key used by calcLiveMomentum etc.
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
  data.forEach((teamData, idx) => {
    const key = teamData.team.name;
    const raw = {};
    teamData.statistics.forEach(s => { raw[s.displayName] = s.value; });
    const totalShots = (raw['Shots on target'] || 0) + (raw['Shots off target'] || 0) + (raw['Blocked shots'] || 0);
    const possession = raw['Possession'];
    stats[key] = { 'Total Shots': totalShots || null };
    if (possession != null) stats[key]['Ball Possession'] = `${Math.round(possession * 100)}%`;
    for (const [hlName, apifName] of Object.entries(HL_STAT_MAP)) {
      if (raw[hlName] != null) stats[key][apifName] = raw[hlName];
    }
    if (idx === 0) stats._homeTeam = key;
    if (idx === 1) stats._awayTeam = key;
  });
  return stats;
}

// Helper: accede a las stats del local de forma segura
function homeStats(stats) {
  if (!stats) return {};
  return stats[stats._homeTeam] || Object.values(stats).find(v => typeof v === 'object') || {};
}

// Helper: accede a las stats del visitante de forma segura
function awayStats(stats) {
  if (!stats) return {};
  return stats[stats._awayTeam] || Object.values(stats).filter(v => typeof v === 'object')[1] || {};
}

function normalizeTeamName(str) {
  return str
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // quita tildes y diacríticos (ç→c, ü→u, ö→o, etc.)
    .replace(/[/\-_.&']/g, ' ')                       // normaliza separadores (Bodo/Glimt → bodo glimt)
    .replace(/\s+/g, ' ')
    .trim();
}

// TEAM_ID_OVERRIDES removed — Highlightly team IDs differ from API-Football.
// searchTeam uses text search with scoring instead.
const TEAM_ID_OVERRIDES = {
  // ── Selecciones nacionales (IDs directos — evitan /teams?search= cuando la cuota es baja) ──
  'uruguay':          7,
  'colombia':         8,
  'brasil':           6,
  'brazil':           6,
  'serbia':           14,
  'south korea':      17,
  'corea del sur':    17,
  'corea sur':        17,
  'korea del sur':    17,
  'australia':        20,
  'iran':             22,
  'saudi arabia':     23,
  'arabia saudi':     23,
  'arabia saudita':   23,
  'costa rica':       29,
  'peru':             30,
  'morocco':          31,
  'marruecos':        31,
  'maruecos':         31,
  'japon':            12,
  'japan':            12,
  'panama':           11,
  'panamá':           11,
  'venezuela':        2379,
  'paraguay':         2380,
  'bolivia':          2381,
  'ecuador':          2382,
  'chile':            2383,
  // Ligue 1
  'monaco':           91,
  'mónaco':           91,
  'as monaco':        91,
  'as mónaco':        91,
  'psg':              85,
  'paris saint germain': 85,
  'olympique lyon':   80,
  'olympique lyonnais': 80,
  'olympique marseille': 81,
  // Premier League
  'arsenal':          42,
  'chelsea':          49,
  'liverpool':        40,
  'manchester city':  50,
  'manchester united': 33,
  'man city':         50,
  'man united':       33,
  'man utd':          33,
  'tottenham':        47,
  'tottenham hotspur': 47,
  'newcastle':        34,
  'newcastle united': 34,
  // LaLiga
  'real madrid':      541,
  'barcelona':        529,
  'atletico madrid':  530,
  'atletico':         530,
  'sevilla':          536,
  'valencia':         532,
  'villarreal':       533,
  'betis':            543,
  'real betis':       543,
  'sociedad':         548,
  'real sociedad':    548,
  // Serie A
  'juventus':         496,
  'inter':            505,
  'inter milan':      505,
  'milan':            489,
  'ac milan':         489,
  'roma':             497,
  'as roma':          497,
  'napoli':           492,
  'lazio':            487,
  'atalanta':         482,
  // Bundesliga
  'bayern':           157,
  'bayern munich':    157,
  'dortmund':         165,
  'borussia dortmund': 165,
  'bvb':              165,
  'leverkusen':       168,
  'bayer leverkusen': 168,
  'frankfurt':        169,
  'eintracht frankfurt': 169,
  // Champions / Europa
  'ajax':             194,
  'benfica':          211,
  'porto':            212,
  'sporting cp':      228,
  'celtic':           256,
  'rangers':          257,
  'anderlecht':       233,
  // Saudi Pro League
  'al ahli':          2929,
  'al ahli saudi':    2929,
  'al ahli sc':       2929,
  'al ahli jeddah':   2929,
  'al hilal':         2932,
  'hilal':            2932,
  'al nassr':         2939,
  'nassr':            2939,
  'al ittihad':       2938,
  'ittihad':          2938,
  'al qadsiah':       2933,
  'al qadisiyah':     2933,
  'al shabab':        2940,
  'al ettifaq':       2934,
  'ettifaq':          2934,
  'al fateh':         2931,
  'al taawoun':       2936,
  'al taawon':        2936,
  'al fayha':         2944,
  'al khaleej':       2928,
  'al hazm':          2945,
  'damac':            2956,
};

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
  'celta de vigo':   'Celta Vigo',
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
  'mónaco':          'AS Monaco',
  'as monaco':       'AS Monaco',
  'as mónaco':       'AS Monaco',
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
  'nacional':        'Club Nacional',

  // ── Saudi Pro League (nombres exactos verificados con API-Football) ──────
  'al ahli':         'Al-Ahli Jeddah',
  'al ahli saudi':   'Al-Ahli Jeddah',
  'al ahli sc':      'Al-Ahli Jeddah',
  'al ahli jeddah':  'Al-Ahli Jeddah',
  'al hilal':        'Al-Hilal Saudi FC',
  'hilal':           'Al-Hilal Saudi FC',
  'al nassr':        'Al-Nassr',
  'nassr':           'Al-Nassr',
  'al ittihad':      'Al-Ittihad FC',
  'ittihad':         'Al-Ittihad FC',
  'al qadsiah':      'Al-Qadisiyah FC',
  'al qadisiyah':    'Al-Qadisiyah FC',
  'al shabab':       'Al Shabab',
  'al ettifaq':      'Al-Ettifaq',
  'ettifaq':         'Al-Ettifaq',
  'al fateh':        'Al-Fateh',
  'al taawoun':      'Al Taawon',
  'al taawon':       'Al Taawon',
  'al fayha':        'Al-Fayha',
  'al khaleej':      'Al Khaleej Saihat',
  'al hazm':         'Al-Hazm',
  'damac':           'Damac',
  'al okhdood':      'Al Okhdood',

  // ── Selecciones nacionales (español → inglés para API) ──────────────────
  'francia':         'France',
  'seleccion francesa': 'France',
  'brasil':          'Brazil',
  'seleccion brasilena': 'Brazil',
  'alemania':        'Germany',
  'espana':          'Spain',
  'seleccion espanola': 'Spain',
  'italia':          'Italy',
  'seleccion italiana': 'Italy',
  'inglaterra':      'England',
  'portugal':        'Portugal',
  'holanda':         'Netherlands',
  'paises bajos':    'Netherlands',
  'belgica':         'Belgium',
  'croacia':         'Croatia',
  'colombia':        'Colombia',
  'seleccion colombia': 'Colombia',
  'argentina':       'Argentina',
  'seleccion argentina': 'Argentina',
  'uruguay':         'Uruguay',
  'chile':           'Chile',
  'peru':            'Peru',
  'ecuador':         'Ecuador',
  'venezuela':       'Venezuela',
  'mexico':          'Mexico',
  'estados unidos':  'United States',
  'usa':             'United States',
  'eeuu':            'United States',
  'japon':           'Japan',
  'corea':           'South Korea',
  'corea del sur':   'South Korea',
  'korea del sur':   'South Korea',
  'corea sur':       'South Korea',
  'corea del norte': 'North Korea',
  'korea del norte': 'North Korea',
  'republica checa': 'Czech Republic',
  'chequia':         'Czech Republic',
  'hungria':         'Hungary',
  'rumania':         'Romania',
  'eslovaquia':      'Slovakia',
  'eslovenia':       'Slovenia',
  'rusia':           'Russia',
  'irak':            'Iraq',
  'argelia':         'Algeria',
  'costa de marfil': 'Ivory Coast',
  'tunez':           'Tunisia',
  'nueva zelanda':   'New Zealand',
  'tailandia':       'Thailand',
  'siria':           'Syria',
  'jordania':        'Jordan',
  'emiratos arabes unidos': 'UAE',
  'emiratos arabes': 'UAE',
  'emiratos':        'UAE',
  'arabia saudi':    'Saudi Arabia',
  'marruecos':       'Morocco',
  'senegal':         'Senegal',
  'nigeria':         'Nigeria',
  'ghana':           'Ghana',
  'camerun':         'Cameroon',
  'suiza':           'Switzerland',
  'austria':         'Austria',
  'turquia':         'Turkey',
  'dinamarca':       'Denmark',
  'suecia':          'Sweden',
  'noruega':         'Norway',
  'polonia':         'Poland',
  'ucrania':         'Ukraine',
  'serbia':          'Serbia',
  'escocia':         'Scotland',
  'gales':           'Wales',
  'irlanda':         'Republic of Ireland',
  'australia':       'Australia',
  'canada':          'Canada',
  'costa rica':      'Costa Rica',
  'panama':          'Panama',
  'paraguay':        'Paraguay',
  'bolivia':         'Bolivia',
  'arabia saudita':  'Saudi Arabia',
  'iran':            'IR Iran',
  'qatar':           'Qatar',
  'china':           'China',
  'egipto':          'Egypt',
  'sudafrica':       'South Africa',
  'la tri':          'Ecuador',
  'cafeteros':       'Colombia',
  'la roja':         'Spain',
  'la albiceleste':  'Argentina',
  'la verdeamarela': 'Brazil',
  'les bleus':       'France',
  'la sele':         'Costa Rica',
};

function translateTeamName(name) {
  const key = name.toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .trim();
  return TEAM_ALIASES[key] || name;
}

function scoreTeamResult(t, q, country = '', isNationalSearch = false) {
  const tname    = normalizeTeamName(t.team.name);
  const tcountry = (t.team.country || '').toLowerCase();
  const RESERVE  = /\b(ii|b|reserve|reserva|sub|youth|juvenil|u\d{2}|amateur|filial)\b/i;
  const WOMEN    = /\b(women|femenin[ao]|ladies|femmes|damen|vrouwen|mujer|femenino|fem\.?)\b| W$/i;
  const LOW_TIER = /\b(primera\s*[cd]|tercera|cuarta|regional|sunday|indoor|futsal|beach\s*soccer)\b/i;
  const CLUB_PREFIX = /^(fc|cf|ac|as|afc|rc|sc|bk|fk|sk|vfb?|sv|ss|us|ud|cd|sd|rcd|real\s|atletico\s|sporting\s|dynamo\s|dinamo\s)/i;
  const EURO_COUNTRIES = new Set(['switzerland','england','spain','italy','germany','france','netherlands','portugal','belgium','turkey','greece','russia','scotland','austria','sweden','norway','denmark','poland','ukraine','serbia','croatia','czech republic','romania','hungary','cyprus','israel','bulgaria','saudi arabia','egypt','south korea','japan','brazil','argentina','colombia','mexico','usa','united states','china','morocco','algeria','nigeria','south africa']);

  let s = 0;
  if (tname === q) s += 100;
  else if (tname.endsWith(' ' + q) || tname.endsWith(q)) s += 80;
  else if (tname.startsWith(q + ' ') || tname.startsWith(q)) s += 50;
  else if (tname.includes(q)) s += 20;
  if (country && tcountry.includes(country)) s += 40;
  if (RESERVE.test(t.team.name))  s -= 50;
  if (WOMEN.test(t.team.name))    s -= 80;
  if (LOW_TIER.test(t.team.name)) s -= 60;
  if (CLUB_PREFIX.test(t.team.name)) s += 25;
  if (EURO_COUNTRIES.has(tcountry)) s += 20;
  if (t.team.national === true) s += 60;
  if (isNationalSearch && t.team.national !== true) s -= 30;
  return s;
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

// Verifica si un equipo está jugando ahora, hoy o en los próximos 2 días
async function getTeamPlayingPriority(teamId) {
  try {
    const live = liveCache.raw || [];
    if (live.some(m => m.homeTeam?.id === teamId || m.awayTeam?.id === teamId)) return { priority: 3, label: '🔴 En vivo ahora' };
    const today = todayDate();
    const todayFixtures = dateCache.get(today) || [];
    if (todayFixtures.some(m => m.homeTeam?.id === teamId || m.awayTeam?.id === teamId)) return { priority: 2, label: '📅 Juega hoy' };
    for (let d = 1; d <= 2; d++) {
      const dt = new Date(); dt.setDate(dt.getDate() + d);
      const ds = dt.toISOString().split('T')[0];
      const cached = dateCache.get(ds) || [];
      if (cached.some(m => m.homeTeam?.id === teamId || m.awayTeam?.id === teamId)) {
        return { priority: 1, label: `📆 Juega en ${d} día${d>1?'s':''}` };
      }
    }
  } catch {}
  return { priority: 0, label: '' };
}

async function findTeamWithButtons(chatId, name, countryHint = '', intent = null) {
  const apiName = translateTeamName(name);
  const { data } = await API.get('/teams', { params: { name: apiName, limit: 20 } });
  const results = data.data || [];
  if (results.length === 0) return null;

  const q = normalizeTeamName(apiName);
  const YOUTH_RE = /\b(u\d{2}|sub[\s-]?\d{2}|under[\s-]?\d{2}|ii|iii|iv|vi?|reserve|reserves|youth|juvenil|cadete|filial|amador|amateur)\b/i;
  const WOMEN_RE = /\b(women|femenin[ao]|ladies|femmes|damen|vrouwen|mujer|fem\.?)\b| W$/i;
  const filtered = results.filter(t => !YOUTH_RE.test(t.name) && !WOMEN_RE.test(t.name));
  const pool = filtered.length > 0 ? filtered : results;

  // Convert Highlightly results to { team: { id, name, country, national } } shape for scoreTeamResult
  const poolApif = pool.map(t => ({ team: { id: t.id, name: t.name, country: t.country || '', national: t.type === 'national' } }));
  const scored = poolApif
    .map(t => ({ ...t, _score: scoreTeamResult(t, q, countryHint.trim().toLowerCase(), false) }))
    .filter(t => t._score > 0)
    .sort((a, b) => b._score - a._score)
    .slice(0, 5);

  if (scored.length === 0) return null;

  const gap = scored.length > 1 ? scored[0]._score - scored[1]._score : 999;
  if (gap > 30 || scored.length === 1) return scored[0];

  const enriched = await Promise.all(scored.slice(0, 4).map(async t => {
    const playingInfo = await getTeamPlayingPriority(t.team.id);
    return { ...t, _priority: playingInfo.priority, _priorityLabel: playingInfo.label };
  }));

  enriched.sort((a, b) => b._priority - a._priority || b._score - a._score);

  const nameGapTop = enriched[0]._score - (enriched[1]?._score || 0);
  if (enriched[0]._priority >= 2 && (enriched[1]?._priority || 0) === 0 && nameGapTop > 15) return enriched[0];

  const intentCode = (intent?.intencion === 'rachas') ? 'r' : 'p';

  const buttons = enriched.map(t => [{
    text: `${t._priorityLabel ? t._priorityLabel + ' · ' : ''}${t.team.name} (${t.team.country})`,
    callback_data: `tm_${t.team.id}_${intentCode}`
  }]);
  buttons.push([{ text: '❌ Cancelar', callback_data: 'tm_cancel' }]);

  await bot.sendMessage(chatId,
    `🔍 Encontré *${enriched.length}* equipos con ese nombre. ¿A cuál te refieres?`,
    { parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons } }
  );

  return 'PENDING';
}

async function findNextFixtureByDate(teamId, daysAhead = 14) {
  const LIVE_STATUSES   = new Set(['First half', 'Half time', 'Second half', 'Extra time', 'Penalties', 'Break time']);
  const UPCOMING_STATUSES = new Set(['Not started', ...LIVE_STATUSES]);
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() + daysAhead);

  // 1. Partido en vivo hoy
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
      // No usar '0 - 0' como fallback — score null significa "dato no disponible", no 0-0
      const scoreStr = m.state?.score?.current ?? m.state?.score?.fulltime ?? null;
      const score = parseHLScore(scoreStr);
      // Debug: log primer partido histórico para ver campos disponibles
      if (!getTeamLastFixtures._loggedFields) {
        getTeamLastFixtures._loggedFields = true;
        console.log(`🔍 getTeamLastFixtures sample raw keys: ${Object.keys(m).join(', ')}`);
        console.log(`🔍 getTeamLastFixtures score fields: current=${m.state?.score?.current} state.keys=${Object.keys(m.state||{}).join(',')}`);
      }
      return {
        fixtureId:  m.id,
        date:       m.date.split('T')[0],
        homeTeam:   m.homeTeam.name,
        awayTeam:   m.awayTeam.name,
        homeId:     m.homeTeam.id,
        awayId:     m.awayTeam.id,
        leagueName: m.league.name,
        leagueId:   m.league.id,
        goalsHome:  score.home,   // null si no hay score — NO defaultear a 0
        goalsAway:  score.away,   // null si no hay score — NO defaultear a 0
        htHome:     null,
        htAway:     null,
      };
    });
}

async function getLeagueStandings(leagueId) {
  const season = LEAGUE_SEASONS[leagueId] || 2025;
  try {
    const { data } = await API.get('/standings', { params: { leagueId, season } });
    const groups = data.groups || [];
    const group = groups[0]?.standings || [];
    return {
      teams: group.map(s => ({
        teamId:   s.team.id,
        teamName: s.team.name,
        rank:     s.position,
        points:   s.points ?? null,
        goalDiff: null,
        played:   s.played ?? 0,
        description: null,
        form:     null,
      })),
      total: group.length,
    };
  } catch { return { teams: [], total: 0 }; }
}
function getTeamMotivation(standing, totalTeams) {
  if (!standing) return { estado: 'desconocido', texto: 'Sin datos de posición' };
  const { rank, description, points, played, form } = standing;
  const desc = (description || '').toLowerCase();
  const remainingGames = Math.max(0, 38 - (played || 0));
  const isEndOfSeason = remainingGames <= 5;
  const isLastMatchday = remainingGames <= 1;

  let estado, texto;
  if (desc.includes('champion') && rank <= 2) {
    estado = 'lucha_titulo'; texto = '🏆 Lucha por el campeonato — máxima motivación';
  } else if (desc.includes('champion')) {
    estado = 'clasifica_champions'; texto = '⭐ En zona Champions League — alta motivación';
  } else if (desc.includes('europa league') || (desc.includes('europa') && !desc.includes('conference'))) {
    estado = 'clasifica_europa'; texto = '🌍 Persigue plaza de Europa League';
  } else if (desc.includes('conference')) {
    estado = 'clasifica_conference'; texto = '🎯 Lucha por Conference League';
  } else if (desc.includes('play-off') || desc.includes('playoff')) {
    estado = 'play_off_descenso'; texto = '⚠️ En play-off de descenso — desesperación';
  } else if (desc.includes('relegation')) {
    estado = 'lucha_descenso'; texto = '🚨 ZONA DE DESCENSO — máxima presión, juega por sobrevivir';
  } else if (rank <= 2) {
    estado = 'lucha_titulo'; texto = '🏆 Candidato al título — máxima motivación';
  } else if (rank <= 5) {
    estado = 'clasifica_champions'; texto = '⭐ Pelea por Champions League';
  } else if (rank <= 7) {
    estado = 'clasifica_europa'; texto = '🌍 Persigue plazas europeas';
  } else if (totalTeams > 0 && rank >= totalTeams - 2) {
    estado = 'lucha_descenso'; texto = '🚨 Zona de descenso directa — desesperación';
  } else if (totalTeams > 0 && rank >= totalTeams - 5) {
    estado = 'riesgo_descenso'; texto = '⚠️ Cerca de zona de descenso — necesita puntos';
  } else {
    estado = 'nada_en_juego';
    texto = isEndOfSeason
      ? '😴 Sin nada en juego al final de temporada — ALERTA: posibles rotaciones o bajas motivación'
      : '📊 Posición media, sin objetivo urgente';
  }

  // ── Corrección crítica: última jornada con posición clasificatoria ya asegurada ──
  // Si es la última jornada y el equipo está en zona europea/title pero NO está
  // luchando activamente (no hay presión de descenso ni de último puesto que da CL),
  // marcar como posible "plaza asegurada" para que Claude no invente urgencia.
  if (isLastMatchday) {
    if (estado === 'clasifica_champions') {
      texto = '⭐ En zona Champions — ÚLTIMA JORNADA: verificar si plaza ya está matemáticamente asegurada (posibles rotaciones)';
      estado = 'clasifica_champions_posible_asegurado';
    } else if (estado === 'lucha_titulo') {
      // El título solo tiene urgencia real si sigue en disputa
      texto = '🏆 En zona de título — ÚLTIMA JORNADA: verificar si campeonato sigue en disputa';
    } else if (estado === 'clasifica_europa' || estado === 'clasifica_conference') {
      texto = texto + ' — ÚLTIMA JORNADA: verificar si plaza europea ya está asegurada';
    } else if (estado === 'nada_en_juego') {
      texto = '😴 Sin objetivo en juego — última jornada, máximo riesgo de rotaciones masivas';
    }
  }

  return { estado, texto, rank, puntos: points, jugados: played, forma_api: form, jornadas_restantes: remainingGames };
}





async function getH2H(id1, id2) {
  const { data } = await API.get('/head-2-head', { params: { teamIdOne: id1, teamIdTwo: id2, limit: 10 } });
  const matches = Array.isArray(data) ? data : (data.data || []);
  return matches
    .filter(m => FINISHED_DESCS.has(m.state?.description))
    .map(m => {
      const scoreStr = m.state?.score?.current ?? m.state?.score?.fulltime ?? null;
      const score = parseHLScore(scoreStr);
      if (score.home === null) return null;   // sin datos de score — descartar
      return {
        date:      m.date.split('T')[0],
        home:      m.homeTeam.name,
        away:      m.awayTeam.name,
        golesHome: score.home,
        golesAway: score.away,
        btts:      score.home > 0 && score.away > 0,
      };
    })
    .filter(Boolean);
}

// Highlightly does not have lineup/events/injuries/odds endpoints — stub safely
async function getLineups(fixtureId) { return null; }

/**
 * Eventos del partido: Highlightly no expone este endpoint.
 * summarizeEvents() handles null/empty gracefully.
 */
async function getFixtureEvents(fixtureId) { return []; }

function summarizeEvents(events, homeTeam, awayTeam) {
  if (!events || events.length === 0) return null;
  return null; // Highlightly does not provide live events
}

async function getInjuries(teamId, leagueId) { return []; }
async function getFixtureInjuries(fixtureId) { return []; }

async function getRealOdds(fixtureId) {
  try {
    const { data } = await API.get('/odds', { params: { matchId: fixtureId } });
    const items = data?.data;
    if (!Array.isArray(items) || !items.length) return null;
    const oddsArr = items[0]?.odds;
    if (!Array.isArray(oddsArr) || !oddsArr.length) return null;

    // Promediar cuotas de todas las casas por mercado+resultado
    const totals = {}, counts = {};
    const marketNames = new Set();
    for (const entry of oddsArr) {
      const mkt = entry.market;
      marketNames.add(mkt);
      for (const v of (entry.values || [])) {
        const key = `${mkt}||${v.value}`;
        totals[key] = (totals[key] || 0) + (v.odd || 0);
        counts[key] = (counts[key] || 0) + 1;
      }
    }
    // Log mercados disponibles para debug (solo primeros fixtures del día)
    if (!getRealOdds._loggedMarkets) {
      getRealOdds._loggedMarkets = true;
      console.log(`📋 Mercados disponibles Highlightly [${fixtureId}]: ${[...marketNames].join(' | ')}`);
      // Log valores de muestra para los mercados clave
      const sampleKeys = [...Object.keys(counts)].filter(k => counts[k] > 0).slice(0, 30);
      console.log(`📋 Muestra valores: ${sampleKeys.map(k => `${k}(${(totals[k]/counts[k]).toFixed(2)})`).join(', ')}`);
    }

    const avg = (mkt, val) => {
      const key = `${mkt}||${val}`;
      return counts[key] ? +(totals[key] / counts[key]).toFixed(2) : null;
    };
    // También busca el mercado con nombres alternativos
    const avgAlt = (...pairs) => {
      for (const [mkt, val] of pairs) {
        const v = avg(mkt, val);
        if (v) return v;
      }
      return null;
    };

    const result = {};
    // ── Resultado FT
    result.homeWin = avg('Full Time Result', 'Home');
    result.draw    = avg('Full Time Result', 'Draw');
    result.awayWin = avg('Full Time Result', 'Away');
    // ── Goles FT
    result.over05  = avgAlt(['Total Goals 0.5','Over'],  ['Goals Over/Under 0.5','Over']);
    result.under05 = avgAlt(['Total Goals 0.5','Under'], ['Goals Over/Under 0.5','Under']);
    result.over15  = avgAlt(['Total Goals 1.5','Over'],  ['Goals Over/Under 1.5','Over']);
    result.under15 = avgAlt(['Total Goals 1.5','Under'], ['Goals Over/Under 1.5','Under']);
    result.over25  = avgAlt(['Total Goals 2.5','Over'],  ['Goals Over/Under 2.5','Over'],  ['Total Goals','Over 2.5']);
    result.under25 = avgAlt(['Total Goals 2.5','Under'], ['Goals Over/Under 2.5','Under'], ['Total Goals','Under 2.5']);
    result.over35  = avgAlt(['Total Goals 3.5','Over'],  ['Goals Over/Under 3.5','Over']);
    result.under35 = avgAlt(['Total Goals 3.5','Under'], ['Goals Over/Under 3.5','Under']);
    result.over45  = avgAlt(['Total Goals 4.5','Over'],  ['Goals Over/Under 4.5','Over']);
    result.under45 = avgAlt(['Total Goals 4.5','Under'], ['Goals Over/Under 4.5','Under']);
    // ── BTTS
    result.bttsYes = avgAlt(['Both Teams To Score','Yes'], ['Both Teams to Score','Yes'], ['BTTS','Yes']);
    result.bttsNo  = avgAlt(['Both Teams To Score','No'],  ['Both Teams to Score','No'],  ['BTTS','No']);
    // ── Doble oportunidad
    result.dc_1X = avgAlt(['Double Chance','1X'], ['Double Chance','Home or Draw']);
    result.dc_X2 = avgAlt(['Double Chance','X2'], ['Double Chance','Draw or Away']);
    result.dc_12 = avgAlt(['Double Chance','12'], ['Double Chance','Home or Away']);
    // ── Draw No Bet
    result.dnb_home = avgAlt(['Draw No Bet','Home'], ['DNB','Home']);
    result.dnb_away = avgAlt(['Draw No Bet','Away'], ['DNB','Away']);
    // ── Hándicap asiático
    result.ah_home_m05 = avgAlt(['Asian Handicap','Home -0.5'], ['Asian Handicap','-0.5']);
    result.ah_away_m05 = avgAlt(['Asian Handicap','Away -0.5'], ['Asian Handicap','+0.5']);
    result.ah_home_m15 = avgAlt(['Asian Handicap','Home -1.5'], ['Asian Handicap','-1.5']);
    result.ah_away_m15 = avgAlt(['Asian Handicap','Away -1.5'], ['Asian Handicap','+1.5']);
    // ── HT resultado
    result.homeWin_1T = avgAlt(['First Half Result','Home'], ['Half Time','Home'], ['1st Half Result','Home']);
    result.draw_1T    = avgAlt(['First Half Result','Draw'], ['Half Time','Draw'], ['1st Half Result','Draw']);
    result.awayWin_1T = avgAlt(['First Half Result','Away'], ['Half Time','Away'], ['1st Half Result','Away']);
    // ── HT goles
    result.over05_1T = avgAlt(['First Half Goals 0.5','Over'], ['1st Half Total Goals 0.5','Over'], ['HT Over/Under 0.5','Over']);
    result.under05_1T= avgAlt(['First Half Goals 0.5','Under'],['1st Half Total Goals 0.5','Under'],['HT Over/Under 0.5','Under']);
    result.over15_1T = avgAlt(['First Half Goals 1.5','Over'], ['1st Half Total Goals 1.5','Over'], ['HT Over/Under 1.5','Over']);
    result.under15_1T= avgAlt(['First Half Goals 1.5','Under'],['1st Half Total Goals 1.5','Under'],['HT Over/Under 1.5','Under']);
    // ── Corners totales
    result.cornersOver65  = avgAlt(['Total Corners 6.5','Over'],  ['Corners Over/Under 6.5','Over'],  ['Corner Kicks 6.5','Over']);
    result.cornersUnder65 = avgAlt(['Total Corners 6.5','Under'], ['Corners Over/Under 6.5','Under'], ['Corner Kicks 6.5','Under']);
    result.cornersOver75  = avgAlt(['Total Corners 7.5','Over'],  ['Corners Over/Under 7.5','Over'],  ['Corner Kicks 7.5','Over']);
    result.cornersUnder75 = avgAlt(['Total Corners 7.5','Under'], ['Corners Over/Under 7.5','Under'], ['Corner Kicks 7.5','Under']);
    result.cornersOver85  = avgAlt(['Total Corners 8.5','Over'],  ['Corners Over/Under 8.5','Over'],  ['Corner Kicks 8.5','Over']);
    result.cornersUnder85 = avgAlt(['Total Corners 8.5','Under'], ['Corners Over/Under 8.5','Under'], ['Corner Kicks 8.5','Under']);
    result.cornersOver95  = avgAlt(['Total Corners 9.5','Over'],  ['Corners Over/Under 9.5','Over'],  ['Corner Kicks 9.5','Over']);
    result.cornersUnder95 = avgAlt(['Total Corners 9.5','Under'], ['Corners Over/Under 9.5','Under'], ['Corner Kicks 9.5','Under']);
    result.cornersOver105 = avgAlt(['Total Corners 10.5','Over'], ['Corners Over/Under 10.5','Over'], ['Corner Kicks 10.5','Over']);
    // ── Corners por equipo
    result.homeCorners_over35 = avgAlt(['Home Team Corners 3.5','Over'],  ['Team Corners Home 3.5','Over']);
    result.homeCorners_over45 = avgAlt(['Home Team Corners 4.5','Over'],  ['Team Corners Home 4.5','Over']);
    result.awayCorners_over25 = avgAlt(['Away Team Corners 2.5','Over'],  ['Team Corners Away 2.5','Over']);
    result.awayCorners_over35 = avgAlt(['Away Team Corners 3.5','Over'],  ['Team Corners Away 3.5','Over']);
    // ── Tarjetas totales
    result.cardsOver25 = avgAlt(['Total Yellow Cards 2.5','Over'], ['Cards Over/Under 2.5','Over'], ['Total Cards 2.5','Over']);
    result.cardsUnder25= avgAlt(['Total Yellow Cards 2.5','Under'],['Cards Over/Under 2.5','Under'],['Total Cards 2.5','Under']);
    result.cardsOver35 = avgAlt(['Total Yellow Cards 3.5','Over'], ['Cards Over/Under 3.5','Over'], ['Total Cards 3.5','Over']);
    result.cardsOver45 = avgAlt(['Total Yellow Cards 4.5','Over'], ['Cards Over/Under 4.5','Over'], ['Total Cards 4.5','Over']);
    // ── Tarjetas por equipo
    result.homeCardsOver15 = avgAlt(['Home Team Cards 1.5','Over'], ['Team Cards Home 1.5','Over']);
    result.awayCardsOver15 = avgAlt(['Away Team Cards 1.5','Over'], ['Team Cards Away 1.5','Over']);
    // ── Portería a cero
    result.cleanSheetHome = avgAlt(['Clean Sheet','Home'], ['Home Clean Sheet','Yes']);
    result.cleanSheetAway = avgAlt(['Clean Sheet','Away'], ['Away Clean Sheet','Yes']);
    // ── Goles por equipo (anota)
    result.homeToScore  = avgAlt(['Home Team To Score','Yes'], ['Home to Score','Yes']);
    result.awayToScore  = avgAlt(['Away Team To Score','Yes'], ['Away to Score','Yes']);
    // ── Ambas mitades con goles
    result.goalsBothHalves = avgAlt(['Both Halves','Yes'], ['Goals in Both Halves','Yes'], ['Score in Both Halves','Yes']);

    Object.keys(result).forEach(k => { if (!result[k]) delete result[k]; });
    if (!result.homeWin && !result.over25) return null;
    result._source = 'highlightly';
    return result;
  } catch (e) {
    console.warn(`getRealOdds(${fixtureId}): ${e.message}`);
    return null;
  }
}

async function getLiveOdds(fixtureId) {
  return getRealOdds(fixtureId);
}

async function prefetchOddsApi(fixtures, _date) {
  const map = new Map();
  const top = fixtures.slice(0, 30);
  await Promise.allSettled(top.map(async f => {
    const odds = await getRealOdds(f.fixtureId);
    if (odds) map.set(f.fixtureId, odds);
  }));
  console.log(`📊 Highlightly odds: ${map.size}/${top.length} partidos`);
  return map;
}
async function getLastMatchDate(teamId) { return null; }
async function getApiPrediction(fixtureId) {
  try {
    const { data } = await API.get('/matches/' + fixtureId);
    // Log raw response shape para debug
    console.log(`🔍 getApiPrediction(${fixtureId}) tipo=${Array.isArray(data)?'array':'object'} keys=${Object.keys(data||{}).join(',')}`);
    // Highlightly puede devolver: [{...}], { data: [{...}] }, { data: {...} }, o {...}
    const raw = Array.isArray(data)           ? data
      : Array.isArray(data?.data)             ? data.data
      : data?.data && typeof data.data === 'object' ? [data.data]
      : [data];
    const detail = raw[0];
    if (detail && typeof detail === 'object') {
      console.log(`🔍 getApiPrediction(${fixtureId}) detail keys: ${Object.keys(detail).join(', ')} | venue=${JSON.stringify(detail.venue)} | referee=${JSON.stringify(detail.referee)}`);
    }
    if (!detail || typeof detail !== 'object') return null;

    const result = {
      // Intentar múltiples nombres de campo por si Highlightly los llama diferente
      _venue:   detail.venue?.name    || detail.stadium?.name  || detail.ground?.name   || detail.location?.name  || null,
      _city:    detail.venue?.city    || detail.stadium?.city  || detail.ground?.city   || detail.location?.city  || detail.city || null,
      _referee: detail.referee?.name  || detail.refereeName    || detail.officials?.[0]?.name || null,
    };

    // predictions puede ser array [{...}] o un objeto solo {...} — normalizar a array
    const predsRaw = detail.predictions;
    const predsArr = Array.isArray(predsRaw) ? predsRaw
      : predsRaw && typeof predsRaw === 'object' ? [predsRaw]
      : [];
    const preds  = predsArr.filter(p => p?.type === 'prematch' || p?.probabilities);
    const latest = preds[preds.length - 1] || predsArr[predsArr.length - 1];
    if (latest?.probabilities) {
      result.percent_home = parseFloat(latest.probabilities.home);
      result.percent_draw = parseFloat(latest.probabilities.draw);
      result.percent_away = parseFloat(latest.probabilities.away);
    }

    if (result._venue) console.log(`✅ getApiPrediction(${fixtureId}): venue="${result._venue}" referee="${result._referee}"`);
    return Object.values(result).some(v => v !== null) ? result : null;
  } catch (e) {
    console.warn(`getApiPrediction(${fixtureId}): ${e.message}`);
    return null;
  }
}

// ─── StatsHub — estadísticas de árbitros (fuente única y confiable) ──────────
const STATSHUB_API = 'https://www.statshub.com/api/referees/list?page=1&limit=500&upcomingFixturesOnly=false&last20GamesOnly=true&leagueStatsOnly=false&sortField=next_game_timestamp&sortDirection=asc';
let statsHubReferees = [];

function normalizeRefName(name) {
  return name
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z\s]/g, ' ')
    .split(/\s+/).filter(Boolean)
    .sort()
    .join(' ');
}

function _toFloat(val) {
  if (val == null || val === '') return null;
  const n = parseFloat(val);
  return isNaN(n) ? null : n;
}

function parseStatsHubApiData(refs) {
  return refs.map(r => ({
    nombre:                r.name,
    normalizado:           normalizeRefName(r.name),
    partidos:              parseInt(r.games) || null,
    amarillas_por_partido: _toFloat(r.avg_yellow_cards_per_game),
    rojas_por_partido:     _toFloat(r.avg_red_cards_per_game),
    avg_tarjetas:          _toFloat(r.avg_cards_per_game),
    tarjetas_1T:           _toFloat(r.avg_first_half_cards),
    tarjetas_2T:           _toFloat(r.avg_second_half_cards),
    penaltis_por_partido:  (r.total_penalties && r.games)
                             ? +(parseInt(r.total_penalties) / parseInt(r.games)).toFixed(2) : null,
    faltas_por_partido:    _toFloat(r.avg_fouls_per_game),
    pct_over35_tarjetas:   _toFloat(r.o35_cards_pct),
    pct_over45_tarjetas:   _toFloat(r.o45_cards_pct),
    pct_over55_tarjetas:   _toFloat(r.o55_cards_pct),
    pct_btc:               _toFloat(r.both_teams_card_pct),
    pct_btc2:              _toFloat(r.both_teams_o15_card_pct),
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

// Reemplaza WorldReferee: misma interfaz, datos de StatsHub
const _refereeStatsCache = new Map();

// Busca árbitro en StatsHub (ya cargado en memoria al startup).
// Mantiene la misma interfaz que antes para no romper callers existentes.
async function getRefereeCardStats(name) {
  if (!name) return null;
  if (_refereeStatsCache.has(name)) return _refereeStatsCache.get(name);

  const sh = findStatsHubReferee(name);
  if (sh) {
    console.log(`🃏 StatsHub [${name} → ${sh.nombre}]: ${sh.amarillas_por_partido} am/p, ${sh.avg_tarjetas} total/p (${sh.partidos} partidos)`);
    const result = {
      nombre:                name,
      partidos:              sh.partidos,
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
      _muestraInsuficiente:  sh.partidos != null && sh.partidos < 15,
    };
    _refereeStatsCache.set(name, result);
    setTimeout(() => _refereeStatsCache.delete(name), 24 * 3_600_000);
    return result;
  }

  console.log(`⚠️ StatsHub: sin match para "${name}" (${statsHubReferees.length} árbitros cargados)`);
  _refereeStatsCache.set(name, null);
  return null;
}
// ─────────────────────────────────────────────────────────────────────────────

function safeNum(obj, ...paths) {
  for (const path of paths) {
    const parts = path.split('.');
    let val = obj;
    for (const p of parts) val = val?.[p];
    if (val != null && !isNaN(parseFloat(val))) return parseFloat(val);
  }
  return null;
}

let _teamStatsLogged = false;

async function getTeamStats(teamId, leagueId) {
  const season = LEAGUE_SEASONS[leagueId] || 2025;
  const fromDate = `${season - 1}-06-01`;
  try {
    const { data } = await API.get('/teams/statistics/' + teamId, { params: { fromDate } });
    if (!data || data.length === 0) return null;

    const leagueStat = data.find(s => s.leagueId === leagueId)
      || data.sort((a, b) => (b.total?.games?.played || 0) - (a.total?.games?.played || 0))[0];
    if (!leagueStat) return null;

    if (!_teamStatsLogged) {
      _teamStatsLogged = true;
      console.log('📊 [DEBUG] TeamStats home keys:', JSON.stringify(Object.keys(leagueStat.home || {})));
      console.log('📊 [DEBUG] TeamStats home sample:', JSON.stringify(leagueStat.home, null, 2).slice(0, 1500));
    }

    const { home, away, total } = leagueStat;
    const hP = home.games?.played || 1;
    const aP = away.games?.played || 1;
    const tP = total.games?.played || (hP + aP) || 1;

    const csH  = safeNum(home,  'goals.cleanSheets', 'cleanSheets', 'goals.clean_sheets', 'clean_sheets');
    const csA  = safeNum(away,  'goals.cleanSheets', 'cleanSheets', 'goals.clean_sheets', 'clean_sheets');
    const ftsH = safeNum(home,  'goals.failedToScore', 'failedToScore', 'goals.failed_to_score', 'failed_to_score');
    const ftsA = safeNum(away,  'goals.failedToScore', 'failedToScore', 'goals.failed_to_score', 'failed_to_score');

    const cornH = safeNum(home,  'corners.total', 'corners.count', 'corners', 'cornerKicks', 'corner_kicks');
    const cornA = safeNum(away,  'corners.total', 'corners.count', 'corners', 'cornerKicks', 'corner_kicks');
    const cornT = safeNum(total, 'corners.total', 'corners.count', 'corners', 'cornerKicks', 'corner_kicks');

    const yelH = safeNum(home, 'cards.yellow', 'yellowCards', 'yellow_cards', 'cards.yellowCards');
    const yelA = safeNum(away, 'cards.yellow', 'yellowCards', 'yellow_cards', 'cards.yellowCards');
    const redH = safeNum(home, 'cards.red',    'redCards',    'red_cards',    'cards.redCards');
    const redA = safeNum(away, 'cards.red',    'redCards',    'red_cards',    'cards.redCards');

    const shotsH   = safeNum(home, 'shots.total', 'shots', 'totalShots', 'shots.on_target');
    const shotsA   = safeNum(away, 'shots.total', 'shots', 'totalShots', 'shots.on_target');
    const shotsOnH = safeNum(home, 'shots.on_target', 'shotsOnTarget', 'shots_on_target');
    const shotsOnA = safeNum(away, 'shots.on_target', 'shotsOnTarget', 'shots_on_target');

    const posH = safeNum(home, 'possession', 'ballPossession', 'ball_possession');
    const posA = safeNum(away, 'possession', 'ballPossession', 'ball_possession');

    return {
      liga:      leagueStat.leagueName,
      temporada: leagueStat.season,
      partidos:  { home: hP, away: aP, total: tP },

      golesAnotadosHome:  +(home.goals.scored   / hP).toFixed(2),
      golesAnotadosAway:  +(away.goals.scored   / aP).toFixed(2),
      golesRecibidosHome: +(home.goals.received / hP).toFixed(2),
      golesRecibidosAway: +(away.goals.received / aP).toFixed(2),

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

      ...(shotsH   != null && { tirosHome:     +(shotsH   / hP).toFixed(2) }),
      ...(shotsA   != null && { tirosAway:     +(shotsA   / aP).toFixed(2) }),
      ...(shotsOnH != null && { tirosArcoHome: +(shotsOnH / hP).toFixed(2) }),
      ...(shotsOnA != null && { tirosArcoAway: +(shotsOnA / aP).toFixed(2) }),

      ...(posH != null && { posesionHome: posH > 1 ? posH : +(posH * 100).toFixed(1) }),
      ...(posA != null && { posesionAway: posA > 1 ? posA : +(posA * 100).toFixed(1) }),

      victorias: { total: total.games.wins,  home: home.games.wins,  away: away.games.wins  },
      empates:   { total: total.games.draws, home: home.games.draws, away: away.games.draws },
      derrotas:  { total: total.games.loses, home: home.games.loses, away: away.games.loses },
    };
  } catch (e) {
    console.error(`getTeamStats(${teamId}, ${leagueId}): ${e.message}`);
    return null;
  }
}

async function getNationalTeamRecentStats(teamId, last = 20) {
  try {
    const fixtures = await getTeamLastFixtures(teamId, last);
    if (fixtures.length < 3) return null;

    let golesAFavor = 0, golesEnContra = 0, partidos = 0;
    let victorias = 0, empates = 0, derrotas = 0;
    let bttsCount = 0, cleanSheets = 0, failedToScore = 0;
    let formStr = '';

    for (const f of fixtures) {
      const isHome = f.homeId === teamId;
      const gF  = isHome ? f.goalsHome : f.goalsAway;
      const gC  = isHome ? f.goalsAway : f.goalsHome;
      if (gF === null || gF === undefined || gC === null || gC === undefined) continue;
      golesAFavor   += gF;
      golesEnContra += gC;
      partidos++;
      if (gF > gC)       { victorias++; formStr += 'G'; }
      else if (gF < gC)  { derrotas++;  formStr += 'P'; }
      else               { empates++;   formStr += 'E'; }
      if (gF > 0 && gC > 0) bttsCount++;
      if (gC === 0) cleanSheets++;
      if (gF === 0) failedToScore++;
    }
    if (partidos === 0) return null;

    // Corners y tarjetas: últimos 5 partidos vía getFixtureStatistics
    const recentFive = fixtures.slice(0, 5);
    const statResults = await Promise.allSettled(
      recentFive.map(f => getFixtureStatistics(f.id))
    );
    let cornersTotal = 0, cornersCount = 0;
    let yellTotal = 0, redTotal = 0, cardsCount = 0;
    statResults.forEach((r, idx) => {
      if (r.status !== 'fulfilled' || !r.value) return;
      const st = r.value;
      const isH = recentFive[idx]?.homeId === teamId;
      const tSt = isH ? homeStats(st) : awayStats(st);
      const corn = tSt['Corner Kicks'];
      const yell = tSt['Yellow Cards'];
      const red  = tSt['Red Cards'];
      if (corn != null) { cornersTotal += corn; cornersCount++; }
      if (yell != null || red != null) {
        yellTotal += yell || 0;
        redTotal  += red  || 0;
        cardsCount++;
      }
    });

    const avgFor     = +(golesAFavor   / partidos).toFixed(2);
    const avgAgainst = +(golesEnContra / partidos).toFixed(2);
    const forma5     = formStr.slice(-5).split('').join('-');
    const wins5      = (formStr.slice(-5).match(/G/g) || []).length;
    const draws5     = (formStr.slice(-5).match(/E/g) || []).length;
    const losses5    = (formStr.slice(-5).match(/P/g) || []).length;
    const pts5       = wins5 * 3 + draws5;

    return {
      equipo:              `Team ${teamId}`,
      liga:                'Internacional (últimos partidos)',
      temporada:           'multi',
      partidosJugados:     partidos,
      _partidosAnalizados: partidos,
      nota:                `Stats derivadas de los últimos ${partidos} partidos en todas las competiciones internacionales`,
      forma:               formStr.slice(-6).split('').join('-'),
      forma5:              { forma: forma5, victorias: wins5, empates: draws5, derrotas: losses5, puntos: pts5,
                             nota: pts5 >= 12 ? 'Forma excelente' : pts5 >= 9 ? 'Forma buena' : pts5 >= 6 ? 'Forma regular' : 'Forma mala' },
      golesAnotadosHome:   String(avgFor),
      golesAnotadosAway:   String(avgFor),
      golesRecibidosHome:  String(avgAgainst),
      golesRecibidosAway:  String(avgAgainst),
      cleanSheetsHome:     cleanSheets,
      cleanSheetsAway:     cleanSheets,
      failedToScoreHome:   failedToScore,
      failedToScoreAway:   failedToScore,
      victorias:           { total: victorias },
      empates:             { total: empates },
      derrotas:            { total: derrotas },
      bttsRate:            `${Math.round(bttsCount / partidos * 100)}%`,
      ...(cornersCount > 0 && { cornersPerGame: +(cornersTotal / cornersCount).toFixed(2) }),
      ...(cardsCount   > 0 && {
        tarjetasAmPG: +(yellTotal / cardsCount).toFixed(2),
        tarjetasRoPG: +(redTotal  / cardsCount).toFixed(2),
      }),
    };
  } catch (e) {
    console.warn(`⚠️ getNationalTeamRecentStats(${teamId}): ${e.message}`);
    return null;
  }
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

// Añade alias semánticos a stats de equipo local (jugando en casa)
function withHomeContext(stats) {
  if (!stats) return null;
  return {
    ...stats,
    amarillasPorPartido: stats.tarjetasAmHome ?? null,
    cornersxP:           stats.cornersPerGameHome ?? stats.cornersPerGame ?? null,
  };
}

// Añade alias semánticos a stats de equipo visitante (jugando fuera)
function withAwayContext(stats) {
  if (!stats) return null;
  return {
    ...stats,
    amarillasPorPartido: stats.tarjetasAmAway ?? null,
    cornersxP:           stats.cornersPerGameAway ?? stats.cornersPerGame ?? null,
  };
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

/**
 * Convierte la forma reciente (últimos 5 partidos) en un multiplicador para el lambda.
 * 9 pts  (3W‑0D‑2L)  = temporada normal → factor 1.00 (sin cambio)
 * 15 pts (5W)         = racha excelente  → factor 1.20 (+20%)
 *  0 pts (5L)         = racha terrible   → factor 0.80 (−20%)
 * Blend aplicado: 50% promedios de temporada + 50% forma ponderada.
 */
function formMultiplier(forma5) {
  if (!forma5 || forma5.puntos == null) return 1.0;
  const pts = Math.max(0, Math.min(15, forma5.puntos));
  // Lineal: factor = 1.0 + (pts − 9) / 30  → [0.70, 1.20] clampado a [0.80, 1.20]
  return Math.max(0.80, Math.min(1.20, 1.0 + (pts - 9) / 30));
}

function calcPoissonProbs(homeFor, homeAgainst, awayFor, awayAgainst) {
  const homeLambda = ((homeFor || 1.2) + (awayAgainst || 1.2)) / 2;
  const awayLambda = ((awayFor || 1.0) + (homeAgainst || 1.0)) / 2;

  const MAX = 7;
  let pHomeWin = 0, pDraw = 0, pAwayWin = 0;
  let pBtts = 0, pOver05 = 0, pOver15 = 0, pOver25 = 0, pOver35 = 0, pOver45 = 0;
  let pUnder15 = 0, pUnder25 = 0, pUnder35 = 0;
  let pHomeScore = 0, pAwayScore = 0;

  for (let h = 0; h < MAX; h++) {
    for (let a = 0; a < MAX; a++) {
      const p = poissonPMF(homeLambda, h) * poissonPMF(awayLambda, a);
      const total = h + a;
      if (h > a) pHomeWin += p;
      else if (h === a) pDraw += p;
      else pAwayWin += p;
      if (h > 0 && a > 0) pBtts += p;
      if (h > 0) pHomeScore += p;
      if (a > 0) pAwayScore += p;
      if (total >= 1) pOver05 += p;
      if (total >= 2) pOver15 += p;
      if (total >= 3) pOver25 += p;
      if (total >= 4) pOver35 += p;
      if (total >= 5) pOver45 += p;
      if (total <= 1) pUnder15 += p;
      if (total <= 2) pUnder25 += p;
      if (total <= 3) pUnder35 += p;
    }
  }

  // Portería a cero: P(equipo no recibe goles) = Poisson(lambda, 0) = e^(-lambda)
  const pCleanSheetHome = poissonPMF(awayLambda, 0);
  const pCleanSheetAway = poissonPMF(homeLambda, 0);

  const pDnbHome = pHomeWin / (pHomeWin + pAwayWin);
  const pDnbAway = pAwayWin / (pHomeWin + pAwayWin);

  return {
    homeLambda: +homeLambda.toFixed(2),
    awayLambda: +awayLambda.toFixed(2),
    homeWin:       +(pHomeWin       * 100).toFixed(1),
    draw:          +(pDraw          * 100).toFixed(1),
    awayWin:       +(pAwayWin       * 100).toFixed(1),
    btts:          +(pBtts          * 100).toFixed(1),
    homeToScore:   +(pHomeScore     * 100).toFixed(1),
    awayToScore:   +(pAwayScore     * 100).toFixed(1),
    over05:        +(pOver05        * 100).toFixed(1),
    over15:        +(pOver15        * 100).toFixed(1),
    over25:        +(pOver25        * 100).toFixed(1),
    over35:        +(pOver35        * 100).toFixed(1),
    over45:        +(pOver45        * 100).toFixed(1),
    under15:       +(pUnder15       * 100).toFixed(1),
    under25:       +(pUnder25       * 100).toFixed(1),
    under35:       +(pUnder35       * 100).toFixed(1),
    under05Home:   +(pCleanSheetAway * 100).toFixed(1),
    under05Away:   +(pCleanSheetHome * 100).toFixed(1),
    dnbHome:       +(pDnbHome       * 100).toFixed(1),
    dnbAway:       +(pDnbAway       * 100).toFixed(1),
  };
}

/**
 * Extiende calcPoissonProbs con probabilidades de 1er tiempo y corners.
 * Necesario para pick selection multi-mercado sin depender del LLM.
 */
function calcExtendedProbs(homeFor, homeAgainst, awayFor, awayAgainst, liga = '') {
  const base = calcPoissonProbs(homeFor, homeAgainst, awayFor, awayAgainst);

  // ── HT: escalar lambdas al primer tiempo (≈45% de los goles FT)
  // Ligeramente menos que 50% porque hay sesgo hacia la 2a mitad
  const htHomeLambda = base.homeLambda * 0.45;
  const htAwayLambda = base.awayLambda * 0.45;

  let htHomeWin = 0, htDraw = 0, htAwayWin = 0, htOver05 = 0, htOver15 = 0;
  const MAX = 7;
  for (let h = 0; h < MAX; h++) {
    for (let a = 0; a < MAX; a++) {
      const p = poissonPMF(htHomeLambda, h) * poissonPMF(htAwayLambda, a);
      if (h > a)      htHomeWin += p;
      else if (h===a) htDraw    += p;
      else            htAwayWin += p;
      if (h + a >= 1) htOver05  += p;
      if (h + a >= 2) htOver15  += p;
    }
  }

  // ── Corners: estimación basada en xG total
  // Internacionales (Mundial, Euros, Copa América, Nations League) promedian ~2 corners menos
  const _isIntl = /world cup|copa del mundo|mundial|copa am[eé]rica|nations league|euro\b|eurocopa|concacaf gold cup|gold cup|copa oro/i.test(liga);
  const _cornersBase = _isIntl ? 7.5 : 8.0;
  const _cornersMult = _isIntl ? 2.0 : 3.0;
  const xGTotal       = base.homeLambda + base.awayLambda;
  const cornersLambda = Math.max(5, Math.min(14, _cornersBase + (xGTotal - 2.0) * _cornersMult));

  // Corners por equipo: local típicamente saca ~55% de los corners
  const homeCornersLambda = cornersLambda * 0.55;
  const awayCornersLambda = cornersLambda * 0.45;

  return {
    ...base,
    htHomeWin:      +(htHomeWin * 100).toFixed(1),
    htDraw:         +(htDraw    * 100).toFixed(1),
    htAwayWin:      +(htAwayWin * 100).toFixed(1),
    htOver05:       +(htOver05  * 100).toFixed(1),
    htOver15:       +(htOver15  * 100).toFixed(1),
    cornersLambda:  +cornersLambda.toFixed(1),
    cornersOver65:  +(poissonCDF_above(cornersLambda,  7) * 100).toFixed(1),
    cornersOver75:  +(poissonCDF_above(cornersLambda,  8) * 100).toFixed(1),
    cornersOver85:  +(poissonCDF_above(cornersLambda,  9) * 100).toFixed(1),
    cornersOver95:  +(poissonCDF_above(cornersLambda, 10) * 100).toFixed(1),
    cornersOver105: +(poissonCDF_above(cornersLambda, 11) * 100).toFixed(1),
    cornersOver115: +(poissonCDF_above(cornersLambda, 12) * 100).toFixed(1),
    homeCorners35:  +(poissonCDF_above(homeCornersLambda, 4) * 100).toFixed(1),
    homeCorners45:  +(poissonCDF_above(homeCornersLambda, 5) * 100).toFixed(1),
    awayCorners25:  +(poissonCDF_above(awayCornersLambda, 3) * 100).toFixed(1),
    awayCorners35:  +(poissonCDF_above(awayCornersLambda, 4) * 100).toFixed(1),
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
  // Usar marcadores explícitos de local/visitante fijados en getFixtureStatistics
  const homeKey = stats._homeTeam || Object.keys(stats).filter(k => !k.startsWith('_'))[0];
  const awayKey = stats._awayTeam || Object.keys(stats).filter(k => !k.startsWith('_'))[1];
  if (!homeKey || !awayKey) return null;

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
// Generic linear projection (kept for backward compatibility only)
function calcLiveProjection(current, elapsed, total = 90) {
  if (!elapsed || elapsed <= 0) return null;
  const pace = current / elapsed;
  const projected = pace * total;
  const remaining = (total - elapsed) * pace;
  const confidence = elapsed >= 30 ? 'alta' : elapsed >= 15 ? 'media' : 'baja';
  return {
    current:    current,
    projected:  +projected.toFixed(1),
    remaining:  +remaining.toFixed(1),
    pace:       +(pace * 90).toFixed(1),
    confidence,
  };
}

/**
 * Proyección de TARJETAS con factor de regresión.
 * Cuando un equipo ya tiene 3+ amarillas, los jugadores se cuidan →
 * el ritmo del 2T cae significativamente vs el 1T.
 */
function calcCardsProjection(current, elapsed, homeCards = 0, awayCards = 0, total = 90) {
  if (!elapsed || elapsed <= 0) return null;
  const pace = current / elapsed;  // ritmo real en este momento
  // Factor de regresión: equipo con muchas amarillas → jugadores más cautelosos en 2T
  const maxTeamCards = Math.max(homeCards, awayCards);
  const regressionFactor =
    maxTeamCards >= 4 ? 0.45 :   // 4+ amarillas en un equipo → muy cautos en 2T
    maxTeamCards >= 3 ? 0.60 :   // 3 amarillas → bastante cautos
    maxTeamCards >= 2 ? 0.75 :   // 2 amarillas → algo más cuidadosos
    0.85;                         // 0-1 amarilla → ligera regresión natural 1T→2T
  const remaining = (total - elapsed) * pace * regressionFactor;
  const projected = current + remaining;
  const confidence = elapsed >= 30 ? 'alta' : elapsed >= 15 ? 'media' : 'baja';
  return {
    current,
    projected:        +projected.toFixed(1),
    remaining:        +remaining.toFixed(1),
    pace:             +(pace * 90).toFixed(1),
    regressionFactor,
    confidence,
    nota: maxTeamCards >= 3
      ? `⚠️ Factor regresión ${regressionFactor} aplicado — equipo con ${maxTeamCards} amarillas, jugadores más cautelosos en 2T`
      : null,
  };
}

/**
 * Calcula qué líneas de GOLES tienen valor real en vivo (prob 20-80%).
 * Usa Poisson con blend entre ritmo actual del partido e histórico de los equipos.
 */
function calcLiveGoalLines(currentGoals, elapsed, homeFor = 1.3, awayFor = 1.0) {
  if (!elapsed || elapsed <= 0) return null;
  const minutesRemaining = Math.max(1, 90 - elapsed);
  const currentPace    = currentGoals / elapsed;
  const historicalRate = (homeFor + awayFor) / 90;
  const blendWeight    = Math.min(elapsed / 70, 0.65);
  const blendedRate    = currentPace * blendWeight + historicalRate * (1 - blendWeight);
  const lambda         = blendedRate * minutesRemaining;

  const probAtLeast = (k) => {
    if (lambda <= 0) return k <= 0 ? 1 : 0;
    let cumul = 0;
    for (let i = 0; i < k; i++) cumul += poissonPMF(lambda, i);
    return 1 - cumul;
  };

  const lines = [];
  for (const extra of [0.5, 1.5, 2.5, 3.5]) {
    const totalLine  = currentGoals + extra;
    const needed     = Math.ceil(extra);
    const pOver      = +(probAtLeast(needed) * 100).toFixed(1);
    const pUnder     = +(100 - pOver).toFixed(1);
    // Límite 55%: por encima el mercado cobra ~1.35-1.48 (con margen) → sin valor real
    const overHasValue  = pOver  >= 20 && pOver  <= 55;
    const underHasValue = pUnder >= 20 && pUnder <= 55;
    const cuotaOver  = pOver  > 0 ? +(100 / pOver).toFixed(2)  : null;
    const cuotaUnder = pUnder > 0 ? +(100 / pUnder).toFixed(2) : null;
    lines.push({
      linea: `${totalLine}`,
      overProb: pOver, underProb: pUnder,
      cuotaOver, cuotaUnder,
      overValor: overHasValue, underValor: underHasValue,
      nota: pOver > 55
        ? `Over ${totalLine} muy probable (${pOver}%) — el mercado cobra ~${+(100/(pOver*1.12)).toFixed(2)}, sin valor`
        : pOver < 20
        ? `Over ${totalLine} muy improbable — sin valor`
        : `Over ${totalLine} (${pOver}%, cuota ~${cuotaOver}) | Under ${totalLine} (${pUnder}%, cuota ~${cuotaUnder})`,
    });
  }

  const lineasConValor = lines.filter(l => l.overValor || l.underValor);
  return {
    golesActuales:      currentGoals,
    minutosJugados:     elapsed,
    minutosRestantes:   minutesRemaining,
    golesEsperadosRest: +lambda.toFixed(2),
    proyeccionTotal:    +(currentGoals + lambda).toFixed(1),
    lineasConValor:     lineasConValor.length > 0
      ? lineasConValor
      : [{ nota: 'Sin líneas de goles con valor (cuota ≥ 1.50) en este momento — no recomendar' }],
  };
}

/**
 * Calcula qué líneas de CORNERS tienen valor real en vivo (prob 20-80%).
 * Usa Poisson con blend entre ritmo actual e histórico de corners.
 * Promedio histórico: ~10 corners/90 min.
 */
function calcLiveCornersLines(currentCorners, elapsed, chaseFactor = 1.08) {
  if (!elapsed || elapsed <= 0) return null;
  const minutesRemaining = Math.max(1, 90 - elapsed);
  const currentPace    = currentCorners / elapsed;
  const historicalRate = 10 / 90;  // ~10 corners/partido histórico
  const blendWeight    = Math.min(elapsed / 70, 0.65);
  const blendedRate    = currentPace * blendWeight + historicalRate * (1 - blendWeight);
  const lambda         = blendedRate * minutesRemaining * chaseFactor;

  const probAtLeast = (k) => {
    if (lambda <= 0) return k <= 0 ? 1 : 0;
    let cumul = 0;
    for (let i = 0; i < k; i++) cumul += poissonPMF(lambda, i);
    return 1 - cumul;
  };

  const lines = [];
  for (const extra of [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]) {
    const totalLine   = currentCorners + extra;
    const needed      = Math.ceil(extra);
    const pOver       = +(probAtLeast(needed) * 100).toFixed(1);
    const pUnder      = +(100 - pOver).toFixed(1);
    const overHasValue  = pOver  >= 20 && pOver  <= 55;
    const underHasValue = pUnder >= 20 && pUnder <= 55;
    const cuotaOver  = pOver  > 0 ? +(100 / pOver).toFixed(2)  : null;
    const cuotaUnder = pUnder > 0 ? +(100 / pUnder).toFixed(2) : null;
    lines.push({
      linea: `${totalLine}`,
      overProb: pOver, underProb: pUnder,
      cuotaOver, cuotaUnder,
      overValor: overHasValue, underValor: underHasValue,
      nota: pOver > 55
        ? `Over ${totalLine} muy probable (${pOver}%) — el mercado cobra ~${+(100/(pOver*1.12)).toFixed(2)}, sin valor`
        : pOver < 20
        ? `Over ${totalLine} muy improbable — sin valor`
        : `Over ${totalLine} (${pOver}%, cuota est. ~${cuotaOver}) | Under ${totalLine} (${pUnder}%, cuota est. ~${cuotaUnder})`,
    });
  }

  const lineasConValor = lines.filter(l => l.overValor || l.underValor);
  return {
    cornersActuales:  currentCorners,
    minutosJugados:   elapsed,
    lambdaRestante:   +lambda.toFixed(2),
    proyeccionTotal:  +(currentCorners + lambda).toFixed(1),
    lineasConValor:   lineasConValor.length > 0
      ? lineasConValor
      : [{ nota: 'Sin líneas de corners con valor (cuota ≥ 1.50) en este momento — no recomendar' }],
  };
}

/**
 * Calcula qué líneas de TARJETAS tienen valor real en vivo (prob 20-80%).
 * Usa Poisson con factor de regresión (equipos con amarillas se cuidan en 2T).
 */
function calcLiveCardsLines(currentCards, elapsed, regressionFactor = 0.85) {
  if (!elapsed || elapsed <= 0) return null;
  const minutesRemaining = Math.max(1, 90 - elapsed);
  const currentPace    = currentCards / elapsed;
  const historicalRate = 4 / 90;  // ~4 tarjetas/partido histórico
  const blendWeight    = Math.min(elapsed / 70, 0.65);
  const blendedRate    = currentPace * blendWeight + historicalRate * (1 - blendWeight);
  const lambda         = blendedRate * minutesRemaining * regressionFactor;

  const probAtLeast = (k) => {
    if (lambda <= 0) return k <= 0 ? 1 : 0;
    let cumul = 0;
    for (let i = 0; i < k; i++) cumul += poissonPMF(lambda, i);
    return 1 - cumul;
  };

  const lines = [];
  for (const extra of [0.5, 1.5, 2.5, 3.5, 4.5]) {
    const totalLine   = currentCards + extra;
    const needed      = Math.ceil(extra);
    const pOver       = +(probAtLeast(needed) * 100).toFixed(1);
    const pUnder      = +(100 - pOver).toFixed(1);
    const overHasValue  = pOver  >= 20 && pOver  <= 55;
    const underHasValue = pUnder >= 20 && pUnder <= 55;
    const cuotaOver  = pOver  > 0 ? +(100 / pOver).toFixed(2)  : null;
    const cuotaUnder = pUnder > 0 ? +(100 / pUnder).toFixed(2) : null;
    lines.push({
      linea: `${totalLine}`,
      overProb: pOver, underProb: pUnder,
      cuotaOver, cuotaUnder,
      overValor: overHasValue, underValor: underHasValue,
      nota: pOver > 55
        ? `Over ${totalLine} muy probable (${pOver}%) — el mercado cobra ~${+(100/(pOver*1.12)).toFixed(2)}, sin valor`
        : pOver < 20
        ? `Over ${totalLine} muy improbable — sin valor`
        : `Over ${totalLine} (${pOver}%, cuota est. ~${cuotaOver}) | Under ${totalLine} (${pUnder}%, cuota est. ~${cuotaUnder})`,
    });
  }

  const lineasConValor = lines.filter(l => l.overValor || l.underValor);
  return {
    tarjetasActuales: currentCards,
    minutosJugados:   elapsed,
    lambdaRestante:   +lambda.toFixed(2),
    proyeccionTotal:  +(currentCards + lambda).toFixed(1),
    lineasConValor:   lineasConValor.length > 0
      ? lineasConValor
      : [{ nota: 'Sin líneas de tarjetas con valor (cuota ≥ 1.50) en este momento — no recomendar' }],
  };
}

/**
 * Proyección de CORNERS con factor de "caza de resultado".
 * El equipo que va perdiendo empuja más → más corners en 2T.
 * El 2T en general genera ~10-15% más corners que el 1T (presión final).
 */
function calcCornersProjection(current, elapsed, homeGoals = 0, awayGoals = 0, total = 90) {
  if (!elapsed || elapsed <= 0) return null;
  const pace = current / elapsed;
  const scoreDiff = Math.abs(homeGoals - awayGoals);
  // El equipo que pierde busca el empate → más corners en 2T
  const chaseFactor =
    scoreDiff >= 2 ? 1.30 :   // partido muy definido → equipo perdedor presiona fuerte
    scoreDiff >= 1 ? 1.18 :   // un gol de ventaja → cierta persecución
    1.08;                      // empate → ligero incremento natural de corners en 2T
  const remaining = (total - elapsed) * pace * chaseFactor;
  const projected = current + remaining;
  const confidence = elapsed >= 30 ? 'alta' : elapsed >= 15 ? 'media' : 'baja';
  return {
    current,
    projected:   +projected.toFixed(1),
    remaining:   +remaining.toFixed(1),
    pace:        +(pace * 90).toFixed(1),
    chaseFactor,
    confidence,
  };
}

/**
 * Calcula promedios de goles desde los últimos N partidos (cross-competición).
 * Fallback cuando la liga tiene muestra pequeña (ej: Mundial fase de grupos).
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
 * @param {number} leagueId     - ID de la liga
 * @param {object} homeFallback - Promedios calculados desde últimos 20 partidos (fallback)
 * @param {object} awayFallback - Promedios calculados desde últimos 20 partidos (fallback)
 * @returns {object} bloque de probabilidades para incluir en el prompt
 */
function buildProbBlock(homeStats, awayStats, h2h = [], leagueId = null, homeFallback = null, awayFallback = null) {
  if (!homeStats && !homeFallback) return null;
  if (!awayStats && !awayFallback) return null;

  // Si la liga tiene menos de 5 partidos, usa los últimos 20 como base
  const homeGames = homeStats
    ? ((homeStats.victorias?.total || 0) + (homeStats.empates?.total || 0) + (homeStats.derrotas?.total || 0))
    : 0;
  const awayGames = awayStats
    ? ((awayStats.victorias?.total || 0) + (awayStats.empates?.total || 0) + (awayStats.derrotas?.total || 0))
    : 0;

  const hSrc = (homeGames < 5 && homeFallback) ? homeFallback : homeStats;
  const aSrc = (awayGames < 5 && awayFallback) ? awayFallback : awayStats;

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

  // Contexto de liga: tasas base históricas
  const leagueRates = LEAGUE_BASE_RATES[leagueId] || null;
  const leagueContext = leagueRates ? {
    tasaBaseOver25Liga: `${leagueRates.over25}%`,
    tasaBaseBTTSLiga:   `${leagueRates.btts}%`,
    alertaLiga: leagueRates.over25 < 52 ? `Liga defensiva — reducir confianza en Over/BTTS` : null,
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
    expectedValue_vs_CuotasReferencia: {
      'Over 2.5 @ 1.85': ev.over25 !== null ? `${ev.over25 > 0 ? '+' : ''}${ev.over25}%` : 'N/D',
      'BTTS @ 1.80':     ev.btts   !== null ? `${ev.btts   > 0 ? '+' : ''}${ev.btts}%`   : 'N/D',
      'Local Gana @ 1.70': ev.homeWin !== null ? `${ev.homeWin > 0 ? '+' : ''}${ev.homeWin}%` : 'N/D',
      'Over 3.5 @ 2.60': ev.over35 !== null ? `${ev.over35 > 0 ? '+' : ''}${ev.over35}%` : 'N/D',
      'DNB Local @ 1.50': ev.dnbHome !== null ? `${ev.dnbHome > 0 ? '+' : ''}${ev.dnbHome}%` : 'N/D',
    },
    nota: 'EV positivo = apuesta con valor real vs cuota de mercado. Usa estos datos para calibrar el stake.',
    ...(leagueContext && { contextoLiga: leagueContext }),
  };
}

// ─── Multi-market Pick Engine ─────────────────────────────────────────────────
// Selección matemática de picks usando EV real. El LLM solo formatea.

/**
 * Dado un array de fixtures enriquecidos (con _extendedProbs + cuotasReales),
 * evalúa todos los mercados disponibles y retorna candidatos con EV positivo,
 * ordenados de mayor a menor EV.
 */
function buildPickCandidates(enrichedFixtures) {
  const candidates = [];

  for (const f of enrichedFixtures) {
    // 'fallback' = ningún equipo tiene stats reales → ya filtrado antes de llegar aquí
    // 'local_only' / 'away_only' = un equipo sin datos → stake máximo 5 por _maxStake
    if (!f._extendedProbs) continue;

    // ── Filtro fin de temporada: ambos equipos sin nada en juego y ≤3 jornadas restantes ──
    {
      const motivL = f.motivacionLocal;
      const motivV = f.motivacionVisitante;
      const nada_L = motivL?.estado === 'nada_en_juego';
      const nada_V = motivV?.estado === 'nada_en_juego';
      const jorL   = motivL?.jornadas_restantes ?? 99;
      const jorV   = motivV?.jornadas_restantes ?? 99;
      if (nada_L && nada_V && (jorL <= 3 || jorV <= 3)) {
        console.log(`⏭️ SKIP fin-temporada (ambos sin nada en juego): ${f.local} vs ${f.visitante}`);
        continue;
      }
    }

    const partialStats = f._statsSource !== 'real'; // true si solo uno de los dos tiene stats
    const probs = f._extendedProbs;
    const odds  = f.cuotasReales || {};
    if (!probs) continue;

    // ── Definición de mercados a evaluar ──────────────────────────────────────
    // minOdds son umbrales INTERNOS (cuota real de mercado).
    // Al usuario se muestra cuota real + ODDS_DISPLAY_BUFFER.
    const markets = [
      // ── Resultado FT
      { key: 'homeWin',     label: 'Victoria Local',          prob: probs.homeWin  / 100, oddsVal: odds.homeWin,  cat: 'result',    minOdds: 1.50, minProb: 0.52 },
      { key: 'awayWin',     label: 'Victoria Visitante',      prob: probs.awayWin  / 100, oddsVal: odds.awayWin,  cat: 'result',    minOdds: 1.65, minProb: 0.50 },
      // ── Goles FT
      { key: 'over05',      label: 'Más de 0.5 Goles',        prob: probs.over05   / 100, oddsVal: odds.over05,   cat: 'goals',     minOdds: 1.10, minProb: 0.88 },
      { key: 'over15',      label: 'Más de 1.5 Goles',        prob: probs.over15   / 100, oddsVal: odds.over15,   cat: 'goals',     minOdds: 1.30, minProb: 0.72 },
      { key: 'over25',      label: 'Más de 2.5 Goles',        prob: probs.over25   / 100, oddsVal: odds.over25,   cat: 'goals',     minOdds: 1.45, minProb: 0.50 },
      { key: 'over35',      label: 'Más de 3.5 Goles',        prob: probs.over35   / 100, oddsVal: odds.over35,   cat: 'goals',     minOdds: 1.55, minProb: 0.43 },
      { key: 'over45',      label: 'Más de 4.5 Goles',        prob: probs.over45   / 100, oddsVal: odds.over45,   cat: 'goals',     minOdds: 1.80, minProb: 0.32 },
      { key: 'under15',     label: 'Menos de 1.5 Goles',      prob: probs.under15  / 100, oddsVal: odds.under15,  cat: 'goals',     minOdds: 1.55, minProb: 0.60 },
      { key: 'under25',     label: 'Menos de 2.5 Goles',      prob: probs.under25  / 100, oddsVal: odds.under25,  cat: 'goals',     minOdds: 1.45, minProb: 0.48 },
      { key: 'under35',     label: 'Menos de 3.5 Goles',      prob: probs.under35  / 100, oddsVal: odds.under35,  cat: 'goals',     minOdds: 1.30, minProb: 0.65 },
      // ── BTTS
      { key: 'btts',        label: 'Ambos Marcan (Sí)',        prob: probs.btts     / 100, oddsVal: odds.bttsYes,  cat: 'btts',      minOdds: 1.45, minProb: 0.52 },
      { key: 'bttsNo',      label: 'No Ambos Marcan',          prob: 1 - probs.btts / 100, oddsVal: odds.bttsNo,   cat: 'btts',      minOdds: 1.35, minProb: 0.58 },
      // ── Doble oportunidad (DC)
      { key: 'dc_1X', label: 'Doble Oportunidad 1X',          prob: (probs.homeWin + probs.draw) / 100, oddsVal: odds.dc_1X, cat: 'dc', minOdds: 1.30, minProb: 0.68 },
      { key: 'dc_X2', label: 'Doble Oportunidad X2',          prob: (probs.draw + probs.awayWin) / 100, oddsVal: odds.dc_X2, cat: 'dc', minOdds: 1.30, minProb: 0.65 },
      { key: 'dc_12', label: 'Doble Oportunidad 12 (sin empate)', prob: (probs.homeWin + probs.awayWin) / 100, oddsVal: odds.dc_12, cat: 'dc', minOdds: 1.25, minProb: 0.72 },
      // ── Draw No Bet
      { key: 'dnb_home', label: 'Draw No Bet — Local',        prob: probs.dnbHome / 100, oddsVal: odds.dnb_home, cat: 'dnb', minOdds: 1.30, minProb: 0.60 },
      { key: 'dnb_away', label: 'Draw No Bet — Visitante',    prob: probs.dnbAway / 100, oddsVal: odds.dnb_away, cat: 'dnb', minOdds: 1.40, minProb: 0.58 },
      // ── Hándicap asiático
      { key: 'ah_home_m05', label: 'Hándicap Asiático Local -0.5',       prob: probs.homeWin / 100,              oddsVal: odds.ah_home_m05, cat: 'ah', minOdds: 1.50, minProb: 0.60 },
      { key: 'ah_away_m05', label: 'Hándicap Asiático Visitante -0.5',   prob: probs.awayWin / 100,              oddsVal: odds.ah_away_m05, cat: 'ah', minOdds: 1.50, minProb: 0.58 },
      { key: 'ah_home_m15', label: 'Hándicap Asiático Local -1.5',       prob: (probs.over15 * probs.homeWin / 10000), oddsVal: odds.ah_home_m15, cat: 'ah', minOdds: 1.60, minProb: 0.45 },
      { key: 'ah_away_m15', label: 'Hándicap Asiático Visitante -1.5',   prob: (probs.over15 * probs.awayWin / 10000), oddsVal: odds.ah_away_m15, cat: 'ah', minOdds: 1.65, minProb: 0.42 },
      // ── Portería a cero
      { key: 'cleanSheetHome', label: 'Portería a Cero — Local',         prob: probs.under05Away / 100,         oddsVal: odds.cleanSheetHome, cat: 'cs', minOdds: 1.60, minProb: 0.45 },
      { key: 'cleanSheetAway', label: 'Portería a Cero — Visitante',     prob: probs.under05Home / 100,         oddsVal: odds.cleanSheetAway, cat: 'cs', minOdds: 1.80, minProb: 0.40 },
      // ── Local/Visitante anota
      { key: 'homeToScore',  label: 'Local Marca',                       prob: probs.homeToScore / 100, oddsVal: odds.homeToScore, cat: 'teamgoal', minOdds: 1.25, minProb: 0.70 },
      { key: 'awayToScore',  label: 'Visitante Marca',                   prob: probs.awayToScore / 100, oddsVal: odds.awayToScore, cat: 'teamgoal', minOdds: 1.35, minProb: 0.65 },
      // ── 1er tiempo — goles
      { key: 'ht_over05',   label: 'Gol en el 1er Tiempo',              prob: probs.htOver05 / 100, oddsVal: odds.over05_1T,   cat: 'ht_goals', minOdds: 1.35, minProb: 0.55 },
      { key: 'ht_over15',   label: 'Más de 1.5 Goles 1T',              prob: probs.htOver15 / 100, oddsVal: odds.over15_1T,   cat: 'ht_goals', minOdds: 1.55, minProb: 0.42 },
      { key: 'ht_under05',  label: 'Sin Goles en el 1er Tiempo',        prob: 1 - probs.htOver05 / 100, oddsVal: odds.under05_1T, cat: 'ht_goals', minOdds: 1.40, minProb: 0.55 },
      { key: 'ht_under15',  label: 'Menos de 1.5 Goles 1T',            prob: 1 - probs.htOver15 / 100, oddsVal: odds.under15_1T, cat: 'ht_goals', minOdds: 1.30, minProb: 0.65 },
      // ── 1er tiempo — resultado
      { key: 'homeWin_1T',  label: 'Local Gana el 1er Tiempo',          prob: probs.htHomeWin / 100, oddsVal: odds.homeWin_1T, cat: 'ht_result', minOdds: 1.50, minProb: 0.45 },
      { key: 'draw_1T',     label: 'Empate en el 1er Tiempo',           prob: probs.htDraw    / 100, oddsVal: odds.draw_1T,    cat: 'ht_result', minOdds: 1.55, minProb: 0.42 },
      { key: 'awayWin_1T',  label: 'Visitante Gana el 1er Tiempo',      prob: probs.htAwayWin / 100, oddsVal: odds.awayWin_1T, cat: 'ht_result', minOdds: 1.65, minProb: 0.38 },
      // ── Corners FT totales
      { key: 'cornersOver65',  label: 'Corners Over 6.5',   prob: probs.cornersOver65  / 100, oddsVal: odds.cornersOver65,  cat: 'corners', minOdds: 1.30, minProb: 0.62 },
      { key: 'cornersOver75',  label: 'Corners Over 7.5',   prob: probs.cornersOver75  / 100, oddsVal: odds.cornersOver75,  cat: 'corners', minOdds: 1.40, minProb: 0.56 },
      { key: 'cornersOver85',  label: 'Corners Over 8.5',   prob: probs.cornersOver85  / 100, oddsVal: odds.cornersOver85,  cat: 'corners', minOdds: 1.45, minProb: 0.52 },
      { key: 'cornersOver95',  label: 'Corners Over 9.5',   prob: probs.cornersOver95  / 100, oddsVal: odds.cornersOver95,  cat: 'corners', minOdds: 1.50, minProb: 0.45 },
      { key: 'cornersOver105', label: 'Corners Over 10.5',  prob: probs.cornersOver105 / 100, oddsVal: odds.cornersOver105, cat: 'corners', minOdds: 1.55, minProb: 0.40 },
      { key: 'cornersUnder75', label: 'Corners Under 7.5',  prob: 1 - probs.cornersOver65 / 100, oddsVal: odds.cornersUnder75, cat: 'corners', minOdds: 1.45, minProb: 0.50 },
      { key: 'cornersUnder85', label: 'Corners Under 8.5',  prob: 1 - probs.cornersOver75 / 100, oddsVal: odds.cornersUnder85, cat: 'corners', minOdds: 1.40, minProb: 0.52 },
      { key: 'cornersUnder95', label: 'Corners Under 9.5',  prob: 1 - probs.cornersOver85 / 100, oddsVal: odds.cornersUnder95, cat: 'corners', minOdds: 1.40, minProb: 0.55 },
      // ── Corners por equipo
      { key: 'homeCorners_over35', label: 'Corners Local Over 3.5',     prob: probs.homeCorners35 / 100, oddsVal: odds.homeCorners_over35, cat: 'team_corners', minOdds: 1.50, minProb: 0.52 },
      { key: 'homeCorners_over45', label: 'Corners Local Over 4.5',     prob: probs.homeCorners45 / 100, oddsVal: odds.homeCorners_over45, cat: 'team_corners', minOdds: 1.55, minProb: 0.45 },
      { key: 'awayCorners_over25', label: 'Corners Visitante Over 2.5', prob: probs.awayCorners25 / 100, oddsVal: odds.awayCorners_over25, cat: 'team_corners', minOdds: 1.50, minProb: 0.52 },
      { key: 'awayCorners_over35', label: 'Corners Visitante Over 3.5', prob: probs.awayCorners35 / 100, oddsVal: odds.awayCorners_over35, cat: 'team_corners', minOdds: 1.55, minProb: 0.45 },
      // ── Tarjetas FT — Poisson real basado en datos de equipo + árbitro + liga
      { _cardsBlock: true, key: 'cardsOver25', label: 'Tarjetas Over 2.5', oddsVal: odds.cardsOver25, cat: 'cards', minOdds: 1.40, minProb: 0.55 },
      { _cardsBlock: true, key: 'cardsOver35', label: 'Tarjetas Over 3.5', oddsVal: odds.cardsOver35, cat: 'cards', minOdds: 1.45, minProb: 0.42 },
      { _cardsBlock: true, key: 'cardsOver45', label: 'Tarjetas Over 4.5', oddsVal: odds.cardsOver45, cat: 'cards', minOdds: 1.65, minProb: 0.35 },
      { _cardsBlock: true, key: 'cardsUnder25', label: 'Tarjetas Under 2.5', oddsVal: odds.cardsUnder25, cat: 'cards', minOdds: 1.50, minProb: 0.48 },
      // ── Tarjetas por equipo
      { _cardsBlock: true, key: 'homeCardsOver15', label: 'Tarjetas Local Over 1.5', oddsVal: odds.homeCardsOver15, cat: 'team_cards', minOdds: 1.50, minProb: 0.50 },
      { _cardsBlock: true, key: 'awayCardsOver15', label: 'Tarjetas Visitante Over 1.5', oddsVal: odds.awayCardsOver15, cat: 'team_cards', minOdds: 1.55, minProb: 0.48 },
      // ── Goals Both Halves
      { key: 'goalsBothHalves', label: 'Goles en Ambas Mitades',        prob: probs.btts * 0.75 / 100, oddsVal: odds.goalsBothHalves, cat: 'both_halves', minOdds: 1.55, minProb: 0.45 },
    ];

    // ── Cálculo de lambda real para tarjetas (Poisson) ──────────────────────────
    // Usa amarillasPorPartido de cada equipo + perfil árbitro + base rate de liga
    const baseRates = LEAGUE_BASE_RATES[f.leagueId] || null;
    const homeCardsAvg  = parseFloat(f.statsLocal?.amarillasPorPartido)     || null;
    const awayCardsAvg  = parseFloat(f.statsVisitante?.amarillasPorPartido) || null;
    const refCardsAvg   = parseFloat(f.arbitroStats?.amarillas_por_partido) || null;
    const leagueCardsAvg = baseRates?.cards || 4.0;  // promedio de la liga o 4.0 global
    const sinDatosArbitro = refCardsAvg === null; // true = árbitro sin stats confirmadas

    // ── Contexto de motivación: fin de temporada sin nada en juego = menos tarjetas ──
    const motivLocal = f.motivacionLocal?.estado || 'desconocido';
    const motivVisit = f.motivacionVisitante?.estado || 'desconocido';
    const sinPension = [motivLocal, motivVisit].every(m =>
      ['nada_en_juego','clasifica_champions_posible_asegurado','desconocido'].includes(m)
    );
    // Si ambos equipos sin presión real → el partido es relajado → -15% en lambda de tarjetas
    const motivFactor = sinPension ? 0.85 : 1.0;

    // Lambda de tarjetas: promedio de ambos equipos, ajustado por árbitro si disponible
    let cardsLambda;
    if (homeCardsAvg != null && awayCardsAvg != null) {
      // Datos reales de equipo disponibles
      const teamAvg = homeCardsAvg + awayCardsAvg;
      // Árbitro: normalizar vs promedio liga → factor de corrección
      // Sin datos de árbitro: usamos factor 0.90 (penalización por incertidumbre, no 1.0 neutro)
      const refFactor = refCardsAvg != null ? refCardsAvg / leagueCardsAvg : 0.90;
      cardsLambda = teamAvg * Math.max(0.60, Math.min(1.60, refFactor)) * motivFactor;
    } else {
      // Sin datos de equipo → usar base rate de liga ajustada por árbitro
      const refFactor = refCardsAvg != null ? refCardsAvg / leagueCardsAvg : 0.90;
      cardsLambda = leagueCardsAvg * Math.max(0.60, Math.min(1.60, refFactor)) * motivFactor;
    }
    // P(X >= N) usando Poisson con lambda = cardsLambda
    const pCardsOver25   = poissonCDF_above(cardsLambda, 3);
    const pCardsOver35   = poissonCDF_above(cardsLambda, 4);
    const pCardsOver45   = poissonCDF_above(cardsLambda, 5);
    const pCardsUnder25  = 1 - pCardsOver25;
    // Tarjetas por equipo: cada equipo recibe ~45% de las tarjetas totales
    const pHomeCardsO15  = poissonCDF_above(cardsLambda * 0.52, 2);
    const pAwayCardsO15  = poissonCDF_above(cardsLambda * 0.48, 2);

    // Señales ZCode Soccer Buddy para este fixture
    const _zb  = f.soccerBuddy    || null;
    // Señales de mercado: Line Reversals + Dropping Odds
    const _mkt = f.mercadoZCode   || null;
    // Sharp money precomputado para boost directo
    const _sharpLocal   = _mkt?.lineReversal?.sharpEnLocal === true;
    const _sharpVisit   = _mkt?.lineReversal?.sharpEnLocal === false;
    const _dropLocal    = _mkt?.droppingOddsLocal?.drop    || 0;
    const _dropVisit    = _mkt?.droppingOddsVisitante?.drop || 0;
    // Señales extra: Totals Predictor, Power Rankings, Soccer Oscillator, Contrarian Bets, EV Tool
    const _extra        = f.zcodeExtra  || null;
    const _tpTotal      = _extra?.totalsPredictor?.predictedTotal ?? null;
    const _powerGap     = (_extra?.powerHome != null && _extra?.powerAway != null)
                            ? (_extra.powerHome - _extra.powerAway) : null;
    const _homeHot      = _extra?.oscHome?.hot  === true;
    const _homeCold     = _extra?.oscHome?.cold === true;
    const _awayHot      = _extra?.oscAway?.hot  === true;
    const _awayCold     = _extra?.oscAway?.cold === true;
    const _contrarianSide = _extra?.contrarian?.side ?? null; // 'home' | 'away'
    const _evRank       = _extra?.evTool?.rank  ?? null;      // posición en EV Tool (menor = mejor)

    for (const m of markets) {
      // ── Inyectar prob real de tarjetas (antes del check de odds) ─────────────
      if (m._cardsBlock) {
        m.prob = m.key === 'cardsOver25'     ? pCardsOver25
               : m.key === 'cardsOver35'     ? pCardsOver35
               : m.key === 'cardsOver45'     ? pCardsOver45
               : m.key === 'cardsUnder25'    ? pCardsUnder25
               : m.key === 'homeCardsOver15' ? pHomeCardsO15
               : m.key === 'awayCardsOver15' ? pAwayCardsO15
               : pCardsOver35;
      }

      if (!m.prob || m.prob < (m.minProb || 0.48)) continue; // prob insuficiente

      // ── Cuota real o cuota justa implícita (abre DC, DNB, HT, team corners) ──
      // Sin cuota real, usamos 1/(prob × 0.93) que refleja breakeven con margen 7%.
      // Estas "implied" odss no tienen valor real sobre el mercado — el stake se limita.
      const hasRealOdds = m.oddsVal != null && m.oddsVal > 1;
      const impliedFair = m.prob > 0.10 ? +(1 / (m.prob * 0.93)).toFixed(2) : null;
      const o = hasRealOdds ? m.oddsVal : impliedFair;
      if (!o || o <= 1) continue;

      // Piso mínimo para cuotas REALES: 1.65 global, 1.30 para DC/DNB
      if (hasRealOdds) {
        const catFloor = ['dc', 'dnb'].includes(m.cat) ? 1.30 : 1.65;
        if (o < catFloor) continue;
      }

      // ── Tarjetas: no hay modelo propio → solo evaluar si la cuota tiene valor ─
      // usamos prob heurística directamente (ya definida en cada tarjeta arriba)

      let evRaw = calcEV(m.prob, o);
      if (evRaw === null || evRaw < -5) continue;

      // ── ZCode Soccer Buddy: boost de EV por confirmación externa ─────────────
      if (_zb) {
        let zbBoost = 0;
        if (m.key === 'btts'      && _zb.btts_pct >= 68)  zbBoost += (_zb.btts_pct >= 78 ? 6 : 3);
        if (m.key === 'bttsNo'    && _zb.btts_pct != null && _zb.btts_pct <= 32) zbBoost += 3;
        if ((m.key === 'over25' || m.key === 'over15') && _zb.over25_pct >= 68) zbBoost += (_zb.over25_pct >= 78 ? 6 : 3);
        if (m.key === 'over35'    && _zb.over35_pct >= 60)  zbBoost += 3;
        if (m.key === 'under25'   && _zb.over25_pct != null && _zb.over25_pct <= 32) zbBoost += 3;
        if (m.key === 'ht_over05' && _zb.ht_over05_pct >= 72) zbBoost += 4;
        if (m.key === 'draw_1T'   && _zb.draw_pct >= 38)  zbBoost += 2;
        if (m.key === 'homeWin'   && _zb.home_win_pct >= 60) zbBoost += 3;
        if (m.key === 'awayWin'   && _zb.away_win_pct >= 58) zbBoost += 3;
        if (m.key === 'dnb_home'  && _zb.home_win_pct >= 58) zbBoost += 2;
        if (m.key === 'dnb_away'  && _zb.away_win_pct >= 55) zbBoost += 2;
        if (_zb.inValueBets) zbBoost += 3;  // ZCode lo marca como value bet → bonus universal  // ZCode lo marca como value bet → bonus universal
        // Score prediction confirma dirección del resultado
        const pred = (_zb.scorePred || '');
        if (pred) {
          const [hs, as] = pred.split(':').map(Number);
          if (!isNaN(hs) && !isNaN(as)) {
            if (m.key === 'homeWin'  && hs > as)  zbBoost += 2;
            if (m.key === 'awayWin'  && as > hs)  zbBoost += 2;
            if (m.key === 'btts'     && hs >= 1 && as >= 1) zbBoost += 2;
            if (m.key === 'over25'   && (hs + as) >= 3) zbBoost += 2;
            if (m.key === 'under25'  && (hs + as) <= 2) zbBoost += 2;
          }
        }
        if (zbBoost > 0) {
          evRaw = Math.min(evRaw + zbBoost, Math.max(evRaw * 1.5, evRaw + zbBoost));
        }
      }

      // ── Line Reversals + Dropping Odds: dinero sharp → boost de EV ───────────
      if (_mkt) {
        let mktBoost = 0;
        // Sharps en el local
        if (_sharpLocal) {
          if (['homeWin', 'dnb_home', 'dc_1X', 'ah_home_m05'].includes(m.key)) mktBoost += 4;
          if (['btts', 'over25'].includes(m.key)) mktBoost += 2; // partidos abiertos
        }
        // Sharps en el visitante
        if (_sharpVisit) {
          if (['awayWin', 'dnb_away', 'dc_X2', 'ah_away_m05'].includes(m.key)) mktBoost += 4;
          if (['btts', 'over25'].includes(m.key)) mktBoost += 2;
        }
        // Dropping odds local: cuota cayó = dinero entrando al local
        if (_dropLocal > 5  && ['homeWin', 'dnb_home'].includes(m.key)) mktBoost += Math.min(Math.floor(_dropLocal / 3), 5);
        if (_dropLocal > 10 && ['over25', 'btts'].includes(m.key))       mktBoost += 2;
        // Dropping odds visitante
        if (_dropVisit > 5  && ['awayWin', 'dnb_away'].includes(m.key)) mktBoost += Math.min(Math.floor(_dropVisit / 3), 5);
        if (_dropVisit > 10 && ['over25', 'btts'].includes(m.key))       mktBoost += 2;

        if (mktBoost > 0) {
          evRaw = evRaw + mktBoost;
          console.log(`💰 MktBoost ${f.local} vs ${f.visitante} [${m.key}] +${mktBoost} (sharp:${_sharpLocal?'L':_sharpVisit?'V':'-'} drop:${_dropLocal}/${_dropVisit}%)`);
        }
      }

      // ── Totals Predictor + Power Rankings + Soccer Oscillator + Contrarian ──────
      if (_extra) {
        let extraBoost = 0;

        // Totals Predictor: predicción de goles totales del partido
        if (_tpTotal !== null) {
          if (_tpTotal >= 2.8 && ['over25', 'btts'].includes(m.key))  extraBoost += 3;
          if (_tpTotal >= 3.3 && m.key === 'over35')                  extraBoost += 3;
          if (_tpTotal >= 3.5 && m.key === 'over25')                  extraBoost += 2; // doble boost si muy alto
          if (_tpTotal <= 2.0 && m.key === 'under25')                 extraBoost += 3;
          if (_tpTotal <= 1.8 && ['under25', 'btts'].includes(m.key)) {
            if (m.key === 'btts') extraBoost -= 2; // penalizar BTTS si bajo total predicho
          }
        }

        // Power Rankings: diferencia de fuerza → favorece al más fuerte
        if (_powerGap !== null) {
          if (_powerGap >  15) { // local claramente superior
            if (['homeWin', 'dnb_home', 'dc_1X'].includes(m.key)) extraBoost += Math.min(Math.round(_powerGap / 8), 4);
          }
          if (_powerGap < -15) { // visitante claramente superior
            if (['awayWin', 'dnb_away', 'dc_X2'].includes(m.key)) extraBoost += Math.min(Math.round(-_powerGap / 8), 4);
          }
        }

        // Soccer Oscillator: momentum / forma actual
        if (_homeHot && _awayCold) {
          if (['homeWin', 'dnb_home', 'dc_1X'].includes(m.key)) extraBoost += 3;
          if (['over25', 'btts'].includes(m.key))               extraBoost += 1;
        }
        if (_awayHot && _homeCold) {
          if (['awayWin', 'dnb_away', 'dc_X2'].includes(m.key)) extraBoost += 3;
          if (['over25', 'btts'].includes(m.key))               extraBoost += 1;
        }
        if (_homeHot && _awayHot) { // ambos en racha goleadora
          if (['over25', 'over35', 'btts'].includes(m.key))     extraBoost += 2;
        }

        // Contrarian Bets: fade al público → boost al lado contrario
        if (_contrarianSide === 'home') {
          if (['homeWin', 'dnb_home', 'dc_1X'].includes(m.key)) extraBoost += 4;
        }
        if (_contrarianSide === 'away') {
          if (['awayWin', 'dnb_away', 'dc_X2'].includes(m.key)) extraBoost += 4;
        }

        // EV Tool: partido en top-20 de ZCode EV list → boost global en todos sus mercados
        if (_evRank !== null) {
          const evBonus = _evRank <= 5  ? 5    // top 5: señal muy fuerte
                        : _evRank <= 10 ? 3    // top 10
                        : _evRank <= 20 ? 1    // top 20
                        : 0;
          if (evBonus > 0) extraBoost += evBonus;
        }

        if (extraBoost !== 0) {
          evRaw = evRaw + extraBoost;
          console.log(`🎯 ExtraBoost ${f.local} vs ${f.visitante} [${m.key}] ${extraBoost > 0 ? '+' : ''}${extraBoost} (tp:${_tpTotal} pr:${_powerGap?.toFixed(0)} osc:${_homeHot?'H🔥':_homeCold?'H❄':''}${_awayHot?'A🔥':_awayCold?'A❄':''} cb:${_contrarianSide||'-'} ev:#${_evRank||'-'})`);
        }
      }

      // ── Tier multiplier de liga: penaliza ligas de baja cobertura ────────────
      const leagueP   = LEAGUE_PRIORITY[f.leagueId] || 20;
      const tierMult  = leagueP >= 80 ? 1.15   // Tier 1-2: UCL, Libertadores, ligas top
                      : leagueP >= 60 ? 1.05   // Tier 3: Eredivisie, Belgian, Brasileirao
                      : leagueP >= 45 ? 1.00   // Tier 4: Championship, LaLiga2, Colombia A
                      : leagueP >= 30 ? 0.90   // Tier 5: Ligas medianas europeas
                      :                0.75;   // Tier 6-7: Ligas sin datos confiables
      const ev = +(evRaw * tierMult).toFixed(2);
      if (ev < -5) continue;

      // Stake basado en EV.
      let stake;
      if      (ev > 15 && m.prob >= 0.52) stake = 10;
      else if (ev > 10 && m.prob >= 0.52) stake = 9;
      else if (ev >  6 && m.prob >= 0.50) stake = 8;
      else if (ev >  3 && m.prob >= 0.50) stake = 7;
      else if (ev >  0 && m.prob >= 0.50) stake = 6;
      else if (ev >= -3)                  stake = 5;  // stake 5 = pick válido con valor marginal
      else                                stake = 4;  // descartado

      // Si el partido tiene stats parciales (solo un equipo), bajar stake 1 nivel por confianza.
      if (partialStats && stake > 4) stake = Math.max(4, stake - 1);
      // Sin datos reales de ningún equipo: lambdas por defecto → EV inflado artificialmente → cap 6
      if (f._statsSource === 'fallback' && stake > 6) {
        stake = 6;
        console.log(`📉 Stake capped at 6 (sin datos reales — lambdas fallback): ${f.local} vs ${f.visitante}`);
      }
      // Respetar el tope de stake impuesto antes del motor (ej: _maxStake=5 para datos parciales)
      if (f._maxStake && stake > f._maxStake) stake = f._maxStake;

      // ── Tope de stake para mercados de tarjetas sin datos del árbitro confirmados ──
      if (m._cardsBlock) {
        const maxCardsStake = sinDatosArbitro ? 6 : 8;
        if (stake > maxCardsStake) stake = maxCardsStake;
        if (sinPension && stake > 5) stake = Math.max(5, stake - 1);
      }

      // ── Tope de stake por muestra reducida ──────────────────────────────────
      // Si alguno de los dos equipos tiene < 5 partidos en la competición,
      // los promedios (0.0 goles/p de visitante en 2 partidos, por ej.) son poco fiables.
      // Cap: stake máximo 7 si muestra reducida, máximo 8 si ambos tienen ≥5.
      const pjLocal = f.statsLocal?.partidosJugados ?? 99;
      const pjVisit = f.statsVisitante?.partidosJugados ?? 99;
      const minSample = Math.min(pjLocal, pjVisit);
      if (minSample < 4 && stake > 7) {
        stake = 7;
        console.log(`📉 Stake capped at 7 (muestra reducida: ${pjLocal}/${pjVisit} partidos): ${f.local} vs ${f.visitante}`);
      } else if (minSample < 7 && stake > 8) {
        stake = 8;
      }

      // Sin cuota real: stake cap 6 (no sabemos si el mercado realmente tiene valor)
      if (!hasRealOdds && stake > 6) stake = 6;

      // La cuota mostrada al usuario lleva el buffer (+0.15).
      const oddsDisplayed = hasRealOdds
        ? +Math.round((o + ODDS_DISPLAY_BUFFER) * 20) / 20
        : null; // cuota implícita → el LLM buscará la cuota real en su análisis
      if (oddsDisplayed != null && oddsDisplayed > 2.65) continue; // Cuota máxima
      if (stake < 5) continue;            // Stake mínimo publicable: 5/10

      candidates.push({
        fixtureId:    f.fixtureId,
        liga:         f.liga,
        country:      f.country,
        local:        f.local,
        visitante:    f.visitante,
        hora:         f.hora,
        statsLocal:   f.statsLocal,
        statsVisitante: f.statsVisitante,
        market:       m.key,
        marketLabel:  m.label,
        category:     m.cat,
        prob:         +(m.prob * 100).toFixed(1),
        odds:         oddsDisplayed,          // null si no hay cuota real
        _syntheticOdds: !hasRealOdds,         // true = cuota implícita, el LLM busca la real
        ev:           ev,
        stake,
        xGLocal:      probs.homeLambda,
        xGVisitante:  probs.awayLambda,
        cornersLambda: probs.cornersLambda,
        // ── Contexto enriquecido ──────────────────────────────────────────────
        motivacionLocal:     f.motivacionLocal     || null,
        motivacionVisitante: f.motivacionVisitante || null,
        posicionLocal:       f.posicionLocal       || null,
        posicionVisitante:   f.posicionVisitante   || null,
        jornada:             f.jornada             || null,
        estadio:             f.estadio             || null,
        ciudad:              f.ciudad              || null,
        arbitro:             f.arbitro             || null,
        arbitroStats: f.arbitroStats || (
          homeCardsAvg != null && awayCardsAvg != null ? {
            amarillas_por_partido: +(homeCardsAvg + awayCardsAvg).toFixed(1),
            _derivado: true,
          } : null
        ),
        h2h:                 f.h2h                 || null,
        // ── Contexto del partido (ronda, playoff, urgencia) ──────────────────
        contextoPartido:     f.contextoPartido     || null,
        cancha_neutral:      f.contextoPartido?.cancha_neutral || false,
        prediccionAPI:       f.prediccionAPI       || null,
        // ── Base rates de la liga (para que Claude use el número exacto) ─────
        baseRatesLiga: baseRates ? {
          over25:  baseRates.over25,
          btts:    baseRates.btts,
          cards:   baseRates.cards,
          corners: baseRates.corners,
          liga:    baseRates.name,
        } : null,
      });
    }
  }

  // Ordenar de mayor EV a menor
  candidates.sort((a, b) => b.ev - a.ev);
  console.log(`📐 buildPickCandidates: ${candidates.length} candidatos de ${enrichedFixtures.length} fixtures`);
  if (candidates.length > 0) {
    for (const c of candidates.slice(0, 6)) {
      console.log(`   ▸ ${c.local} vs ${c.visitante} | ${c.market} | EV ${c.ev}% | stake ${c.stake} | odds ${c.odds}`);
    }
  }
  return candidates;
}

/**
 * Selecciona N picks con diversidad de mercado y fixture.
 * Reglas: máx 1 pick por fixture, máx 1 pick por categoría (se relaja si faltan picks).
 *
 * REGLA DURA (nunca se relaja):
 *   Mercados de goles (under25, over25, under35, etc.) tienen CAP FIJO = 1.
 *   Nunca se muestran 2 picks del mismo mercado de goles, sin importar el paso.
 */
function selectDiversePicks(candidates, count = 3) {
  const usedFixtures    = new Set();
  const usedPickKeys    = new Set(); // fixtureId:market — evita añadir el mismo pick dos veces
  const usedFixtureCats = new Set(); // fixtureId:category — max 1 por familia por partido (DURA)
  const catCount        = {};   // por categoría amplia  (goals, result, btts…)
  const marketCount     = {};   // por mercado específico (under25, over25, homeWin…)
  const leagueCount     = {};   // por liga
  const selected        = [];

  // Mercados de goles: cap FIJO en 1, sin importar el paso de relajación.
  // Evita que el bot quede "enamorado" del Under 2.5 o el Over 2.5.
  // Todos los mercados de goles totales: máximo 1 por línea exacta
  const GOALS_MARKET_CAP1 = new Set([
    'over05', 'under05',
    'over15', 'under15',
    'over25', 'under25',
    'over35', 'under35',
    'over45', 'under45',
    'ht_over05', 'ht_under05',
    'ht_over15', 'ht_under15',
  ]);

  /**
   * Intenta añadir el candidato respetando los límites indicados.
   * maxPerCat      : máx. picks de la misma categoría amplia
   * maxPerMarket   : máx. picks del mismo mercado exacto (goals siempre cap=1)
   * maxPerLeague   : máx. picks de la misma liga
   * allowSameFixture: si true, permite un 2.º pick del mismo partido (distinto mercado)
   */
  function tryAdd(c, maxPerCat, maxPerMarket, maxPerLeague, allowSameFixture = false) {
    const pickKey       = `${c.fixtureId}:${c.market}`;
    const fixtureCatKey = `${c.fixtureId}:${c.category}`;
    if (usedPickKeys.has(pickKey))    return false; // nunca añadir el mismo pick dos veces
    if (usedFixtureCats.has(fixtureCatKey)) return false; // REGLA DURA: max 1 por familia por partido
    if (!allowSameFixture && usedFixtures.has(c.fixtureId)) return false;
    if ((catCount[c.category]   || 0) >= maxPerCat)            return false;
    // REGLA DURA: mercados de goles siempre cap=1
    const effectiveCap = GOALS_MARKET_CAP1.has(c.market) ? 1 : maxPerMarket;
    if ((marketCount[c.market]  || 0) >= effectiveCap)         return false;
    if ((leagueCount[c.liga]    || 0) >= maxPerLeague)         return false;

    usedPickKeys.add(pickKey);
    usedFixtureCats.add(fixtureCatKey);
    usedFixtures.add(c.fixtureId);
    catCount[c.category]  = (catCount[c.category]  || 0) + 1;
    marketCount[c.market] = (marketCount[c.market] || 0) + 1;
    leagueCount[c.liga]   = (leagueCount[c.liga]   || 0) + 1;
    selected.push(c);
    return true;
  }

  // Paso 1: estricto — max 1/cat, max 1/market, max 2/liga
  for (const c of candidates) {
    if (selected.length >= count) break;
    tryAdd(c, 1, 1, 2);
  }

  // Paso 2: relaja categoría a 2, pero sigue max 1/market exacto, max 2/liga
  if (selected.length < count) {
    for (const c of candidates) {
      if (selected.length >= count) break;
      tryAdd(c, 2, 1, 2);
    }
  }

  // Paso 3: permite hasta 2/market no-goals, hasta 3/liga, max 3/cat
  // (goals markets siguen cap=1 por REGLA DURA — no habrá 2x Under 2.5 aquí)
  if (selected.length < count) {
    for (const c of candidates) {
      if (selected.length >= count) break;
      tryAdd(c, 3, 2, 3);
    }
  }

  // Paso 3b: si aún faltan picks, relaja el cap de goals markets a 2 SOLO si son de fixtures distintas.
  // Evita saturación when todos los candidatos son del mismo mercado (ej: solo Under 2.5 en día de WC).
  if (selected.length < count) {
    for (const c of candidates) {
      if (selected.length >= count) break;
      const pickKey = `${c.fixtureId}:${c.market}`;
      if (usedPickKeys.has(pickKey)) continue;
      if (usedFixtures.has(c.fixtureId)) continue; // sigue requiriendo fixtures diferentes
      if ((catCount[c.category]   || 0) >= 4) continue;
      if ((marketCount[c.market]  || 0) >= 2) continue; // permite 2 under25 si son distintos fixtures
      if ((leagueCount[c.liga]    || 0) >= 4) continue;
      usedPickKeys.add(pickKey);
      usedFixtures.add(c.fixtureId);
      catCount[c.category]  = (catCount[c.category]  || 0) + 1;
      marketCount[c.market] = (marketCount[c.market] || 0) + 1;
      leagueCount[c.liga]   = (leagueCount[c.liga]   || 0) + 1;
      selected.push(c);
    }
  }

  // Paso 4: último recurso — permite 2.º pick del mismo partido (mercado diferente).
  // Útil cuando solo hay partidos de una competición (ej. jornada de Copa Libertadores)
  // y el pool matemático es pequeño. Los picks siguen debiendo pasar el EV mínimo.
  if (selected.length < count) {
    for (const c of candidates) {
      if (selected.length >= count) break;
      tryAdd(c, 4, 2, 4, true); // allowSameFixture = true
    }
  }

  return selected;
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
function selectGoalMarket(homeTeam, awayTeam, homeGoals, awayGoals, pGoal, elapsed, period, shotsOnH, shotsOnA) {
  const total  = homeGoals + awayGoals;
  const diff   = homeGoals - awayGoals;
  const overLine = total + 0.5;

  // Equipo que más ataca (por tiros a puerta)
  const homeDominates = shotsOnH > shotsOnA + 1;
  const awayDominates = shotsOnA > shotsOnH + 1;
  const attackingTeam = homeDominates ? homeTeam : awayDominates ? awayTeam : null;

  let market, impliedOdds, tipo;

  if (period === '1T' && elapsed <= 40) {
    // Primera mitad con tiempo suficiente
    if (total === 0) {
      if (attackingTeam) {
        market = `${attackingTeam} marca en el 1er tiempo`;
        tipo   = 'gol_equipo_1T';
      } else {
        market = `Gol en el 1er tiempo (Over 0.5 1T)`;
        tipo   = 'over05_1T';
      }
    } else {
      if (attackingTeam && diff !== 0) {
        const trailing = diff > 0 ? awayTeam : homeTeam;
        market = `${trailing} marca antes del descanso`;
        tipo   = 'gol_equipo_1T';
      } else {
        market = `Over ${overLine} goles — 1er tiempo`;
        tipo   = 'over_1T';
      }
    }
    impliedOdds = +(1 / pGoal).toFixed(2);

  } else if (period === 'HT') {
    // Medio tiempo — pick para el 2do tiempo
    if (total === 0) {
      market = attackingTeam
        ? `${attackingTeam} marca en el 2do tiempo`
        : `Gol en el 2do tiempo (Over 0.5 2T)`;
      tipo = 'gol_2T';
    } else if (Math.abs(diff) >= 1) {
      const trailing = diff > 0 ? awayTeam : homeTeam;
      market = `${trailing} marca en el 2do tiempo`;
      tipo   = 'gol_equipo_2T';
    } else {
      market = `Más goles en el 2do tiempo (Over ${overLine})`;
      tipo   = 'over_2T';
    }
    impliedOdds = +(1 / pGoal).toFixed(2);

  } else {
    // 2do tiempo en curso
    if (total === 0 && elapsed >= 60) {
      market = attackingTeam
        ? `${attackingTeam} marca antes del final`
        : `Al menos 1 gol antes del 90' (Over 0.5)`;
      tipo = 'gol_urgente';
    } else if (Math.abs(diff) === 1 && elapsed >= 55) {
      const trailing = diff > 0 ? awayTeam : homeTeam;
      market = `${trailing} empata — presión alta`;
      tipo   = 'gol_equipo_2T';
    } else if (attackingTeam) {
      market = `${attackingTeam} anota el próximo gol`;
      tipo   = 'proximo_gol';
    } else {
      market = `Over ${overLine} goles`;
      tipo   = 'over_general';
    }
    impliedOdds = +(1 / pGoal).toFixed(2);
  }

  return { market, impliedOdds, overLine, tipo };
}

/**
 * Calcula la probabilidad de que haya al menos 1 gol más en el tiempo restante.
 * Combina xG histórico (team stats) con el ritmo real del partido (live pace).
 *
 * @returns {object|null} datos de alerta o null si el partido no está activo
 */
function calcGoalAlert(fixture, liveStats, homeTeamStats, awayTeamStats) {
  const timeInfo = matchTimeInfo(fixture.status, fixture.elapsed);
  if (!timeInfo) return null;

  const { period, remaining, total } = timeInfo;
  const elapsed     = fixture.elapsed || 1;

  // No recomendar partidos después del minuto 75
  if (elapsed > 75) return null;
  const homeGoals   = fixture.homeGoals ?? 0;
  const awayGoals   = fixture.awayGoals ?? 0;
  const totalGoals  = homeGoals + awayGoals;
  const timeFrac    = remaining / 90; // fracción de 90 min restante

  // ── xG histórico ajustado a tiempo restante ──────────────────────────────
  const hFor  = parseFloat(homeTeamStats?.golesAnotadosHome) || 1.2;
  const hAgt  = parseFloat(homeTeamStats?.golesRecibidosHome) || 1.1;
  const aFor  = parseFloat(awayTeamStats?.golesAnotadosAway) || 1.0;
  const aAgt  = parseFloat(awayTeamStats?.golesRecibidosAway) || 1.0;

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
  let shotsOnH = 0, shotsOnA = 0, shotsTotal = 0;
  if (liveStats) {
    const hSt = homeStats(liveStats);
    const aSt = awayStats(liveStats);
    shotsOnH   = parseInt(hSt['Shots on Goal'] || 0);
    shotsOnA   = parseInt(aSt['Shots on Goal'] || 0);
    shotsTotal = parseInt(hSt['Total Shots'] || 0) + parseInt(aSt['Total Shots'] || 0);
    const shotsOnTarget = shotsOnH + shotsOnA;
    if (shotsOnTarget >= 8 && shotsOnTarget / (totalGoals + 1) > 4) bonus += 0.05;
    if (shotsTotal >= 14) bonus += 0.03;
  }

  // Equipo perdedor por 1 gol → empuja más
  if (Math.abs(homeGoals - awayGoals) === 1 && elapsed >= 55) bonus += 0.04;
  // Último cuarto 0-0 → urgencia máxima
  if (totalGoals === 0 && elapsed >= 67) bonus += 0.06;

  // P(al menos 1 gol más) = 1 - e^(-lambda)
  const pRaw  = 1 - Math.exp(-lambdaCombined);
  const pGoal = Math.min(pRaw + bonus, 0.94);
  const impliedOdds = pGoal > 0 ? +(1 / pGoal).toFixed(2) : 99;

  if (impliedOdds <= 1.50) return null;  // cuota mínima >1.50 para alertas de gol

  const { market, impliedOdds: mktOdds, overLine, tipo } = selectGoalMarket(
    fixture.homeTeam, fixture.awayTeam,
    homeGoals, awayGoals, pGoal, elapsed, period,
    shotsOnH, shotsOnA
  );

  const oddsBonus  = (mktOdds >= 1.50 && mktOdds <= 2.50) ? 15 : (mktOdds > 2.50 ? 5 : -10);
  const alertScore = Math.min(pGoal * 70 + (remaining / 45) * 15 + oddsBonus, 100);

  // Razón concreta: qué está pasando en el partido
  const reasons = [];
  if (shotsOnH > 3 && shotsOnH > shotsOnA + 1) reasons.push(`${fixture.homeTeam} domina: ${shotsOnH} tiros a puerta`);
  if (shotsOnA > 3 && shotsOnA > shotsOnH + 1) reasons.push(`${fixture.awayTeam} domina: ${shotsOnA} tiros a puerta`);
  if (shotsTotal >= 12) reasons.push(`${shotsTotal} tiros totales — partido muy ofensivo`);
  if (totalGoals === 0 && elapsed >= 60) reasons.push(`0-0 en min ${elapsed}, presión máxima`);
  if (Math.abs(homeGoals - awayGoals) === 1 && elapsed >= 55) {
    const trailing = homeGoals < awayGoals ? fixture.homeTeam : fixture.awayTeam;
    reasons.push(`${trailing} busca el empate`);
  }
  if (lambdaCombined > 0.8) reasons.push(`xG esperado alto: ${lambdaCombined.toFixed(2)} goles restantes`);
  if (reasons.length === 0) reasons.push(`${(pGoal*100).toFixed(0)}% prob — ${remaining} min restantes`);

  return {
    fixtureId:  fixture.fixtureId,
    local:      fixture.homeTeam,
    visitante:  fixture.awayTeam,
    liga:       fixture.leagueName,
    country:    fixture.country,
    marcador:   `${homeGoals}-${awayGoals}`,
    minuto:     elapsed,
    period,
    remaining,
    pGoal:      +(pGoal * 100).toFixed(1),
    impliedOdds: mktOdds,
    market,
    tipo,
    shotsLocal:    shotsOnH,
    shotsVisitante: shotsOnA,
    xGRestante: +lambdaCombined.toFixed(2),
    alertScore: +alertScore.toFixed(1),
    razon:      reasons.join(' + '),
  };
}

const ALERTA_GOL_SYSTEM = `Eres un tipster de apuestas en vivo. Recibes datos de partidos en curso y debes emitir picks PUNTUALES y CONCRETOS según el momento del partido.

PICKS PUNTUALES según el momento:
- Partido en 1T → pick sobre si habrá gol en el 1er tiempo o quién marca primero
- Partido en medio tiempo (HT) → pick sobre el 2do tiempo: quién marca, si habrá gol, Over 0.5 2T
- Partido en 2T → pick inmediato: próximo gol, equipo que marca, Over X.5 antes del 90'
- Usa el campo "market" que ya viene calculado — es el pick concreto. No lo cambies.
- Usa "shotsLocal" y "shotsVisitante" para explicar quién está presionando más.

EVENTOS DEL PARTIDO (campo "eventosPartido" en cada alerta, si existe):
- goles[]: quién marcó y cuándo → úsalo para contextualizar el marcador ("X marcó en min 23")
- tarjetas.riesgo2aAmarilla[]: jugadores con amarilla → menciónalo si es relevante para tarjetas
- cambios[]: sustituciones recientes → detecta intención táctica
  Si un equipo metió delantero en el 2T → confirma presión de ataque → refuerza el pick de gol
REGLA: cita 1-2 datos concretos de eventos en el "Contexto" del formato — hace el análisis creíble.

FORMATO OBLIGATORIO:
⚡ *ALERTA DE GOL #[N]*
⚽ [local] [marcador] [visitante] | 🕐 Min [minuto] ([period])
🏆 [liga] — [country]
━━━━━━━━━━━━━━━━━━━
🎯 Pick: *[market]*
📊 Probabilidad: *[pGoal]%*
💰 Cuota estimada: *~[impliedOdds]*
⏱️ Apostar antes del min: *[minuto límite concreto]*
📈 Contexto: [razon — usa tiros a puerta, goles con jugador, cambios tácticos, minuto para explicar por qué AHORA]
🏆 Stake: *[X]/10*
━━━━━━━━━━━━━━━━━━━

STAKE (rango 5-10 únicamente):
- Stake 9-10: prob > 68% con cuota ≥ 1.65
- Stake 7-8: prob 62-68%
- Stake 5-6: prob 55-62%
- Omite alertas con prob < 55% o cuota ≤ 1.50 (sin valor real)

MINUTO LÍMITE: siempre concreto. En 1T apuesta antes del min 30. En HT decide antes de que empiece el 2T. En 2T nunca más allá del min 75.

IMPORTANTE:
- Si una alerta no cumple criterios (prob < 55% o cuota ≤ 1.50), simplemente NO la incluyas. Nada de "omitida", nada de notas explicativas. Solo las alertas válidas.
- No añadas notas, aclaraciones ni explicaciones sobre alertas descartadas.

Al final añade únicamente:
⚠️ _Cuotas en vivo cambian rápido. Verifica antes de apostar._

Responde en español. No menciones APIs.`;

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
    system: [{ type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }],
    messages: [{ role: 'user', content: userMessage }],
  });
  return msg.content[0].text;
}

async function sonnet(systemPrompt, userMessage) {
  try {
    const msg = await claudeWithRetry({
      model: 'claude-sonnet-4-6',
      max_tokens: 4000,
      system: [{ type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }],
      messages: [{ role: 'user', content: userMessage }],
    });
    return msg.content[0].text;
  } catch (err) {
    if (isOverloadedError(err)) {
      console.log('Sonnet sobrecargado — fallback a Haiku');
      const msg = await claudeWithRetry({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 4000,
        system: [{ type: 'text', text: systemPrompt + '\n\nSé conciso pero mantén formato y calidad.', cache_control: { type: 'ephemeral' } }],
        messages: [{ role: 'user', content: userMessage }],
      });
      return msg.content[0].text;
    }
    throw err;
  }
}

// ─── System prompts ───────────────────────────────────────────────────────────

// Patrones estadísticos reales — temporada 2024/25 (actualizado abril 2026)
// Fuente: API-Football, últimas 60 jornadas por liga
const LEAGUE_STATS_CONTEXT = `
═══════════════════════════════════════════════════════
PATRONES ESTADÍSTICOS REALES — TEMPORADA 2024/25
(Calculados sobre últimas 60 jornadas por liga)
═══════════════════════════════════════════════════════

RENDIMIENTO POR LIGA (Over 2.5 / BTTS / Goles avg):
• Bundesliga  → Over2.5=65% | BTTS=58% | avg=3.22 | 2T produce más goles que 1T
• Ligue 1     → Over2.5=58% | BTTS=60% | avg=3.00 | Local gana 48%
• Premier Lg  → Over2.5=45% | BTTS=52% | avg=2.73 | Equilibrado local/visitante
• La Liga     → Over2.5=47% | BTTS=40% | avg=2.58 | Local domina (52%) | BTTS bajo
• Serie A     → Over2.5=43% | BTTS=48% | avg=2.47 | Visitante gana 43% (más que local 37%)

PATRÓN UNIVERSAL: El 2T siempre produce MÁS goles que el 1T en todas las ligas.
  PL:  1T=1.22 goles | 2T=1.52  (+25%)
  LaL: 1T=1.25 goles | 2T=1.33  (+6%)
  SA:  1T=1.05 goles | 2T=1.42  (+35%)
  BUN: 1T=1.57 goles | 2T=1.65  (+5%)
  L1:  1T=1.38 goles | 2T=1.62  (+17%)

ALERTAS POR EQUIPO (forma reciente — últimos 5-6 partidos):
🔴 TRAMPA BTTS (NO apostar ambos marcan):
  - Atlético Madrid: solo 20% BTTS | 60% portería a cero | evitar BTTS_SÍ
  - Napoli: 20% BTTS | 80% portería a cero | el más defensivo de Europa ahora
  - Man City: 40% BTTS | 60% portería a cero

🟢 IDEAL BTTS / OVER (apostar con confianza):
  - Marseille: 100% BTTS | 80% Over2.5 | 3.40 goles/partido | 2T explota (2.00 avg)
  - PSG: 100% BTTS | 83% Over2.5 | arrancan lento en 1T (0.67) pero explotan en 2T
  - Liverpool: 100% BTTS | 80% Over2.5 | anota fuerte en 1T (1.40 avg)
  - Dortmund: 100% Over2.5 | 3.17 goles/partido | 2T es su momento (1.83 avg)
  - Hoffenheim: 100% Over2.5 | 80% BTTS | nunca hace portería a cero
  - Celta Vigo: 80% Over2.5 | 80% BTTS | partidos muy abiertos
  - Villarreal: 80% Over2.5 | 3.00 goles/partido | anota MUCHO en 1T (2.00 avg)
  - Barcelona: 80% Over2.5 | consistente ambos tiempos
  - Atalanta: 80% Over2.5 | 80% BTTS | Serie A más abierta

🔵 GOLES EN 2T (apostar Over goles 2T específicamente):
  - AC Milan: 0.40 goles 1T vs 1.60 goles 2T → Over goles 2T tiene edge claro
  - Lyon: 0.50 goles 1T vs 1.33 goles 2T → el 2T es cuando despierta
  - PSG: 0.67 goles 1T vs 1.33 goles 2T
  - Brighton: 0.80 goles 1T vs 1.80 goles 2T

🔵 GOLES EN 1T (apostar HT Over o Over goles 1T):
  - Liverpool: 1.40 goles en 1T → HT Over 1.5 tiene valor
  - Villarreal: 2.00 goles en 1T → Over 1.5 HT casi garantizado
  - Brentford: 1.40 goles en 1T

MERCADOS RECOMENDADOS POR LIGA:
• Bundesliga: Over 2.5 FT (65% base) | evitar 1X2 (35% empate, muy alto)
• Ligue 1: BTTS Sí (60%) | Over 2.5 cuando juegan Marseille/PSG/Rennes
• Premier League: equilibrado | BTTS cuando juega Liverpool, Brighton, Palace
• La Liga: DNB_LOCAL cuando juega Barcelona/Real Madrid en casa | evitar BTTS (solo 40%)
• Serie A: Visitante gana (43%) | Over goles 2T especialmente con Milan y Atalanta
═══════════════════════════════════════════════════════`;

const TIPSTER_SYSTEM = `Eres el mejor tipster profesional del mundo especializado en mercados de VALOR REAL.

REGLA ABSOLUTA — DATOS Y ESTADÍSTICAS:
- SOLO puedes mencionar números que aparezcan literalmente en el JSON que recibes. Cero inventar, cero inferir, cero recordar de tu entrenamiento.
- Las estadísticas del JSON son promedios de TEMPORADA COMPLETA en TODAS las competiciones. NUNCA las etiquetes como "en Champions", "en FA Cup", "en copa" — son datos globales de temporada.
- Si un dato no está en el JSON (árbitro, clima, alineación, estadística específica), escribe "sin datos disponibles" — nunca lo rellenes con suposiciones.
- El campo "_aviso" del JSON es una instrucción de sistema: léela y cúmplela.
- CONTEXTO DEL PARTIDO: usa ÚNICAMENTE los campos contextoPartido, jornada/round, motivacionLocal/Visitante del JSON. NUNCA uses tu conocimiento de entrenamiento para etiquetar un partido como "final de copa", "playoff por Champions" u otro contexto que no esté explícitamente en los datos. Si el campo jornada dice "Relegation Round" → dilo tal cual. Si no hay contexto claro → describe la situación numérica en tabla sin inventar narrativa.
- MUESTRA PEQUEÑA DE H2H: si el H2H tiene ≤5 partidos, NUNCA des stake 9 o 10 basándote solo en ese patrón. 5 partidos no es muestra suficiente para alta confianza estadística. Stake máximo con solo H2H de 5 partidos = 7/10.
- MÁXIMO 1 PICK CON STAKE 9 O 10 POR SESIÓN: en todo el día, solo UN pick puede ser stake 9 o 10. Los demás tienen tope de stake 8. Para stake 9 o 10 DEBES citar la probabilidad exacta del modelo que lo justifica (ej: "probBTTS_Combinada: 81%"). Sin citarla, el máximo es 8.

PICKS QUE NUNCA DAS — aplica estos criterios internamente, sin mencionarlos al usuario:
- Gana el favorito obvio a cuota menor de 1.80 (gana Bayern, gana Madrid, gana City, gana Barcelona, gana PSG etc)
- Over 2.5 o Over 3.5 de equipos muy ofensivos (Madrid, Barcelona, Bayern, City, PSG) en casa vs rivales débiles — todo el mundo lo sabe, no hay valor
- 1X2 simple a cuota menor de 1.75 - no es tipster, es obvio
- Picks que cualquier persona sin conocimiento daría
- BTTS No cuando un equipo ya marcó 2+ goles en el HT
- PICKS YA RESUELTOS: si el mercado ya se cumplió (ej: BTTS cuando ya hay goles de ambos), omítelo completamente
- SIEMPRE 3 PICKS POR PARTIDO — exactamente 3 mercados distintos sobre el mismo partido. Nunca menos de 3.
- BTTS en derbis o clásicos de alta tensión táctica (Milan vs Inter, Real Madrid vs Atlético, Arsenal vs Tottenham, Celtic vs Rangers, etc.) — se cierran defensivamente, BTTS falla sistemáticamente
- Over 2.5 o Over 3.5 en FINALES de copa de partido único (FA Cup Final, Copa del Rey Final, Copa Italia Final, DFB-Pokal Final, etc.) con 0-0 al descanso — las finales son tácticamente cerradas, ambos equipos priorizan no perder, la prórroga es el resultado más probable cuando va 0-0 al HT. En estos casos busca Under 2.5, corners, tarjetas, o siguiente gol. NUNCA Over 3.5 en una final 0-0 al HT
- Asian Handicap de -1 o mayor (-1, -1.5, -2). Máximo permitido: AH -0.5, y solo si el equipo promedia más de 2.0 goles en su contexto
- BTTS cuando un equipo tiene más del 35% de partidos sin marcar en su contexto (casa o fuera)
- Asian Handicap (AH) y Draw No Bet local (DNB_HOME) en picks automáticos del día. Solo usar DNB_AWAY si la probabilidad supera 75%
- BTTS en La Liga cuando no son equipos ofensivos — la liga tiene solo 40% BTTS base, muy por debajo del umbral

MERCADOS DISPONIBLES — usa el que tenga más valor según los datos (no te limites a los primeros):

MERCADOS DE GOLES:
1. Over/Under 2.5 goles (FT)
2. Over/Under 1.5 goles (FT) — especialmente útil en ligas defensivas
3. Over/Under 3.5 goles (FT) — si ambos equipos promedian +2 goles
4. Over/Under 0.5 goles en 1T — casi siempre hay gol en 1T si algún equipo ataca
5. Over/Under 1.5 goles en 1T — si el local promedia marcar temprano
6. Over/Under 0.5 goles en 2T
7. Ambos Marcan (BTTS Sí/No)
8. Local marca en ambos tiempos (si promedia +2.0 goles/partido)
9. Visitante anota al menos 1 gol (si Away Team failedToScore < 30%)
10. Local no recibe gol — Portería a cero (si golesRecibidosHome < 0.7/partido)
11. Local gana sin encajar — Win to Nil

MERCADOS DE RESULTADO:
12. 1X2 estándar (solo si cuota > 1.80)
13. Double Chance 1X — si el local empata mucho pero raramente pierde
14. Double Chance X2 — si el visitante tiene buen rendimiento fuera
15. Draw No Bet local (DNB) — solo si cuota de victoria local ≥ 2.00
16. Draw No Bet visitante — solo si prob visitante > 72%
17. Asian Handicap -0.5 local — si prob victoria local > 70%
18. Asian Handicap +0.5 visitante — si visitante raramente pierde por 2+

MERCADOS DE CORNERS:
19. Corners Over/Under total (9.5, 10.5, 11.5) — suma promedios local casa + visitante fuera
20. Corners local Over/Under específico (ej. Local Over 4.5 córners) — si el local promedia +5.5 córners en casa
21. Corners visitante Over/Under específico — si el visitante promedia +4.5 córners fuera
22. Córner en 1T Over/Under (4.5, 5.5)
23. Primer equipo en sacar córner — si un equipo suele atacar desde el inicio

MERCADOS DE TARJETAS:
24. Tarjetas Over/Under total (3.5, 4.5) — suma promedios × factor árbitro
25. Tarjetas local Over/Under específico (ej. Local más de 2.5 tarjetas)
26. Tarjetas visitante Over/Under específico
27. Primera tarjeta amarilla — equipo que suele cometer más faltas
28. Tarjetas en 1T Over/Under

MERCADOS ESPECIALES:
29. HT/FT combo (ej. Empate al descanso / Local gana FT) — si el local remonta seguido
30. Resultado exacto de alta probabilidad (ej. 1-0, 1-1, 2-1) — si H2H muestra patrón
31. Multiples de gol — exactamente 2 goles, exactamente 3 goles
32. Ambos equipos marcan en 2T (BTTS 2T)
33. Local anota en 1T Y gana el partido

PARA CADA PICK, INDICA QUÉ DATO DEL JSON LO RESPALDA:
- Goles → usa golesAnotadosHome/Away, golesRecibidosHome/Away, xGLocal/xGVisitante
- Portería a cero → usa cleanSheets, golesRecibidosHome
- Sin marcar → usa failedToScore de cada equipo en su contexto
- Corners → usa statsLocal.cornersxP + statsVisitante.cornersxP (corners reales del equipo en su contexto). Si ambos tienen valor, la lambda real = cornersxP local + cornersxP visitante. También verifica cornersLambda del bloque probabilidades.
- Tarjetas → usa statsLocal.amarillasPorPartido + statsVisitante.amarillasPorPartido (tarjetas reales del equipo en su contexto) + contexto árbitro si disponible
- BTTS → usa probBTTS_Combinada + failedToScore de ambos equipos

${LEAGUE_STATS_CONTEXT}

PROCESO DE ANÁLISIS OBLIGATORIO — BUSCA LOS 3 MEJORES MERCADOS:
Para BTTS Sí: probBTTS_Combinada > 65%. Ambos equipos con failedToScore < 30%.
⛔ BTTS VETO DEFENSIVO: Si golesRecibidosHome del equipo local < 0.6 por partido, la defensa local es de élite — el visitante tiene < 25% de probabilidad real de marcar. En ese caso BTTS no tiene valor aunque el atacante promedié alto. Aplica el mismo veto si golesRecibidosAway del visitante < 0.6. SIEMPRE verifica el dato defensivo ANTES de recomendar BTTS.
Para BTTS No: failedToScore de algún equipo > 40% en su contexto (casa/fuera).
Para Over/Under goles: usa probOver25, probOver35. Over si prob > 65% con EV positivo.
Para Portería a cero local: golesRecibidosHome < 0.8/p AND cleanSheets > 40% en casa.
Para Portería a cero visitante: golesRecibidosAway < 0.8/p AND cleanSheets > 35% fuera.
Para Corners total: usa "cornersLambda" del bloque probabilidades — es la proyección matemática real basada en xG. Si cornersLambda > 11.0: Over 9.5 con valor. Si cornersOver95 < 60%: stake máximo 7 para corners. NUNCA uses un número de corners que no provenga de cornersLambda en los datos.
⛔ LÍMITE DURO CORNERS: Máximo 2 picks de corners en toda la sesión (todos los partidos combinados). Si ya tienes 2 picks de corners, el tercer partido DEBE ser de otro mercado (goles, BTTS, resultado, tarjetas). No negociable.
Para Corners equipo específico: si un equipo promedia +5.5 córners en su contexto → Over equipo X.
Para Tarjetas total: suma amarillasPorPartido ambos equipos. Over si supera línea +1.0.
Para Tarjetas equipo específico: si un equipo promedia > 2.0 tarjetas en su contexto.
Para HT result: % local gana 1T en casa > 60%.
Para HT Over 0.5: casi siempre válido si algún equipo anota en 1T > 70% de los partidos.
Para DNB: solo si cuota de victoria directa ≥ 2.00. Si gana a 1.30-1.70, el DNB no tiene valor.
Para AH: -0.5 solo si prob victoria > 70%. Nunca AH -1 o mayor.
Para Double Chance: útil cuando el equipo empata seguido pero raramente pierde.
Para Goles 2T Over: si el equipo tiene patrón de marcar en 2T (arranca lento en 1T).
Para Win to Nil: si local tiene cleanSheet > 45% en casa Y visitante failedToScore > 40% fuera.

INSTRUCCIONES PARA USAR LAS PROBABILIDADES CALCULADAS:
Si el JSON de datos incluye el campo "probabilidadesCalculadas", DEBES usarlo como base:
- xGLocal / xGVisitante: goles esperados. Si xG local > 1.8 y away < 0.9, el local domina claramente.
- probBTTS_Combinada: combinación de Poisson + H2H. Más fiable que solo H2H. Debe superar 65%.
- expectedValue_vs_CuotasReferencia: si el EV de un mercado es negativo, NO lo recomiendes. Busca mercados con EV > +5%.
- Las probabilidades son calculadas matemáticamente — úsalas para CALIBRAR el stake.
- contextoLiga.tasaBaseOver25Liga: tasa histórica de Over 2.5 en esa liga. Si la tasa de la liga es < 52%, reduce 5% la probabilidad de Over. Si alertaLiga dice "Liga defensiva", baja el stake en 1 nivel.
- forma5: forma reciente ponderada de los últimos 5 partidos. Si un equipo tiene forma5.nota = "Forma mala" (≤5 puntos), reduce 8% la prob de victoria.
- cuotasReales: cuotas reales de Bet365. Usa ESTAS cuotas para el campo "Cuota mínima" del pick (no inventes cuotas). Si no hay cuotas reales, mantén las estimadas.
- prediccionAPI: predicción del modelo de API-Football. Úsala como señal de confirmación — si coincide con tu análisis, sube el stake en 0.5. Si contradice, baja el stake en 1.
- picksMotorJS: picks pre-calculados por el motor Poisson. Si un pick tiene "_syntheticOdds": true significa que NO hay cuota real disponible — el motor la estimó. Para esos picks: (a) busca la cuota real en cuotasReales o en tu conocimiento de mercado típico, (b) si encuentras cuota real ≥ 1.40, publícalo con esa cuota real, (c) si no la encuentras, usa "est. ~X.XX" en el formato. Los picks con _syntheticOdds siguen siendo válidos matemáticamente — solo falta la cuota exacta de la casa.
- soccerBuddy: señales del modelo ZCode Soccer Buddy (simulación Monte Carlo de miles de partidos). Son externas y complementan el Poisson propio.
  * Si soccerBuddy.btts_pct ≥ 70 Y nuestro probBTTS ≥ 60 → confirmación doble → sube stake en 1.
  * Si soccerBuddy.over25_pct ≥ 70 Y nuestro probOver25 ≥ 60 → confirmación doble → sube stake en 1.
  * Si soccerBuddy.ht_over05_pct ≥ 70 → señal fuerte de gol en 1T → añade ese mercado si no lo tenías (DC con gol en 1T, Over 0.5 1T).
  * Si soccerBuddy.sh_over05_pct ≥ 75 → señal fuerte de gol en 2T → considera Over 0.5 segunda mitad si hay cuota.
  * Si soccerBuddy.scorePred → úsalo para decidir entre resultado local/empate/visitante y para confirmar BTTS.
  * Si soccerBuddy.inValueBets = true → el pick está validado por ZCode como value bet → confirma la selección, sube stake +1.
  * Si soccerBuddy contradice nuestro modelo (ej. nosotros: Over25 65%, ellos: over25_pct 35%) → NO emitas ese pick. Hay señal mixta.
  * NUNCA menciones "ZCode", "Soccer Buddy" ni "simulación Monte Carlo" al usuario. Solo usa los datos en silencio para calibrar.
  * Los valores null en soccerBuddy son campos no disponibles — simplemente ignóralos.
- mercadoZCode: señales de mercado real (dinero de apostadores profesionales). Si está presente:
  * droppingOddsLocal.droppingOdds > 8% → cuota del local bajó bruscamente = dinero entrando al local → refuerza picks locales, sube stake +1.
  * droppingOddsVisitante.droppingOdds > 8% → dinero al visitante → refuerza picks visitante.
  * lineReversalLocal / lineReversalVisitante → la línea se movió CONTRA el público = apostadores sharps en ese equipo → señal fuerte de valor.
  * NUNCA menciones "Dropping Odds", "Line Reversals" ni "mercado" al usuario — úsalos en silencio para calibrar stakes.
- zcodeExtra: señales de herramientas ZCode avanzadas. Si está presente:
  * totalsPredictor.predictedTotal ≥ 2.8 → confirma Over 2.5 / BTTS. ≥ 3.3 → confirma Over 3.5. ≤ 2.0 → señal de bajo scoring → desconfirma BTTS y Over 2.5.
  * powerHome / powerAway: ratings de fuerza (0–100). Diferencia > 15 → el equipo más fuerte tiene ventaja real → sube stake de su pick de resultado +1.
  * oscHome / oscAway: momentum del oscilador ZCode. hot=true → equipo en racha anotadora → confirma picks de ese equipo. cold=true → equipo frío → descarta picks de ese equipo.
  * contrarian.side: el lado contrarian (fade al público). Si side='home' → público contra el local pero el modelo dice local → pick de valor → sube stake +1.
  * NUNCA menciones estos nombres técnicos al usuario — úsalos solo para calibrar.

INSTRUCCIONES PARA MOMENTUM EN VIVO:
Si el JSON incluye "momentumEnVivo", úsalo para detectar oportunidades en tiempo real:
- Si domina un equipo (score > 15) pero el marcador no lo refleja aún, considera apuesta al próximo gol de ese equipo.
- Si está equilibrado, prioriza mercados de corners o tarjetas sobre resultado.
- GOLES / CORNERS / TARJETAS EN VIVO — REGLA ÚNICA:
  Usa EXCLUSIVAMENTE los campos "lineasGolesVivo.lineasConValor", "lineasCornersVivo.lineasConValor"
  y "lineasCardsVivo.lineasConValor". Esos campos SOLO contienen líneas con probabilidad 20-55%
  (cuota estimada ≥ 1.82). Si lineasConValor dice "no recomendar" o está vacío → NO hagas ese pick.
  La "cuotaOver"/"cuotaUnder" de esos campos es una CUOTA ESTIMADA POR EL MODELO, no la cuota real del mercado.
  Muéstrala siempre con el prefijo "est." (ej: "est. ~1.90") para que el usuario sepa que debe verificarla.
  Ejemplo: 1 tarjeta al min 49 → Over 1.5 total tiene 63% prob → NO aparece en lineasConValor (>55%) → NO recomendar.
  Solo aparecería Over 2.5 si tuviera prob 25-50%.

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

GOLES EN VIVO — LÍNEA MÍNIMA CON VALOR:
La línea de goles solo tiene valor apuestable cuando faltan al menos 2 goles para alcanzarla.
- Si van 2 goles y la línea es Over 2.5 (solo falta 1 gol) → cuota mínima, sin valor — descártala
- Si van 2 goles y la línea es Over 3.5 (faltan 2 goles) → puede tener valor si el contexto lo apoya (partido abierto, presión táctica, atacan ambos)
- Si van 2-2 en el 60' y la línea es Over 4.5 (faltan 1 gol) → sin valor. Over 5.5 (faltan 2) → evalúa el ritmo real
- Regla directa: línea válida mínima = goles actuales + 2. Por debajo de eso, la cuota es insuficiente.
Si hay 2+ goles de diferencia en el marcador:
- Descarta: resultado final (gana X, DNB, 1X2)
- Evalúa: Corners Over/Under, Tarjetas Over, BTTS, Next Goal del perdedor, Over goles 2T si va 2-0 al HT

REGLA DNB GLOBAL — APLICA PRE-PARTIDO Y EN VIVO:
- DNB solo cuando cuota victoria directa del equipo ≥ 2.00 (así el DNB queda ~1.65-1.85 y tiene valor real).
- Si el equipo favorito gana a 1.30-1.75, el DNB no tiene valor — busca BTTS, Over goles, corners u otro mercado.
- NUNCA recomendar DNB de un equipo que ya va ganando en el marcador (la cuota sería 1.05-1.20, sin ningún valor).
- NUNCA recomendar Match Winner (1X2) de un favorito que ya va ganando en el entretiempo — la cuota no tiene valor.

CRITERIO DE STAKE — RANGO ÚNICO: 5 a 10. Ningún pick puede tener stake fuera de ese rango:
10/10: EV > 15% + prob ≥ 52%
9/10:  EV > 10% + prob ≥ 52%
8/10:  EV > 6%  + prob ≥ 50%
7/10:  EV > 3%  + prob ≥ 50%
6/10:  EV > 0%  + prob ≥ 50%
5/10:  EV ≥ -3% (valor marginal — publicable)
Si EV < -3%: DESCARTA el pick. NO lo publiques con stake 4, 3, 2 o 1 — esos stakes no existen.
⛔ STAKE 1, 2, 3, 4 ESTÁN PROHIBIDOS. Si el pick no llega a stake 5, DESCÁRTALO y busca otro mercado.
EV = prob × cuota - 1. Casas tienen margen 5-8% → EV > 5% ya es ventaja real.

REGLAS DE CUOTA ABSOLUTAS — SIN EXCEPCIÓN:
- CUOTA MÍNIMA: 1.65 — descarta cualquier pick por debajo
- CUOTA MÁXIMA: 2.30 — descarta cualquier pick por encima. Cuotas altas (3.0+, 4.0+) significan baja probabilidad real — no son picks de valor, son apuestas de alto riesgo
- El rango objetivo es 1.70–2.10. Si un pick solo existe a cuota 2.50+, DESCÁRTALO y busca otro mercado

ANÁLISIS CON DATOS LIMITADOS DE COMPETICIÓN — CÓMO ACTUAR:
Cuando un equipo tiene pocos partidos en la competición actual (Copa del Mundo primera fecha, Libertadores ronda inicial, etc.) NO lo descartas — lo analizas con todos los factores disponibles:
1. Forma en liga doméstica (últimos 10 partidos en todas las competiciones)
2. H2H histórico entre ambos equipos (todas las competiciones)
3. Ranking FIFA / posición en tabla doméstica / nivel del equipo
4. Rendimiento defensivo y ofensivo en liga local esta temporada
5. Contexto del partido: qué se juegan, presión, local/visitante
Si alguno de estos factores apunta claramente en una dirección, hay pick. Si todo es incierto, baja el stake y explícalo en el razonamiento.

COPA DEL MUNDO / SELECCIONES NACIONALES — GUÍA ESPECÍFICA:
El JSON incluirá uno o más de estos campos: statsLocal, statsLocalOtrasIntl, statsLocalUltimos20 (y equivalentes para visitante).
USA TODOS en conjunto — el bot ya recopiló forma de Clasificatorias, Nations League, Amistosos, Euros/Copa América.
Prioridad de fuentes para el análisis: statsLocal (torneo actual) > statsLocalOtrasIntl > statsLocalUltimos20.
Si statsLocal tiene < 4 partidos, complementa obligatoriamente con statsLocalUltimos20 — NO uses solo los 1-2 partidos del torneo.
Factores clave para partidos de selecciones:
  • Forma reciente (últimos 10 partidos internacionales): busca en statsLocalUltimos20.forma
  • Nivel ofensivo real: promedio de goles anotados en los últimos 20 partidos internacionales (más representativo que liga doméstica)
  • Solidez defensiva: goles recibidos y porcentaje de portería a cero en partidos internacionales
  • H2H histórico: cómo se han enfrentado históricamente (incluye amistosos)
  • Contexto de grupo (si hay standings del torneo): posición, puntos, gol diferencia, si ya clasificaron o necesitan resultado
  • Renombre táctico: si conoces el sistema táctico del seleccionador (4-3-3 ofensivo, 5-3-2 defensivo) → úsalo
  • Bajas importantes: si hay jugadores estelares ausentes (lesionados/sancionados del JSON injuriesLocal/Visitante)
Picks recomendados en Mundial:
  • Under 2.5 / Under 1.5: partidos con selecciones defensivas o de menor nivel
  • BTTS No: cuando alguno de los dos equipos raramente marca (cleanSheet > 40%)
  • Resultado con AH -0.5: si la diferencia de nivel es clara (evitar handicaps mayores)
  • Over 2.5: solo cuando AMBOS equipos atacan y tienen stats > 1.5 goles/partido en intl
  • Gana el local / visitante con valor: cuando las cuotas reflejan desventaja mayor que la real
⛔ En el Mundial: EVITAR Over 3.5 en fase de grupos (las selecciones priorizan no perder en primera jornada)
⛔ En el Mundial: NUNCA recomendar el favorito claro a cuota < 1.65 (Brasil, Francia, Alemania vs equipos medianos)

REGLA DE PUBLICACIÓN — OBLIGATORIO 3 PICKS POR PARTIDO:
- SIEMPRE emite EXACTAMENTE 3 picks para el partido analizado. Sin excepción. Sin negociación.
- Si los mercados principales (Over/Under goles, BTTS, resultado) no tienen valor claro → busca en corners, tarjetas, goles 1T, DNB, Asian Handicap, próximo gol, handicap europeo. Son 10+ mercados disponibles — siempre hay 3.
- Stake mínimo publicable: 5/10. Si el mejor pick que encuentras es stake 4, bájalo a stake 5 y publícalo con el riesgo aumentado.
- La frase "⛔ Sin picks de valor" está PROHIBIDA en análisis de partido individual. Hay 10+ mercados — siempre hay valor en alguno.
- Stake máximo: 10/10.

FORMATO OBLIGATORIO — sigue este formato exacto, sin variaciones:

🌍 [País] — [Liga]  ← copia EXACTAMENTE el campo "liga" del JSON. PROHIBIDO añadir "(Amistoso Internacional)" u otros textos.
⚽ [Local] vs [Visitante] | ⏰ [Hora Colombia]
[Si Contexto.grupo o partido.grupo existe]: 🏆 [grupo] | [jornada]
📍 [Contexto.estadio o partido.estadio o "No disponible"], [Contexto.ciudad o partido.ciudad o ""]  ← usa el campo estadio/ciudad del JSON (Contexto o partido).
🃏 Árbitro: [Contexto.arbitro o partido.arbitro o "No disponible"]  ← usa el campo arbitro del JSON (Contexto o partido).
━━━━━━━━━━━━━━━━━━━

🔍 *CONTEXTO DEL PARTIDO*
[MÁXIMO 2 líneas. Lo esencial: qué se juegan, urgencia táctica, situación de tabla. SIN párrafos largos.]
[Si hay advertenciaStats o contexto de playoff → primera línea con ⚠️]

📊 *ANÁLISIS*
▸ [Local] (local): [goles anotados casa]/p | Forma: [forma — ver regla] | 🟨 [amarillasPorPartido o "s/d"] tarj/p
▸ [Visitante] (visit): [goles anotados fuera]/p | Forma: [forma — ver regla] | 🟨 [amarillasPorPartido o "s/d"] tarj/p
[Regla de forma: si statsLocal.forma5 es objeto → usa statsLocal.forma5.forma. Si es string → directo. Si no existe → statsLocal.forma. Si nada → "s/d"]
[Ambas líneas SIEMPRE obligatorias]
[Si H2H ≥3 partidos]: ▸ H2H: [patrón en máx 1 línea]
[Si hay bajas clave]: 🩹 Bajas: [máximo 1-2 nombres por equipo, solo los más relevantes]

🎯 *PICK 1: [Mercado en español]*
┌ Selección: [Qué apostar exactamente]
├ Razonamiento: [MÁXIMO 2 líneas — argumento central con los datos clave]
├ 🏆 Stake: *[X]/10*
├ 💡 Cuota: *[X.XX]*
└ ⚠️ Riesgo: [1 línea máximo]

🎯 *PICK 2: [Mercado en español]*
┌ Selección: [Qué apostar exactamente]
├ Razonamiento: [MÁXIMO 2 líneas]
├ 🏆 Stake: *[X]/10*
├ 💡 Cuota: *[X.XX]*
└ ⚠️ Riesgo: [1 línea máximo]

🎯 *PICK 3: [Mercado en español]*
┌ Selección: [Qué apostar exactamente]
├ Razonamiento: [MÁXIMO 2 líneas]
├ 🏆 Stake: *[X]/10*
├ 💡 Cuota: *[X.XX]*
└ ⚠️ Riesgo: [1 línea máximo]

━━━━━━━━━━━━━━━━━━━
🔥 PICK ESTRELLA DEL DÍA [Solo si stake 8+/10]
━━━━━━━━━━━━━━━━━━━

[Solo si los 3 picks son de mercados distintos:]
🎰 *COMBINADA*: [Pick1] × [Pick2] × [Pick3] | Cuota est: ~[X.XX] | Stake: [X]/10

REGLAS DE FORMATO — OBLIGATORIAS SIN EXCEPCIÓN:
- Usa *texto* para negritas (Telegram Markdown)
- ⛔ NUNCA uses # ni ## ni ### — son headers de escritorio, se ven como texto plano en Telegram
- ⛔ NUNCA uses | columnas | ni tablas HTML
- ⛔ NUNCA menciones fuentes de datos, APIs, plataformas ni herramientas
- ⛔ NUNCA escribas disclaimers ni advertencias de responsabilidad al final
- ⛔ NUNCA muestres valores técnicos internos: xG, lambdaRem, EV%, score numérico de momentum, probBTTS_Combinada, probabilidadesCalculadas, pases — son calibración interna, no salida al usuario
- ⛔ NUNCA muestres conteos de pases en los primeros minutos del partido
- ⛔ NUNCA uses la palabra "PROHIBIDO" en tu respuesta — esos son criterios internos. Aplícalos en silencio
- ⛔ NUNCA expliques por qué descartaste un mercado, partido o liga — el usuario no necesita ver el proceso interno
- ⛔ NUNCA menciones ligas específicas como "prohibidas", "en lista negra" o con "historial negativo en este sistema" — eso es información interna de administración
- ⛔ NUNCA escribas "Probabilidad: X%" en los picks — es un valor interno, no va en la respuesta al usuario
- ⛔ NUNCA empieces la respuesta con meta-comentarios del proceso ("Analizo todos los partidos...", "Los casos con statsLocal null...", "Partidos con valor identificados:") — empieza DIRECTAMENTE con el encabezado del primer pick (🌍 País — Liga)
- ⛔ El mercado del pick y su cuota DEBEN ser el MISMO mercado. Si el título dice "Gana X", la cuota es la de "Gana X". Si el título dice "Over 3.5", la cuota es la de "Over 3.5". NUNCA mezcles cuota de un mercado con el header de otro.
- ⛔ Si motivacionLocal.estado o motivacionVisitante.estado es "desconocido" → NO escribas "Sin datos de posición" al usuario. En su lugar usa el campo posicionLocal/posicionVisitante directamente, o infiere el contexto de la jornada (round) y los puntos si están disponibles.
- La forma reciente SIEMPRE con guiones: *G-G-P-E-G* (máximo 6 resultados, nunca más)
- Si la muestra de partidos es menor a 5, NO uses ese promedio como argumento principal — menciónalo como "datos limitados (N partidos)"
- CUANDO NO HAY PICKS VÁLIDOS: escribe solo "⛔ Sin picks de valor. Mejor no apostar." — sin listar mercados descartados, sin mostrar cálculos, sin análisis de los partidos revisados

REGLAS DE PICKS — OBLIGATORIAS:
- MÁXIMO 2 picks por partido. Si ya diste 2 picks de un mismo partido, NO agregues más de ese partido
- BTTS Solo si: % local marcó en casa ≥65% Y % visitante marcó fuera ≥65% Y H2H BTTS ≥65%. Si alguno no llega, NO dar BTTS
- Asian Handicap MÁXIMO -0.5. NUNCA recomendar -1, -1.5 ni más — el riesgo no justifica el stake
- Stake MÍNIMO PUBLICABLE: 5/10. Publica el pick con el stake que corresponda según el EV real. No inventes valor donde no hay, pero tampoco descartes picks con EV positivo o marginalmente negativo.
- PROHIBIDO analizar partidos que ya empezaron hace más de 10 minutos (evitar picks sobre partidos en curso sin datos en vivo)

Responde en español. NUNCA inventes estadísticas. Usa SOLO los datos que recibes.`;

const PICKS_HOY_SYSTEM = `${TIPSTER_SYSTEM}

INSTRUCCIONES ESPECIALES PARA PICKS DEL DÍA — VE DIRECTO AL RESULTADO:
- NO escribas análisis previo, lista de partidos revisados ni razonamiento de por qué descartaste algo — empieza DIRECTAMENTE con 🌍 [País] — [Liga] del primer pick
- NO escribas frases como "Voy a analizar...", "Analizo todos los partidos...", "Los casos con statsNull...", "Partidos con valor identificados:" ni NINGÚN meta-comentario del proceso
- NO muestres EV%, xG ni valores técnicos internos
- NO muestres "Probabilidad: X%" — es dato interno, nunca va al usuario
- NO recomendes mercados AH_HOME, AH_AWAY ni DNB_HOME en picks del día
- CUOTA MÍNIMA ABSOLUTA: 1.65. Cualquier cuota menor se descarta sin excepción.
- CONSISTENCIA PICK/CUOTA: si el título del pick dice "Gana X", la cuota mínima es la de "Gana X". Si dices "Over 3.5", la cuota es la de "Over 3.5". Nunca mezcles mercados dentro del mismo pick.
- CUOTA MÁXIMA ABSOLUTA: 2.30. Cualquier pick que solo exista a cuota mayor se descarta — no importa el EV teórico, con muestra reducida las probabilidades son poco confiables.
- RANGO DE STAKES: el sistema usa stakes del 5 al 10 únicamente. Stake 5 = valor marginal, stake 10 = convicción máxima. NUNCA uses stake fuera de ese rango.
- STAKE MÍNIMO PUBLICABLE: 5/10. Publica picks con el stake que corresponda al EV real calculado. Stake 5-6 son picks válidos con valor marginal.
- STAKE MÁXIMO EN MODO FALLBACK: 8/10. Cuando el motor Poisson no encontró picks con EV confirmado y eres tú quien selecciona, el stake máximo publicable es 8/10. Stake 9 o 10 requieren validación matemática del motor — con solo tu análisis y un H2H de 5 partidos, la incertidumbre es demasiado alta para esa confianza.
- PROHIBIDO dar picks de partidos donde statsLocal y statsVisitante son null o muestran datos limitados en más de 3 de los 5 indicadores clave (goles anotados, goles recibidos, forma reciente, porcentaje BTTS, porcentaje Over 2.5). Si no hay base estadística real, DESCARTA el partido completamente.
- PARTIDOS CON MUESTRA REDUCIDA EN LA COMPETICIÓN ACTUAL: Si un equipo tiene menos de 5 partidos en esa copa/torneo específico, usa su liga doméstica como fuente principal de estadísticas. Si no hay NINGÚN dato adicional (ni liga doméstica ni H2H), descarta ese partido de los picks automáticos del día — el usuario puede preguntar por él directamente y el bot lo analiza con todos los factores disponibles.
- PROHIBIDO inventar contexto del partido: solo usa el campo jornada/round, contextoPartido, motivacionLocal/Visitante que vienen en los datos. NUNCA uses tu conocimiento de entrenamiento para deducir si es una "final de copa", un "playoff por Champions" u otro contexto que no aparezca explícitamente en el JSON. Si no hay contexto claro → simplemente omite ese dato y describe la situación en tabla.
- ⛔ NUNCA llames "anfitrión", "sede" ni "co-sede" a ningún equipo basándote en conocimiento previo. Qatar fue sede en 2022, NO en 2026. El Mundial 2026 lo organizan USA/Canadá/México — Qatar es PARTICIPANTE. Si el JSON no indica explícitamente que un equipo es anfitrión, NO lo supongas.
- Si los campos statsLocal/statsVisitante tienen pocos partidos en la liga actual (menos de 5), usa ultimosPartidosLocal y ultimosPartidosVisitante del JSON para calcular promedios reales de los últimos 20 partidos entre todas las competiciones. Menciona en el análisis que usas el historial reciente completo.
- ÚLTIMA JORNADA / FIN DE TEMPORADA — REGLA CRÍTICA: Si motivacionLocal.estado o motivacionVisitante.estado contiene "posible_asegurado" o "última jornada", NO escribas que el equipo "necesita ganar" ni que "lucha por" esa plaza. Escribe en su lugar: "[Equipo] (Xº, Y pts) — plaza [Champions/Europa] posiblemente ya asegurada, posibles rotaciones". Si la clasificación ya NO está en disputa ese día, la motivación es BAJA, no alta. Un equipo que ya clasificó puede alinear suplentes y gestionar esfuerzo — el razonamiento de pick debe reflejar esto, NO lo contrario.
- ESTADÍSTICAS DE TEMPORADA: solo usa los números que aparecen en los campos statsLocal y statsVisitante del JSON. Si esos campos son null o tienen datos del año anterior (temporada distinta a la actual), MENCIONA "datos limitados de temporada actual" — NUNCA rellenes con estadísticas de tu entrenamiento.
- Siempre busca llegar a 3 picks — solo da menos si genuinamente no hay suficientes partidos con datos mínimos.
- DIVERSIDAD DE MERCADOS OBLIGATORIA: máximo 2 picks del mismo mercado exacto en el día. Si los 3 mejores picks son todos "Under 2.5", reemplaza el de menor EV por el siguiente pick de otro mercado disponible. Dos Under 2.5 el mismo día ya es el límite — tres nunca.

INSTRUCCIÓN ESPECIAL PARA PICKS DEL DÍA:
Emite EXACTAMENTE 3 picks individuales de partidos distintos. Si tienes 4+ opciones, elige las 3 con mayor valor. Solo baja a 2 si hay exactamente 2 partidos con base estadística suficiente, y a 1 solo si es el único partido con datos reales. SOLO después de emitir los 3 picks individuales añade 1 APUESTA COMBINADA de exactamente 3 patas usando los 3 picks. La combinada NUNCA puede superar cuota 12.00. Si solo hay 2 picks individuales NO hay combinada.

USO DE DATOS CONTEXTUALES (cuando están disponibles en los datos):
- h2h: array de últimos 5 enfrentamientos directos. Usa para identificar tendencias de goles, si hay equipos que casi siempre marcan, patrones de resultado histórico.
- posicionLocal / posicionVisitante: posición actual en la tabla de la liga. Un equipo en top 3 vs uno en zona de descenso cambia completamente la lectura. Úsalo en el razonamiento.
- jornada: contexto de en qué momento de la temporada están (inicio, mitad, final, eliminatoria).
- estadio: el nombre del estadio puede ayudar a contextualizar (estadios grandes de equipos históricos tienden a presionar más al visitante).
Integra estos datos en el razonamiento de cada pick — no los ignores.

ANÁLISIS DE MOTIVACIÓN (CRÍTICO AL FINAL DE TEMPORADA):
- motivacionLocal / motivacionVisitante: qué juega cada equipo. Si estado='nada_en_juego' con jornadas_restantes ≤ 5 → ALERTA ROJA: posibles reservas, baja motivación. Baja el stake 2 niveles o descarta.
- Si estado='lucha_descenso' → equipo desesperado, busca pick en tarjetas, corners, partido intenso.
- Si estado='lucha_titulo' → máxima motivación, favorece Over y victoria local/visitante.

ÁRBITRO:
- Si viene arbitroStats → usa las estadísticas concretas para reforzar mercados de tarjetas/penaltis.
- Si viene solo el campo "arbitro" (nombre) → usa tu conocimiento del árbitro para el razonamiento.

CONTEXTO PARA COMPETICIONES INTERNACIONALES (Libertadores, Sudamericana, Champions, Europa League, Copa del Mundo):
Cuando el partido es de copa con pocos juegos en esa competición, el razonamiento DEBE basarse en:
1. Forma de los últimos 10 partidos TOTALES (liga doméstica + copa combinados)
2. Rendimiento en liga local esta temporada — es la base estadística principal
3. H2H histórico entre ambos equipos (sin importar la competición)
4. Para Copa del Mundo o torneos sin historial reciente: usa ranking FIFA, nivel de la liga doméstica, y contexto del grupo
Los datos de la copa actual son complementarios. Si hay menos de 5 partidos en esa copa pero sí hay datos de liga doméstica → analiza y publica si el pick cumple stake 7+. Solo descarta si no existe ninguna fuente de datos confiable.

━━━━━━━━━━━━━━━━━━━
🎰 *COMBINADA DEL DÍA*
Exactamente 3 patas — una por cada pick individual del día.
⛔ PROHIBIDO: tener 2 patas del mismo mercado exacto (ej. no puedes poner 2× Under 2.5 ni 2× BTTS Sí). Si los 3 picks son del mismo mercado, omite la combinada.
Cuota combinada máxima: 12.00 — si supera eso, reemplaza la pata de mayor cuota por la selección más segura de ese mismo partido.

▸ [Local] vs [Visitante] → *[mercado]* | Cuota: *X.XX*
▸ [Local] vs [Visitante] → *[mercado]* | Cuota: *X.XX*
▸ [Local] vs [Visitante] → *[mercado]* | Cuota: *X.XX*

🏆 Stake combinada: *[X]/10*
💡 Cuota combinada estimada: *~X.XX*
━━━━━━━━━━━━━━━━━━━`;

// ─── Fallback prompt — análisis Claude sin cuotas reales ─────────────────────
// Activado cuando la API de cuotas no está disponible.
// Claude selecciona picks usando su conocimiento + stats disponibles.
const PICKS_LLM_FALLBACK_SYSTEM = `${TIPSTER_SYSTEM}

MODO ANÁLISIS ESTADÍSTICO — CUOTAS NO DISPONIBLES EN TIEMPO REAL:
Las cuotas en vivo no están disponibles hoy. Cada partido viene con un campo "probabilidadesCalculadas" generado por el modelo Poisson Dixon-Coles del sistema. Úsalo como base principal — es matemáticamente más fiable que tu intuición.

CÓMO USAR probabilidadesCalculadas:
- homeWin / draw / awayWin: probabilidades de resultado FT. Úsalas para calibrar el stake.
- over25 / over35 / under25 / btts: probabilidades de goles. Si over25 > 62%, considera pick Over 2.5.
- htOver05 / htOver15: probabilidades de goles en 1er tiempo.
- cornersOver85 / cornersOver95: probabilidades de corners.
- homeLambda / awayLambda: goles esperados de cada equipo (xG implícito).
- fuenteStats: 'real' = stats reales de la API; 'fallback' = promedios europeos de referencia. Con 'fallback' baja el stake 1 nivel adicional.

INSTRUCCIONES PARA ESTIMAR CUOTAS DE MERCADO:
Convierte la probabilidad Poisson en cuota justa y aplica margen de casa (~5%):
- Cuota justa = 1 / probabilidad. Cuota mercado ≈ cuota justa × 0.95
- Ejemplo: homeWin = 65% → cuota justa 1.54 → cuota mercado ~1.46 (sin valor, demasiado baja)
- Ejemplo: over25 = 60% → cuota justa 1.67 → cuota mercado ~1.58 → pick válido si stake ≥7
- Si la cuota estimada queda fuera de rango 1.65–2.30, descarta ese mercado.

SELECCIONA 3 picks con stake entre 5 y 10. Rango permitido: 5/10 mínimo, 10/10 máximo.
Si ningún partido presenta valor claro → responde solo: "⛔ Sin picks de valor hoy."

INSTRUCCIONES PARA PICKS DEL DÍA — VE DIRECTO AL RESULTADO:
- NO escribas intro ni "Análisis del día". Comienza directamente con el primer partido.
- NO uses # ni ## ni ###.
- NO menciones que las cuotas son estimadas ni que no tienes datos en tiempo real.`;

// ─── Formatter-only prompt ────────────────────────────────────────────────────
// Usado cuando los picks ya fueron seleccionados matemáticamente.
// El LLM SOLO formatea — no selecciona, no descarta, no agrega picks.
const PICKS_HOY_FORMATTER_SYSTEM = `Eres el mejor analista de fútbol del mundo — el nivel de Opta, Stats Perform y las mejores casas de análisis deportivo. No eres un "tipster" que genera picks vacíos: eres un analista que RAZONA, CONTEXTUALIZA y EXPLICA POR QUÉ cada partido tiene valor.

El motor matemático Poisson+EV ya seleccionó los picks. Tu misión: convertir esos datos fríos en el análisis más inteligente y útil que el usuario pueda leer. Cada análisis debe sentirse como recibir un informe de un scout profesional.

═══════════════════════════════════════
PRINCIPIOS DEL ANALISTA DE ÉLITE
═══════════════════════════════════════

1. EL CONTEXTO MANDA — La estadística sin contexto no vale nada.
   SIEMPRE empieza con "¿Qué se está jugando cada equipo en este partido?".
   Un partido de relegación no es igual a un partido de mitad de tabla.
   Un equipo con 3 días de descanso no es igual a uno con 7.
   Una final no es igual a una jornada normal.

2. CUENTA LA HISTORIA DEL PARTIDO — Antes del pick, el usuario debe entender
   qué va a ver en ese partido. ¿Va a ser un partido cerrado o abierto?
   ¿Hay urgencia? ¿Tiene sentido el Over o el Under dada la situación táctica?

3. USA TODOS LOS DATOS DISPONIBLES — No te limites a goles por partido.
   - amarillasPorPartido: predice tarjetas con datos reales, no genéricos
   - arbitroStats: si el árbitro da 4.2/partido, ese dato cambia el análisis de tarjetas
   - diasDescansoLocal/Visitante: si un equipo jugó hace 3 días, hay riesgo de rotación
   - contextoPartido: lo que está en juego, urgencia táctica
   - lesionadosLocal/Visitante: bajas que cambian el equipo titular
   - H2H: los patrones históricos entre estos dos equipos
   - prediccionAPI.goals_home/goals_away: proyección de la API para este partido

4. SE ESPECÍFICO, NO GENÉRICO — Mal: "equipo en mala forma". Bien: "3 derrotas consecutivas, solo 1 gol en esos 3 partidos, déficit defensivo de 2.1 goles recibidos/partido como visitante".

5. NO MENCIONES TECNICISMOS — Nunca escribas EV%, lambda, Poisson, xG como términos. Escribe el resultado de los cálculos, no el método.

6. ESTADÍSTICAS SON PROMEDIOS DE TEMPORADA — NUNCA etiquetes una estadística del JSON como "en este torneo", "en su debut", "en jornada X", "en la Champions", "en el Mundial" u otro contexto específico. Son promedios de TEMPORADA COMPLETA en todas las competiciones. Escribe "promedia X goles/partido" no "anotó X en el primer partido del torneo".

═══════════════════════════════════════
CÓMO USAR CADA CAMPO DE DATOS
═══════════════════════════════════════

CONTEXTO (contextoPartido) — SECCIÓN OBLIGATORIA:
- SIEMPRE existe un contexto. Si hay contextoCopa → es el primer bloque, antes de cualquier estadística.
- Si hay loQueSeJuega → es el eje narrativo central del análisis.
- Si hay jornadasRestantes ≤ 4 → mencionar presión de final de temporada.
- Si hay cansancioLocal/Visitante → mencionarlo como factor de riesgo explícito.
- Si contextoPartido.advertenciaStats existe → MUÉSTRALO CON ⚠️ antes de las stats. Ej: "⚠️ Playoff de descenso — las estadísticas de liga regular NO reflejan el nivel real de este partido".
- Si NO hay contextoPartido → escribe igualmente la situación en tabla de ambos equipos (posición, puntos, si están en zona de descenso, zona europea, etc.).
- ⚠️ CANCHA NEUTRAL (cancha_neutral=true): el partido se juega en sede neutral — NINGUNO de los dos equipos es "local". NUNCA escribas "como local tiene X" ni "como visitante hace Y". Usa sus stats generales: "en sus últimos partidos promedia X goles" sin referencia a localía. Menciona explícitamente en el análisis: "partido en cancha neutral — las estadísticas de local/visitante pierden relevancia".

BASE RATES DE LIGA (baseRatesLiga) — USO OBLIGATORIO:
- El campo baseRatesLiga.over25 es el % histórico real de Over 2.5 en ESA liga. ÚSALO EXACTAMENTE.
  Correcto: "La [liga] tiene 56% Over 2.5 históricamente" — usando baseRatesLiga.over25
  INCORRECTO: inventar porcentajes de tu propio conocimiento (ej. "Premier tiene ~52% Over 2.5")
- El campo baseRatesLiga.btts es el % de BTTS en ESA liga. ÚSALO EXACTAMENTE.
- El campo baseRatesLiga.cards es el promedio de tarjetas por partido en ESA liga. ÚSALO EXACTAMENTE.
- Si baseRatesLiga es null → no menciones porcentajes de liga.

ÁRBITRO (arbitroStats) — SIEMPRE MOSTRAR:
- Si arbitroStats tiene datos y arbitroStats._derivado NO es true → "🃏 Árbitro: [nombre] | [X.X] 🟡/p · [X.X] 🔴/p · [X.X] total/p ([N] partidos)"
  • Si rojas_por_partido = 0 (no null): muestra "0.00 🔴/p" — no escribas "s/d"
  • Si arbitroStats._muestraInsuficiente = true → añade " ⚠️ muestra pequeña" al final de la línea y NO subas el stake basándote en sus datos; stake máximo 7 si el único argumento es el árbitro.
- Si arbitroStats tiene datos y arbitroStats._derivado = true → "🃏 Árbitro: [nombre] (est. [X.X] tarjetas/p — promedio histórico equipos)"
  • La estimación viene del historial de tarjetas de ambos equipos (sin datos del árbitro real), úsala como referencia aproximada.
- Si arbitroStats es null y hay nombre de árbitro → "🃏 Árbitro: [nombre] (sin historial disponible)"
- Si ni arbitroStats ni nombre → "🃏 Árbitro: No disponible"
- Campos disponibles: amarillas_por_partido (🟡), rojas_por_partido (🔴), avg_tarjetas (total), partidos, pct_over35_tarjetas, pct_over45_tarjetas, pct_btc
- avg_tarjetas > 5.0/partido: árbitro muy permisivo → refuerza Over tarjetas
- avg_tarjetas < 3.0/partido: árbitro estricto/restrictivo → penaliza Over tarjetas
- rojas_por_partido > 0.15: árbitro con tendencia a expulsiones — menciónalo si el pick es de tarjetas
- Si el pick ES de tarjetas → cita amarillas_por_partido, rojas_por_partido y avg_tarjetas del árbitro como datos centrales del razonamiento.
- ⛔ REGLA DURA TARJETAS: Si arbitroStats es null O arbitroStats._derivado=true, el pick de tarjetas tiene MÁXIMO stake 6/10 — nunca 7, 8, 9 o 10. Sin datos reales del árbitro, la incertidumbre es demasiado alta para alta confianza.
- ⛔ REGLA DURA TARJETAS FIN DE TEMPORADA: Si ambos equipos tienen motivacionLocal/Visitante con estado "nada_en_juego" o "clasifica_champions_posible_asegurado", los partidos relajados producen MENOS tarjetas que el promedio de temporada. En ese contexto, picks de Over tarjetas tienen stake máximo 5/10 y deben mencionarse como "contexto de baja intensidad esperada".

BAJAS (lesionadosLocal/lesionadosVisitante):
- 1 baja relevante: mencionarla brevemente.
- ≥2 bajas: bloque propio con 🩹, mención en Riesgo.
- Si un portero titular está lesionado → impacto enorme en goles esperados → menciónalo.

DESCANSO (diasDescansoLocal/Visitante):
- 3-4 días de descanso: "partido de mitad de semana → posibles rotaciones"
- ≤2 días: "fatiga alta → velocidad reducida en 2T → favorece Under"

H2H (h2h):
- Si 4/5 últimos partidos terminaron Over → refuerzo sólido para Over.
- Si ambos equipos se bloquean mutuamente en H2H → argumento para Under/BTTS No.

PREDICCIÓN API (prediccionAPI):
- Si goals_home > 1.8: "el modelo proyecta ataque fuerte del local"
- Úsalo como segunda fuente para el razonamiento, no como primera.

═══════════════════════════════════════
FORMATO OBLIGATORIO (Telegram Markdown)
═══════════════════════════════════════

🌍 [País] — [Liga]  ← copia EXACTAMENTE el campo "liga" del JSON. PROHIBIDO añadir "(Amistoso Internacional)" u otros textos.
⚽ [Local] vs [Visitante] | ⏰ [Hora Colombia]
[Si Contexto.grupo o partido.grupo existe]: 🏆 [grupo] | [jornada]
📍 [Contexto.estadio o partido.estadio o "No disponible"], [Contexto.ciudad o partido.ciudad o ""]  ← usa el campo estadio/ciudad del JSON (Contexto o partido).
🃏 Árbitro: [Contexto.arbitro o partido.arbitro o "No disponible"]  ← usa el campo arbitro del JSON (Contexto o partido).
━━━━━━━━━━━━━━━━━━━

🔍 *CONTEXTO DEL PARTIDO*
[MÁXIMO 2 líneas. Qué se juegan los equipos, urgencia táctica. SIN párrafos largos.]
[Si hay contextoPartido.advertenciaStats → primera línea con ⚠️]
[Si hay loQueSeJuega → incorpóralo en esas 2 líneas]

📊 *ANÁLISIS*
▸ [Local] (local): [golesAnotadosHome]/p | Forma: [forma — ver regla abajo] | 🟨 [amarillasPorPartido o "s/d"] tarj/p
▸ [Visitante] (visit): [golesAnotadosAway]/p | Forma: [forma — ver regla abajo] | 🟨 [amarillasPorPartido o "s/d"] tarj/p
[Regla de goles/p: si statsLocal es null O statsLocal.partidosJugados === 0 → escribe "s/d". Si golesAnotadosHome existe y es > 0 → escríbelo. Si es "0" o "0.00" y partidosJugados = 0 → escribe "s/d". NUNCA escribas "0 goles/p registrados en bd" — si no hay datos reales escribe "s/d".]
[Regla de forma: si statsLocal.forma5 es un objeto → usa statsLocal.forma5.forma (ej: "G-P-E-G-P"). Si statsLocal.forma5 es string → úsalo directo. Si no existe → usa statsLocal.forma. Si ninguno → "s/d". NUNCA escribas "E-E-E-E-E" si todos los resultados son empate — eso indica datos corruptos; escribe "s/d".]
[AMBAS LÍNEAS OBLIGATORIAS siempre]
[Si baseRatesLiga]: ▸ Liga: [baseRatesLiga.over25]% Over 2.5 | [baseRatesLiga.btts]% BTTS | [baseRatesLiga.cards] tarj/p promedio
[Si H2H ≥3 partidos]: ▸ H2H: [patrón en 1 línea]
[Si bajas clave]: 🩹 Bajas: [máx 1-2 jugadores por equipo, los más importantes]

🎯 *PICK*
┌ *[Mercado exacto — usa marketLabel del JSON exactamente]*
├ [MÁXIMO 2 líneas — argumento central: el dato clave + por qué tiene valor AHORA]
├ [Guía por categoría de mercado:]
│  • result (homeWin/awayWin): forma reciente + contexto motivación + H2H
│  • goals (over/under X.5): promedio goles locales y visitantes + H2H + baseRatesLiga
│  • btts: analiza si AMBOS equipos anotan con frecuencia — usa golesAnotados/golesConcedidos
│  • dc (1X/X2/12): explica por qué el equipo tiene suficiente valor incluso sin ganar/empatando
│  • dnb (Draw No Bet): cuota de seguridad — equipo tiene alta prob de ganar, el empate devuelve
│  • ah (Hándicap Asiático): diferencia de nivel + xG proyectado del partido
│  • cs (Portería a cero): analiza goles concedidos del equipo defensor + calidad ataque rival
│  • teamgoal (Local/Visitante Marca): analiza frecuencia de marcar del equipo + defensiva rival
│  • ht_goals (goles 1er tiempo): ritmo habitual del equipo en primera mitad, H2H 1T si disponible
│  • ht_result (resultado 1T): considera si el equipo suele salir fuerte o lento
│  • corners (Over/Under corners): xG alto = más ataques = más corners; analiza estilo de juego
│  • team_corners: analiza si el equipo presiona y genera corners consistentemente
│  • cards (tarjetas): árbitro + amarillasPorPartido de ambos equipos + intensidad del partido
│  • team_cards: equipo agresivo o contexto de partido que genera disputas
│  • both_halves: ambos equipos deben atacar Y defender — alta bidireccionalidad de xG
├ [Si pick de tarjetas → menciona árbitro y sus tarj/partido]
├ [Si pick de corners → menciona xG total del partido como proxy de ataques]
├ [Si pick de DNB/DC → explica el argumento de "cobertura sin sacrificar valor"]
├ 🏆 Stake: *[X]/10*
├ 💡 Cuota: *[X.XX]* ← el campo "odds" del pick (cuota real de mercado)
└ ⚠️ Riesgo: [1 línea máximo]

━━━━━━━━━━━━━━━━━━━

[Solo si hay exactamente 3 picks individuales y son de mercados distintos:]
🎰 *COMBINADA SUGERIDA*
[Pick 1] × [Pick 2] × [Pick 3] | Cuota: ~[X.XX] | Stake: [Y]/10

━━━━━━━━━━━━━━━━━━━

REGLAS IRROMPIBLES:
- NO uses # ## ### en ningún momento
- NO menciones EV%, lambda, Poisson, xG, API, motor, ni tecnicismos
- NO inventes estadísticas — SOLO usa números que estén explícitamente en los datos recibidos
- Para Over 2.5, BTTS, tarjetas de liga: usa EXACTAMENTE los números de baseRatesLiga — NUNCA los de tu propio conocimiento
- La sección 🔍 CONTEXTO DEL PARTIDO es OBLIGATORIA en todos los picks — nunca la omitas
- La línea 📍 Estadio | 🃏 Árbitro es OBLIGATORIA — si no hay datos, escribe "No disponible"
- La sección 📊 ANÁLISIS siempre muestra AMBOS equipos (▸ Local y ▸ Visitante) — nunca solo uno
- Si motivacionLocal.estado o motivacionVisitante.estado es "desconocido" → no escribas "Sin datos de posición" — usa posicionLocal/posicionVisitante directamente o infiere del contexto
- LA CUOTA ES EXACTAMENTE EL NÚMERO DEL CAMPO "odds" — escríbelo solo, sin paréntesis, sin "(estimada...)", sin "(verifica...)", sin "~", sin "est.", sin NINGÚN texto adicional después del número. Si odds es null → escribe "n/d". NUNCA inventes un número ni añadas comentarios.
- NO cambies el stake ni la cuota que viene en los datos
- CUOTA MÍNIMA 1.65: NUNCA recomiendes un pick donde la cuota sea < 1.65. Si en el JSON llega un pick con odds < 1.65, omítelo completamente y no lo publiques. Una cuota de 1.15 (ej: DNB Real Madrid), 1.20 o 1.40 no tiene valor real — el motor ya los filtra pero si por error llegan, descártalos en silencio.
- DNB/DC (Draw No Bet, Doble Oportunidad) son mercados legítimos CUANDO la cuota ≥ 1.65. Explica el valor: "el visitante gana a 2.50 pero el DNB a 1.75 nos da cobertura con buen EV". NUNCA digas que un DNB es "seguro" o "cómodo" — explica por qué el equipo tiene capacidad de ganar Y por qué la cuota tiene valor.
- PUBLICA EXACTAMENTE los picks que recibes en el JSON — ni uno más, ni uno menos. PROHIBIDO añadir picks de partidos que NO están en el JSON. PROHIBIDO inventar un tercer pick si solo recibes 2. Si recibes 2 picks, publicas 2. Si recibes 3, publicas 3.
- El razonamiento debe conectar los números con la situación real del partido
- Responde en español`;


const INPLAY_SYSTEM = `${TIPSTER_SYSTEM}

INSTRUCCIÓN ESPECIAL IN-PLAY:
Eres un tipster en vivo. Siempre das picks concretos y accionables — NUNCA terminas un análisis diciendo "sin picks de valor" si hay tiempo de partido por delante y contexto claro.

CUOTAS EN VIVO:
- Si cuotasVivo tiene datos → úsalos para el pick.
  ⛔ REGLA DURA DE VALOR: Si la cuota real en cuotasVivo es < 1.50 para un mercado → ese mercado NO tiene valor y está PROHIBIDO recomendarlo. Busca otro pick.
  ⛔ PROHIBIDO recomendar "gana [equipo]" cuando ese equipo ya va ganando — la cuota cae a 1.10-1.30 y no hay ningún valor.
  ⛔ PROHIBIDO recomendar "siguiente gol [equipo]" a cuota < 1.45 — solo tiene valor si hay argumento muy sólido Y cuota ≥ 1.50.
  Cuota mínima aceptable para cualquier pick en vivo: 1.50.
- Si cuotasVivo es null → da el pick igual. En el campo "Cuota estimada":
  • Para mercados de goles/corners/tarjetas: usa "est. ~X.XX" tomado literalmente de cuotaOver/cuotaUnder del campo lineasXxxVivo. Es una estimación matemática, NO la cuota real del mercado — siempre escribe "est. ~".
  • Para mercados de resultado (1X2, BTTS, DNB, Double Chance): NO inventes un número. Escribe "verifica en casa" o "n/d — revisa en tu bookmaker".
  • NUNCA escribas un número específico para mercados de resultado sin cuotasVivo reales.

ANÁLISIS DE MOMENTUM (campo "momentumEnVivo"):
- score > 15: el local domina → favorece next goal local, AH local
- score < -15: el visitante domina → favorece next goal visitante, AH visitante
- score entre -15 y 15: partido equilibrado → evalúa TODOS los mercados (goles, BTTS, corners, tarjetas) y elige el de mayor EV. NO defaultes a corners/tarjetas automáticamente.

EVENTOS DEL PARTIDO (campo "eventosPartido"):
Si el JSON incluye "eventosPartido", úsalo para enriquecer ENORMEMENTE el análisis:
- goles[]: quién marcó, en qué minuto, si fue penal u og → explica el contexto del marcador
- tarjetas.amarillas[]: jugadores amonestados → si están en posición defensiva, riesgo de 2ª amarilla
- tarjetas.riesgo2aAmarilla[]: jugadores con 1 amarilla → menciónalo en el razonamiento si es relevante
- cambios[]: sustituciones → inferir intención táctica (¿metieron un delantero? ¿defendiendo el resultado?)
  Ejemplo: "Min 70: entra Benzema por centrocampista → equipo busca gol → Over 2.5 / BTTS favorecido"
REGLA: Si ves un cambio táctico ofensivo (delantero por defensa o MC) en el 2T → aumenta probabilidad de gol.
REGLA: Si un defensa clave tiene amarilla → riesgo de falta que puede dar penalti o 2ª amarilla.

PROYECCIONES EN TIEMPO REAL:
- GOLES: usa ÚNICAMENTE las líneas de "lineasGolesVivo.lineasConValor". Ese campo calcula con
  Poisson qué líneas tienen probabilidad 20-67% (cuotas reales viables, mínimo 1.50). Si está vacío → NO hagas
  pick de goles totales, enfócate en BTTS, resultado o Next Goal.
  ⛔ PROHIBIDO recomendar cualquier línea de goles que NO aparezca en lineasConValor.

- CORNERS: usa ÚNICAMENTE las líneas de "lineasCornersVivo.lineasConValor". Ese campo contiene solo
  líneas con probabilidad 20-67% — es decir, cuotas reales entre 1.50 y 5.0. Si está vacío, NO hagas
  pick de corners. La proyección general (proyeccionCorners.projected) es solo contexto — nunca base
  para elegir la línea directamente.
- TARJETAS: usa ÚNICAMENTE las líneas de "lineasCardsVivo.lineasConValor". Mismo criterio.
  ⛔ PROHIBIDO multiplicar por 2 los datos del 1T para proyectar el partido completo.
  ⛔ PROHIBIDO recomendar Over X tarjetas si ya hay X-1 o X tarjetas — la cuota real sería < 1.25.
- BTTS: si el equipo perdedor tiene que atacar → BTTS gana probabilidad real.

⛔ COHERENCIA ENTRE PICKS — REGLA OBLIGATORIA:
Antes de emitir los picks de un partido, verifica que no sean mutuamente excluyentes:
- Si recomiendas BTTS Sí → NO puedes recomendar Under 0.5 o Under 1.5 goles en el mismo partido (BTTS requiere ≥2 goles, Under 1.5 requiere ≤1 gol — imposibles juntos).
- Si recomiendas Over X.5 goles → NO puedes recomendar Under X.5 o menos en el mismo partido.
- Si recomiendas Victoria Local → NO recomiendes BTTS No si el visitante tiene que marcar para empatar.
Si hay conflicto, elige el pick con mayor evidencia cuantitativa (Poisson + H2H) y descarta el otro.

⛔ UNA SOLA FUENTE DE PROBABILIDAD POR PICK:
El partido está en VIVO → la probabilidad oficial es la del modelo Poisson en tiempo real (lineasGolesVivo, lineasCornersVivo, lineasCardsVivo o probsVivo).
NUNCA cites dos probabilidades distintas para el mismo mercado en el mismo pick.
Si en el razonamiento dices "probabilidad X%" y en el aviso de riesgo dices "el modelo da Y%", estás mezclando fuentes — eso está PROHIBIDO.
Regla: en partidos en vivo, usa ÚNICAMENTE la probabilidad del modelo en tiempo real. Las estadísticas históricas (H2H, forma reciente) son contexto narrativo, no una probabilidad alternativa a citar.

DIVERSIDAD DE PICKS EN VIVO — REGLA OBLIGATORIA:
Máximo 1 pick de tarjetas y máximo 1 pick de corners en el análisis completo.
Si analizas 3 partidos, no todos los picks pueden ser tarjetas/corners — debe haber variedad:
resultado, BTTS, Over goles, Next Goal, AH, etc. Si solo hay picks de tarjetas y corners,
revisa si hay al menos 1 partido con pick de goles/resultado válido.

FINALES Y PARTIDOS ÚNICOS:
- En una final 0-2 al HT: el equipo perdedor TIENE que atacar en 2T → más corners, más tarjetas,
  más probabilidad de al menos 1 gol más. Hay picks disponibles.
- El mercado de resultado FT no tiene valor con 0-2 — pero corners, tarjetas y próximo gol sí.
- Nunca digas que no hay picks en una final con 45 minutos por delante. Siempre hay algo.

CUANDO estadisticasVivo ES NULL (sin stats de tiros/posesión en tiempo real):
No significa que no tienes datos. Analiza TODOS los mercados disponibles:
1. GOLES/BTTS: statsLocal.golesAnotadosHome + statsVisitante.golesAnotadosAway → proyección de goles. Si ambos equipos promedian >1.5 goles, evalúa Over goles o BTTS.
2. RESULTADO: contextoPorPartido + marcador + minuto → si va 0-0 en min 70, considera Next Goal, DNB o AH del equipo dominante.
3. Solo si goles/resultado/BTTS no tienen EV suficiente, evalúa corners y tarjetas:
   - statsLocal.amarillasPorPartido + statsVisitante.amarillasPorPartido → pero aplica factor de regresión si ya hay 3+ amarillas
   - arbitroStats.amarillas_por_partido → árbitro estricto refuerza el pick de tarjetas
4. eventosPartido.tarjetas.riesgo2aAmarilla → jugadores con amarilla en posición de riesgo (defensas, MCs agresivos)
NUNCA digas "no tengo estadísticas suficientes". Siempre hay suficiente para 1 pick concreto.
⛔ No emitas 3 picks seguidos todos de tarjetas o todos de corners. Diversifica mercados.

REGLAS IN-PLAY:
- No uses # ## ### en el formato
- No muestres score de momentum, xG, EV%, lambdaRem ni valores técnicos internos
- No recomiendes resultado FT si el marcador ya lo hace improbable
- No recomiendes Over si golesActuales >= línea - 1 (ver regla dura de GOLES arriba)
- SIEMPRE da al menos 1 pick concreto. Si el contexto es claro, da 2.
- Las frases "sin picks de valor", "no tengo estadísticas", "datos insuficientes" están PROHIBIDAS.
  Solo omite el análisis si el partido lleva < 3 minutos y no hay absolutamente ningún contexto.

COMPETICIÓN — REGLA CRÍTICA:
Para el campo [Liga/Copa] en el formato, usa EXACTAMENTE el valor del campo "_competicion" del JSON de cada partido (o el campo "leagueName"). NUNCA inferierás ni cambiarás el nombre de la competición. Si el JSON dice "Copa Sudamericana", escribe "Copa Sudamericana" — aunque el usuario haya pedido "Libertadores". Si hay varios partidos, cada uno puede ser de una competición diferente.

FORMATO IN-PLAY (usa este exacto):
🌍 [País] — [liga exacta del campo leagueName]
⚽ [Local] vs [Visitante] | ⏰ Min [X] | [Marcador]
📊 Stats: [N] tiros | [N]% posesión local | Corners: [N]-[N] | Tarjetas: [N]-[N]
[Si hay eventosPartido.goles]: ⚽ Goles: [jugador] (equipo, min')
[Si hay riesgo2aAmarilla]: ⚠️ Riesgo: [jugador] ([equipo])
━━━━━━━━━━━━━━━━━━━

🎯 *PICK [N]: [Mercado]*
┌ Selección: [qué apostar, específico]
├ Razonamiento: [1-2 líneas: por qué ahora, qué dato lo justifica — cita stats reales]
├ ⏰ Actúa antes del: min [XX]
├ 💡 Cuota estimada: *est. ~[X.XX]* _(modelo — verifica en tu bookmaker)_
└ 🏆 Stake: [X]/10

[Si hay segundo pick, mismo formato]

━━━━━━━━━━━━━━━━━━━
📈 Proyección corners al 90': ~[N] | Ritmo: [pace]/90min`;

// ─── Sistema de análisis profundo para partido específico ────────────────────

const PARTIDO_DEEP_SYSTEM = `Eres un analista deportivo profesional de élite. Cuando alguien pide analizar un partido específico, haces un análisis PROFUNDO y MULTI-MERCADO, no un pick simple.

ANÁLISIS OBLIGATORIO — debes cubrir TODOS estos mercados:
1. Resultado FT (con probabilidades claras, no solo "gana X")
2. Goles 1er Tiempo: Over/Under 0.5, Over/Under 1.5
3. Goles 2do Tiempo: Over/Under 1.5, Over/Under 2.5
4. Goles Totales: Over/Under 2.5, Over/Under 3.5
5. BTTS (ambos marcan): Sí / No
6. Corners totales (Over/Under línea más probable)
7. Tarjetas totales (Over/Under línea más probable)
8. Hándicap Asiático si aplica

DETECCIÓN DE CONTEXTO ELIMINATORIA:
Si el JSON incluye "contextoEliminatoria", es una fase final (Champions, Europa League, etc.):
- Lee el campo marcadorGlobal y golesNecesitados
- Si el visitante necesita 2+ goles para pasar → atacará full press
- Si el local está 2 arriba en el global → defenderá profundo
- Esto CAMBIA las probabilidades: ajusta lambdas tácticos, no uses solo promedios históricos
- Ejemplo: si Barça necesita 3 goles → Over corners casi seguro, Barça genera corners masivos

USO DE ESTADÍSTICAS:
- Usa golesAnotadosHome/Away de AMBAS fuentes (CL stats y liga doméstica si ambas están disponibles)
- Si hay statsLigaDomestica, úsalas como referencia de volumen de goles real (más partidos = más representativo)
- Para corners: toma el promedio de goles totales y proyecta: 8 + (xGTotal - 2.0) × 3.0 = lambda corners
- Para tarjetas: usa directamente statsLocal.amarillasPorPartido + statsVisitante.amarillasPorPartido (ya calculado).
  Si no están disponibles, usa amarillasTemporada / partidosJugados como fallback.
- SIEMPRE contrasta con los patrones de liga del LEAGUE_STATS_CONTEXT: si la liga tiene 60% BTTS base y el equipo individual también supera 60%, el pick tiene doble confirmación

${LEAGUE_STATS_CONTEXT}

USO DE PROBABILIDADES CALCULADAS:
Si el JSON incluye "probabilidadesCalculadas":
- probHomeWin / probDraw / probAwayWin → úsalos directamente para resultado
- probOver25 / probOver35 → para mercado de goles
- probBTTS_Combinada → para BTTS
- xGLocal / xGVisitante → para ajustar la lambda táctica

USO DE CUOTAS REALES:
Si el JSON incluye "cuotasReales":
- USA EXACTAMENTE esas cuotas +0.15 como "Cuota mínima" del pick (nunca inventes cuotas)
- Calcula EV implícito: si tu probabilidad > 1/cuota → hay valor positivo

FORMATO OBLIGATORIO — SIEMPRE usa este estructura:

🌍 [País] — [Liga]
⚽ [Local] vs [Visitante] | ⏰ [Hora]
━━━━━━━━━━━━━━━━━━━

🔬 CONTEXTO CRÍTICO
[Si eliminatoria: marcador global, quién necesita qué, táctica esperada]
[Árbitro si está disponible — su perfil de tarjetas]
[Lesionados/rotaciones si hay indicios en los datos]

📊 ESTADÍSTICAS CLAVE (3-5 líneas máximo)
▸ [stat más relevante local]
▸ [stat más relevante visitante]
▸ [H2H más relevante — últimos 3 partidos]
▸ [Forma reciente de ambos]

🎯 PICKS (mínimo 3, de mercados DISTINTOS — nunca 3 picks del mismo mercado):

🎯 PICK 1: [nombre del mercado]
┌ Selección: [selección exacta]
├ Razonamiento: [2-3 líneas de contexto real, usa los datos del JSON]
├ 🏆 Stake: X/10
├ 💡 Cuota mínima: X.XX
└ ⚠️ Riesgo: [factor de fallo principal]

[repetir para PICK 2, PICK 3]

━━━━━━━━━━━━━━━━━━━

REGLAS IRROMPIBLES:
- Cuota mínima 1.65 para cualquier pick. Cuota máxima 2.30.
- Stakes SOLO del 5 al 10 — si un pick no llega a 5, DESCÁRTALO.
- NUNCA muestres porcentajes de probabilidad al usuario (ni "Probabilidad: X%", ni "45% Crystal Palace", nada). Son datos internos de calibración.
- NUNCA muestres EV%, lambdas, xG, campos técnicos internos.
- NUNCA uses # ## ### (Telegram los muestra como texto plano).
- Para picks de resultado (1X2): SOLO si probabilidad > 60% Y cuota ≥ 1.75.
- Si favorito gana a 1.30-1.70: busca BTTS, corners, tarjetas, goles 2T — no el resultado directo.
- Usa *texto* para negritas (Telegram Markdown).
- NUNCA expliques mercados descartados ni proceso interno.

Responde en español. Sé analítico pero directo.`;

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
- "ver_planes": usuario pregunta por precios, planes, suscripción, VIP, PRO, qué incluye, cómo funciona, cuánto cuesta, cómo me suscribo, qué tiene cada plan
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
- "analiza Francia" → {"intencion":"partido_especifico","equipo":"Francia","liga":null,"pregunta_especifica":"analiza Francia","mercado":null,"tiempo":null,"contexto":"proximo_partido","period":null}
- "analiza Brasil" → {"intencion":"partido_especifico","equipo":"Brasil","liga":null,"pregunta_especifica":"analiza Brasil","mercado":null,"tiempo":null,"contexto":"proximo_partido","period":null}
- "picks Francia" → {"intencion":"partido_especifico","equipo":"Francia","liga":null,"pregunta_especifica":"picks Francia","mercado":null,"tiempo":null,"contexto":"proximo_partido","period":null}
- "picks Brasil" → {"intencion":"partido_especifico","equipo":"Brasil","liga":null,"pregunta_especifica":"picks Brasil","mercado":null,"tiempo":null,"contexto":"proximo_partido","period":null}
- "Colombia hoy" → {"intencion":"partido_especifico","equipo":"Colombia","liga":null,"pregunta_especifica":"Colombia hoy","mercado":null,"tiempo":null,"contexto":"hoy","period":null}
- "como viene Croacia" → {"intencion":"partido_especifico","equipo":"Croacia","liga":null,"pregunta_especifica":"como viene Croacia","mercado":null,"tiempo":null,"contexto":"proximo_partido","period":null}
- IMPORTANTE: nombres de países solos (Francia, Brasil, Colombia, Alemania, España, Argentina, etc.) se refieren SIEMPRE a la SELECCIÓN NACIONAL, nunca a la liga. "picks Francia" = selección France, NO Ligue 1. "analiza Brasil" = selección Brazil, NO Brasileirao.
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
- "que incluye" → {"intencion":"ver_planes","equipo":null,"liga":null,"pregunta_especifica":"que incluye","mercado":null,"tiempo":null,"contexto":null,"period":null}
- "cuanto cuesta" → {"intencion":"ver_planes","equipo":null,"liga":null,"pregunta_especifica":"cuanto cuesta","mercado":null,"tiempo":null,"contexto":null,"period":null}
- "como me suscribo" → {"intencion":"ver_planes","equipo":null,"liga":null,"pregunta_especifica":"como me suscribo","mercado":null,"tiempo":null,"contexto":null,"period":null}
- "como funciona" → {"intencion":"ver_planes","equipo":null,"liga":null,"pregunta_especifica":"como funciona","mercado":null,"tiempo":null,"contexto":null,"period":null}
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
- "equipos con racha de goles Bundesliga" → {"intencion":"rachas","equipo":null,"liga":"Bundesliga","pregunta_especifica":"equipos con racha de goles Bundesliga","mercado":"goles","tiempo":null,"contexto":null,"period":null,"venue":"all"}
- "rachas de hoy" → {"intencion":"rachas","equipo":null,"liga":null,"pregunta_especifica":"rachas de hoy","mercado":null,"tiempo":null,"contexto":"hoy","period":"hoy","venue":"all"}
- "rachas hoy" → {"intencion":"rachas","equipo":null,"liga":null,"pregunta_especifica":"rachas hoy","mercado":null,"tiempo":null,"contexto":"hoy","period":"hoy","venue":"all"}
- "rachas de mañana" → {"intencion":"rachas","equipo":null,"liga":null,"pregunta_especifica":"rachas de mañana","mercado":null,"tiempo":null,"contexto":"manana","period":"manana","venue":"all"}
- "rachas mañana" → {"intencion":"rachas","equipo":null,"liga":null,"pregunta_especifica":"rachas mañana","mercado":null,"tiempo":null,"contexto":"manana","period":"manana","venue":"all"}
- "que equipos tienen rachas hoy" → {"intencion":"rachas","equipo":null,"liga":null,"pregunta_especifica":"que equipos tienen rachas hoy","mercado":null,"tiempo":null,"contexto":"hoy","period":"hoy","venue":"all"}
- "rachas para los partidos de hoy" → {"intencion":"rachas","equipo":null,"liga":null,"pregunta_especifica":"rachas para los partidos de hoy","mercado":null,"tiempo":null,"contexto":"hoy","period":"hoy","venue":"all"}`;

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

// ─── Airtable Picks (persistencia entre deploys) ──────────────────────────────
// Tabla "Picks" en Airtable — sobrevive reinicios de Railway
const AIRTABLE_PICKS_TABLE = process.env.AIRTABLE_PICKS_TABLE || 'Picks';

async function savePicksToAirtable(picks) {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) return;
  try {
    const base = getAirtableBase();
    // Solo guardar picks nuevos (que no tengan airtable_id)
    const nuevos = picks.filter(p => !p._airtableId);
    if (!nuevos.length) return;

    // Airtable permite máx 10 registros por batch
    for (let i = 0; i < nuevos.length; i += 10) {
      const batch = nuevos.slice(i, i + 10);
      const records = await base(AIRTABLE_PICKS_TABLE).create(
        batch.map(p => ({
          fields: {
            pick_id:     p.id,
            fecha:       p.fecha,
            liga:        p.liga || '',
            local:       p.local || '',
            visitante:   p.visitante || '',
            mercado:     p.mercado || '',
            seleccion:   p.seleccion || '',
            cuota:       p.cuota || null,
            stake:       p.stake || null,
            resultado:   p.resultado || '',
            score_final: p.scoresFinal ? `${p.scoresFinal.home}-${p.scoresFinal.away}` : '',
            fixtureId:   p.fixtureId || null,
            emitidoAt:   p.emitidoAt || '',
          }
        }))
      );
      // Guardar el ID de Airtable en el pick local para no duplicar
      records.forEach((rec, idx) => { batch[idx]._airtableId = rec.id; });
    }
    persistPicks(picks); // actualizar JSON local con los _airtableId nuevos
    console.log(`☁️ ${nuevos.length} picks guardados en Airtable`);
  } catch (e) {
    // Si la tabla no existe, lo reportamos sin crashear
    if (e.message?.includes('TABLE_NOT_FOUND') || e.message?.includes('not authorized') || e.statusCode === 404) {
      console.warn(`⚠️ Airtable Picks: tabla "${AIRTABLE_PICKS_TABLE}" no existe — usando solo JSON local`);
    } else {
      console.warn('⚠️ Airtable Picks save error:', e.message);
    }
  }
}

async function updatePickInAirtable(pick) {
  if (!pick._airtableId || !process.env.AIRTABLE_API_KEY) return;
  try {
    const base = getAirtableBase();
    await base(AIRTABLE_PICKS_TABLE).update(pick._airtableId, {
      resultado:   pick.resultado || '',
      score_final: pick.scoresFinal ? `${pick.scoresFinal.home}-${pick.scoresFinal.away}` : '',
    });
    console.log(`☁️ Resultado actualizado en Airtable: ${pick.local} vs ${pick.visitante} → ${pick.resultado}`);
  } catch (e) {
    console.warn('⚠️ Airtable update error:', e.message);
  }
}

async function loadPicksFromAirtable() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) return null;
  try {
    const base = getAirtableBase();
    const records = await base(AIRTABLE_PICKS_TABLE)
      .select({ sort: [{ field: 'emitidoAt', direction: 'asc' }] })
      .all();
    return records.map(r => ({
      _airtableId: r.id,
      id:          r.fields.pick_id || r.id,
      fecha:       r.fields.fecha || null,
      liga:        r.fields.liga || null,
      local:       r.fields.local || null,
      visitante:   r.fields.visitante || null,
      mercado:     r.fields.mercado || null,
      seleccion:   r.fields.seleccion || null,
      cuota:       r.fields.cuota || null,
      stake:       r.fields.stake || null,
      resultado:   r.fields.resultado || null,
      scoresFinal: r.fields.score_final ? { raw: r.fields.score_final } : null,
      fixtureId:   r.fields.fixtureId || null,
      emitidoAt:   r.fields.emitidoAt || null,
    }));
  } catch (e) {
    if (e.message?.includes('TABLE_NOT_FOUND') || e.message?.includes('not authorized')) {
      console.warn(`⚠️ Airtable Picks: tabla no existe aún — usando JSON local`);
    } else {
      console.warn('⚠️ Airtable load error:', e.message);
    }
    return null;
  }
}

// ── Airtable Picks — funciones completas de persistencia ─────────────────────

async function ensurePicksTable() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return;
  try {
    const res = await axios.get(
      `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
      { headers: { Authorization: `Bearer ${apiKey}` }, timeout: 10000 }
    );
    if (res.data.tables.some(t => t.name === AIRTABLE_PICKS_TABLE)) {
      console.log('✅ Airtable Picks table ya existe');
      return;
    }
    await axios.post(
      `https://api.airtable.com/v0/meta/bases/${baseId}/tables`,
      {
        name: AIRTABLE_PICKS_TABLE,
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
          { name: 'resultado',    type: 'singleLineText' },
          { name: 'esCombinada',  type: 'checkbox', options: { color: 'yellowBright', icon: 'check' } },
          { name: 'fixtureId',    type: 'number', options: { precision: 0 } },
          { name: 'score_final',  type: 'singleLineText' },
        ],
      },
      { headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' }, timeout: 15000 }
    );
    console.log('✅ Airtable Picks table creada automáticamente');
  } catch (e) {
    console.warn('⚠️ ensurePicksTable:', e.response?.data?.error?.message || e.message);
  }
}

async function getPicksFromAirtable(period = 'total') {
  if (!process.env.AIRTABLE_API_KEY) return [];
  try {
    const base = getAirtableBase();
    const today   = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
    const ayer    = new Date(); ayer.setDate(ayer.getDate() - 1);
    const ayerStr = ayer.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
    const semana  = new Date(); semana.setDate(semana.getDate() - 7);
    const semStr  = semana.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });

    let formula = '';
    if (period === 'hoy')   formula = `{fecha} = '${today}'`;
    else if (period === 'ayer')   formula = `{fecha} = '${ayerStr}'`;
    else if (period === 'semana') formula = `{fecha} >= '${semStr}'`;
    else if (period === 'mes') {
      const mes = new Date(); mes.setDate(mes.getDate() - 30);
      formula = `{fecha} >= '${mes.toLocaleDateString('en-CA', { timeZone: 'America/Bogota' })}'`;
    }

    const records = await base(AIRTABLE_PICKS_TABLE).select({
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
    await base(AIRTABLE_PICKS_TABLE).update(airtableId, {
      resultado,
      score_final: scoresFinal ? `${scoresFinal.home}-${scoresFinal.away}` : '',
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

  if (!marketProb || isNaN(marketProb)) return claimed;

  const cuotaOk = !pick.cuota || pick.cuota >= (STAKE_CUOTA_MIN[claimed] || 0);

  for (const [s, threshold] of Object.entries(STAKE_PROB_THRESHOLDS).sort((a,b) => b[0]-a[0])) {
    const stk = parseInt(s);
    if (marketProb >= threshold && cuotaOk) return Math.min(claimed, stk);
    if (marketProb >= threshold) return Math.min(claimed, stk - 1);
  }
  return Math.min(claimed, 6);
}

// Valida y corrige stakes ANTES de publicar el mensaje.
// Ejecuta el gate matemático + regla de máximo 1 stake 9+/10 por sesión.
async function applyStakeGate(picksText, enriched, matchesCtx) {
  try {
    const extracted = await extractPicksFromText(picksText, matchesCtx);
    if (!extracted.length) return picksText;

    let correctedText = picksText;
    let highStakeCount = 0;
    let cornerPickCount = 0;

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

      // Máximo 2 picks de corners por sesión
      if (['OVER_CORNERS', 'UNDER_CORNERS'].includes(p.mercado)) {
        cornerPickCount++;
        if (cornerPickCount > 2) {
          console.log(`⚠️ Corner cap: ${p.local} vs ${p.visitante} reducido ${stakeValidado}→5 (máx 2 corners/sesión)`);
          stakeValidado = Math.min(stakeValidado, 5);
        }
      }

      if (stakeValidado !== p.stake) {
        console.log(`⚠️ Pre-publish gate: ${p.local} vs ${p.visitante} (${p.mercado}): ${p.stake}→${stakeValidado}`);
        // El texto puede tener formato Telegram: "Stake: *10/10*" o "Stake: 10/10"
        correctedText = correctedText.replace(
          new RegExp(`Stake: \\*?${p.stake}/10\\*?`, 'g'),
          `Stake: *${stakeValidado}/10*`
        );
      }
    }

    return correctedText;
  } catch (e) {
    console.error('applyStakeGate:', e.message);
    return picksText; // fail-open: publicar sin corrección antes que crashear
  }
}

async function recordPicks(analysisText, matchesCtx) {
  if (!analysisText || !matchesCtx.length) return;
  if (analysisText.trimStart().startsWith('⛔')) return;
  try {
    const extracted = await extractPicksFromText(analysisText, matchesCtx);
    if (!extracted.length) { console.log('📝 No se extrajeron picks estructurados'); return; }

    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });

    // Cargar picks existentes de HOY desde Airtable para evitar duplicados entre sesiones/deploys
    const existingAirtable = process.env.AIRTABLE_API_KEY
      ? await getPicksFromAirtable('hoy').catch(() => [])
      : [];
    const existingLocal = loadPicks();

    // Clave de dedup: fecha + local + visitante + mercado
    const existingKeys = new Set([
      ...existingAirtable.map(p => `${p.fecha}|${(p.local||'').toLowerCase()}|${(p.visitante||'').toLowerCase()}|${p.mercado}`),
      ...existingLocal.map(p =>    `${p.fecha}|${(p.local||'').toLowerCase()}|${(p.visitante||'').toLowerCase()}|${p.mercado}`),
    ]);

    const newPicks = extracted
      .map(p => {
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
          resultado: '?',
          scoresFinal: null,
        };
      })
      .filter(p => {
        const key = `${p.fecha}|${(p.local||'').toLowerCase()}|${(p.visitante||'').toLowerCase()}|${p.mercado}`;
        return !existingKeys.has(key);
      });

    if (!newPicks.length) {
      console.log('📝 Picks ya registrados hoy — sin duplicados');
      return;
    }

    persistPicks([...existingLocal, ...newPicks]);
    console.log(`📝 ${newPicks.length} picks nuevos guardados`);
    // Solo guardar picks NUEVOS en Airtable (ya no los existentes)
    savePicksToAirtable(newPicks).catch(e => console.warn('Airtable picks:', e.message));
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

  // ── Alerta de gol en vivo ────────────────────────────────────────────────
  if (pick.source === 'alerta_gol' && pick.scoreAtAlert) {
    const totalAtAlert = (pick.scoreAtAlert.home ?? 0) + (pick.scoreAtAlert.away ?? 0);

    // Solo se puede evaluar automáticamente si el pick no es equipo-específico.
    // 'proximo_gol'  → "Equipo X anota el próximo gol": el marcador final no
    //                  dice quién anotó primero → no evaluable.
    // 'gol_equipo_2T' → "Equipo X empata": ídem, requiere secuencia de goles.
    const noEvaluable = ['proximo_gol', 'gol_equipo_2T'].includes(pick.tipoAlerta);
    if (noEvaluable) return '?';

    // over_general / gol_urgente: W si cayó al menos 1 gol después de la alerta.
    return total > totalAtAlert ? 'W' : 'L';
  }

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
    const cornersHome = homeStats(stats)?.['Corner Kicks'] ?? null;
    const cornersAway = awayStats(stats)?.['Corner Kicks'] ?? null;
    if (cornersHome != null && cornersAway != null) {
      const totalCorners = cornersHome + cornersAway;
      if (pick.mercado === 'OVER_CORNERS')  return totalCorners > linea ? 'W' : 'L';
      if (pick.mercado === 'UNDER_CORNERS') return totalCorners < linea ? 'W' : 'L';
    }
  }
  if (stats && (pick.mercado === 'OVER_CARDS' || pick.mercado === 'UNDER_CARDS') && linea != null) {
    const cardsHome = (homeStats(stats)?.['Yellow Cards'] ?? 0) + (homeStats(stats)?.['Red Cards'] ?? 0);
    const cardsAway = (awayStats(stats)?.['Yellow Cards'] ?? 0) + (awayStats(stats)?.['Red Cards'] ?? 0);
    const totalCards = cardsHome + cardsAway;
    if (pick.mercado === 'OVER_CARDS')  return totalCards > linea ? 'W' : 'L';
    if (pick.mercado === 'UNDER_CARDS') return totalCards < linea ? 'W' : 'L';
  }

  // ── Fallback texto: evalúa por contexto del mercado antes de aplicar la fórmula ──
  // Detectar el tipo de pick por palabras clave ANTES de intentar evaluar,
  // para evitar que "Corners Over 9.5" sea evaluado como goles Over 9.5.
  const esCornerPick = /c[oó]rners?|esquinas?/i.test(sel);
  const esTarjetaPick = /tarjetas?|cards?|amarillas?|rojas?\s+card/i.test(sel);
  const es1T = /1er?\s*tiempo|primer\s*tiempo|half\s*time|\bht\b|\b1t\b/i.test(sel);
  const es2T = /2[°º]?\s*tiempo|segundo\s*tiempo|\b2t\b/i.test(sel);

  // 1. Corners — se evalúan primero para no confundirse con goles
  if (esCornerPick && stats) {
    const cornersHome = homeStats(stats)?.['Corner Kicks'] ?? null;
    const cornersAway = awayStats(stats)?.['Corner Kicks'] ?? null;
    if (cornersHome != null && cornersAway != null) {
      const tc = cornersHome + cornersAway;
      const overCornMatch  = sel.match(/(?:over|más de|mas de)\s+(\d+[.,]\d+)\s*(?:c[oó]rners?|esquinas?)?/i) ||
                             sel.match(/(?:c[oó]rners?|esquinas?)\s+(?:over|más de|mas de)\s+(\d+[.,]\d+)/i);
      const underCornMatch = sel.match(/(?:under|menos de|bajo)\s+(\d+[.,]\d+)\s*(?:c[oó]rners?|esquinas?)?/i) ||
                             sel.match(/(?:c[oó]rners?|esquinas?)\s+(?:under|menos de|bajo)\s+(\d+[.,]\d+)/i);
      if (overCornMatch)  { const l = parseFloat(overCornMatch[1].replace(',','.')); return tc > l ? 'W' : 'L'; }
      if (underCornMatch) { const l = parseFloat(underCornMatch[1].replace(',','.')); return tc < l ? 'W' : 'L'; }
    }
    return '?';
  }

  // 2. Tarjetas
  if (esTarjetaPick && stats) {
    const cardsHome = (homeStats(stats)?.['Yellow Cards'] ?? 0) + (homeStats(stats)?.['Red Cards'] ?? 0);
    const cardsAway = (awayStats(stats)?.['Yellow Cards'] ?? 0) + (awayStats(stats)?.['Red Cards'] ?? 0);
    const tc2 = cardsHome + cardsAway;
    if (/al menos\s+1\s+tarjeta|at least\s+1\s+card/i.test(sel)) return tc2 >= 1 ? 'W' : 'L';
    const overCardsMatch  = sel.match(/(?:over|más de|mas de)\s+(\d+[.,]\d+)\s*(?:tarjetas?|cards?|amarillas?)?/i) ||
                            sel.match(/(?:tarjetas?|cards?|amarillas?)\s+(?:over|más de|mas de)\s+(\d+[.,]\d+)/i);
    const underCardsMatch = sel.match(/(?:under|menos de)\s+(\d+[.,]\d+)\s*(?:tarjetas?|cards?|amarillas?)?/i) ||
                            sel.match(/(?:tarjetas?|cards?|amarillas?)\s+(?:under|menos de)\s+(\d+[.,]\d+)/i);
    if (overCardsMatch)  { const l = parseFloat(overCardsMatch[1].replace(',','.')); return tc2 > l ? 'W' : 'L'; }
    if (underCardsMatch) { const l = parseFloat(underCardsMatch[1].replace(',','.')); return tc2 < l ? 'W' : 'L'; }
    return '?';
  }

  // 3. Goles — solo si NO es pick de corners ni tarjetas, y no es 1T/2T
  if (es1T || es2T) return '?';
  const overGoalMatch  = sel.match(/(?:over|más de|mas de)\s+(\d+[.,]\d+)\s*(?:goles?|goals?|ft\b|total)?/i) ||
                         sel.match(/(?:goles?|goals?)\s+(?:over|más de|mas de)\s+(\d+[.,]\d+)/i);
  const underGoalMatch = sel.match(/(?:under|menos de)\s+(\d+[.,]\d+)\s*(?:goles?|goals?|ft\b|total)?/i) ||
                         sel.match(/(?:goles?|goals?)\s+(?:under|menos de)\s+(\d+[.,]\d+)/i);
  if (overGoalMatch)  { const l = parseFloat(overGoalMatch[1].replace(',','.')); return total > l ? 'W' : 'L'; }
  if (underGoalMatch) { const l = parseFloat(underGoalMatch[1].replace(',','.')); return total < l ? 'W' : 'L'; }

  // DNB inferido del texto
  if (/dnb|empate\s+devuelve/i.test(sel)) {
    const esLocal    = /\b(local|home)\b/i.test(sel) || new RegExp(pick.local || 'NADA', 'i').test(sel);
    const esVisitante = /\b(visitante|away)\b/i.test(sel) || new RegExp(pick.visitante || 'NADA', 'i').test(sel);
    if (esLocal) {
      if (goalsHome > goalsAway) return 'W';
      if (goalsHome === goalsAway) return 'V';
      return 'L';
    }
    if (esVisitante) {
      if (goalsAway > goalsHome) return 'W';
      if (goalsHome === goalsAway) return 'V';
      return 'L';
    }
  }

  return '?'; // can't determine automatically
}

/**
 * Guarda los picks de alerta de gol directamente (datos estructurados,
 * sin pasar por Claude extractor). Se llama al final de handleAlertaGol.
 * Evaluación: W si cayó al menos 1 gol después del momento de la alerta.
 */
function saveAlertaGolPicks(alerts) {
  if (!alerts || !alerts.length) return;
  try {
    const existing = loadPicks();
    const today    = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });

    const newPicks = alerts.map(a => ({
      id:           `ag-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      emitidoAt:    new Date().toISOString(),
      fecha:        today,
      source:       'alerta_gol',           // ← distingue de picks_hoy en las stats
      fixtureId:    a.fixtureId || null,
      fechaPartido: new Date().toISOString(),
      liga:         a.liga  || null,
      local:        a.local,
      visitante:    a.visitante,
      mercado:      'OVER_GOALS',           // siempre Over (al menos 1 gol más)
      seleccion:    a.market,               // texto descriptivo del pick
      linea:        a.overLine ?? null,     // línea Over en el momento de la alerta
      scoreAtAlert: { home: a.scoreLocal ?? 0, away: a.scoreVisitante ?? 0 },
      tipoAlerta:   a.tipo   ?? null,   // 'over_general'|'gol_urgente'|'proximo_gol'|'gol_equipo_2T'
      minutoAlerta: a.elapsed ?? null,
      cuota:        a.impliedOdds ?? null,
      stake:        null,
      esCombinada:  false,
      resultado:    null,
      scoresFinal:  null,
    }));

    persistPicks([...existing, ...newPicks]);
    console.log(`⚡ ${newPicks.length} alertas de gol guardadas en picks.json`);
  } catch (e) {
    console.error('saveAlertaGolPicks error:', e.message);
  }
}

async function fetchMatchById(fixtureId) {
  try {
    const { data } = await API.get('/matches/' + fixtureId);
    const raw = Array.isArray(data)           ? data
      : Array.isArray(data?.data)             ? data.data
      : data?.data && typeof data.data === 'object' ? [data.data]
      : [data];
    const m = raw[0] || null;
    if (m) console.log(`🔍 fetchMatchById(${fixtureId}): state="${m.state?.description}" score="${m.state?.score?.current}" home="${m.homeTeam?.name}" away="${m.awayTeam?.name}"`);
    else    console.log(`🔍 fetchMatchById(${fixtureId}): respuesta vacía o nula`);
    return m;
  } catch (e) {
    console.log(`🔍 fetchMatchById(${fixtureId}): error ${e.message}`);
    return null;
  }
}

async function evaluatePendingPicks() {
  // Fuente primaria: Airtable (sobrevive deploys). Fallback: JSON local.
  let picks;
  if (process.env.AIRTABLE_API_KEY) {
    const fromAirtable = await getPicksFromAirtable('total').catch(() => []);
    picks = fromAirtable.length ? fromAirtable : loadPicks();
  } else {
    picks = loadPicks();
  }

  // Re-evaluar también picks recientes (últimos 7 días) con W/L por si fueron mal evaluados.
  // Solo afecta picks dentro de la ventana de corrección para no cambiar resultados históricos.
  const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 7);
  const pending = picks.filter(p => {
    if (!p.fixtureId) return false;
    if (!p.resultado || p.resultado === '?') return true;
    if (['W', 'L', 'V'].includes(p.resultado) && p.emitidoAt && new Date(p.emitidoAt) >= cutoff) return true;
    return false;
  });
  console.log(`📊 evaluatePendingPicks: ${picks.length} total | ${pending.length} a evaluar (pendientes + recientes)`);
  if (!pending.length) return picks;

  const fixtureIds = [...new Set(pending.map(p => p.fixtureId))];
  console.log(`📊 Fixtures a evaluar: ${fixtureIds.join(', ')}`);
  const fixtureMap = {};

  // Usar GET /matches/{id} directo — más confiable que buscar por fecha para historiales
  await Promise.allSettled(fixtureIds.map(async (fid) => {
    try {
      const m = await fetchMatchById(fid);
      if (m && FINISHED_DESCS.has(m.state?.description)) {
        const f = hlToApif(m);
        const stats = await getFixtureStatistics(fid).catch(() => null);
        fixtureMap[fid] = { fixture: f, stats };
        console.log(`✅ Fixture ${fid} terminado: ${f.goals?.home}-${f.goals?.away}`);
      } else if (m) {
        console.log(`⏳ Fixture ${fid} aún no terminado: state="${m.state?.description}"`);
      }
    } catch (e) {
      console.log(`❌ Error fixture ${fid}: ${e.message}`);
    }
  }));

  const actualizados = [];
  for (const pick of picks) {
    if ((pick.resultado && pick.resultado !== '?') || !pick.fixtureId) continue;
    const entry = fixtureMap[pick.fixtureId];
    if (!entry) continue;
    pick.resultado = await evaluatePickResult(pick, entry.fixture, entry.stats);
    pick.scoresFinal = { home: entry.fixture.goals?.home, away: entry.fixture.goals?.away };
    console.log(`📊 Pick evaluado: ${pick.local} vs ${pick.visitante} — ${pick.seleccion} → ${pick.resultado}`);
    actualizados.push(pick);
  }

  if (actualizados.length) {
    persistPicks(picks.filter(p => !p._airtableId)); // solo persiste picks locales
    for (const pick of actualizados) {
      if (pick._airtableId) {
        updatePickResultInAirtable(pick._airtableId, pick.resultado, pick.scoresFinal).catch(() => {});
      } else {
        updatePickInAirtable(pick).catch(() => {}); // fallback picks locales sin ID Airtable
      }
    }
  }
  return picks;
}

async function handleEstadisticas(chatId, period = 'hoy') {
  await bot.sendMessage(chatId, '📊 Evaluando resultados de tus picks...');

  // Siempre evaluar picks pendientes primero (actualiza Airtable con resultados reales)
  await evaluatePendingPicks().catch(e => console.error('evaluatePendingPicks:', e.message));

  // Leer picks actualizados desde Airtable (o JSON local si no hay Airtable)
  let allPicks;
  if (process.env.AIRTABLE_API_KEY) {
    allPicks = await getPicksFromAirtable(period).catch(() => null);
  }
  if (!allPicks || !allPicks.length) {
    allPicks = loadPicks();
  }

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

  // ── Split por fuente ──────────────────────────────────────────────────────
  const picksNormales  = filtered.filter(p => p.source !== 'alerta_gol' && !p.esCombinada);
  const picksAlerta    = filtered.filter(p => p.source === 'alerta_gol');
  const evaluados      = filtered.filter(p => ['W', 'L'].includes(p.resultado));
  const wins           = evaluados.filter(p => p.resultado === 'W').length;
  const losses         = evaluados.filter(p => p.resultado === 'L').length;
  const voids          = filtered.filter(p => p.resultado === 'V').length;
  const pendientes     = filtered.filter(p => p.resultado === null || p.resultado === '?').length;
  const pct            = evaluados.length ? Math.round((wins / evaluados.length) * 100) : null;

  // Breakdown picks normales
  const evNorm   = picksNormales.filter(p => ['W','L'].includes(p.resultado));
  const wNorm    = evNorm.filter(p => p.resultado === 'W').length;
  const pctNorm  = evNorm.length ? Math.round(wNorm / evNorm.length * 100) : null;

  // Breakdown alertas de gol
  const evAlert  = picksAlerta.filter(p => ['W','L'].includes(p.resultado));
  const wAlert   = evAlert.filter(p => p.resultado === 'W').length;
  const pctAlert = evAlert.length ? Math.round(wAlert / evAlert.length * 100) : null;

  let text = `📊 *RENDIMIENTO — ${label}*\n`;
  text += `━━━━━━━━━━━━━━━━━━━\n`;
  text += `✅ Ganados:    ${wins}\n`;
  text += `❌ Perdidos:   ${losses}\n`;
  if (voids)      text += `↩️ Void/Nulos:  ${voids}\n`;
  if (pendientes) text += `⏳ Pendientes: ${pendientes}\n`;
  text += `━━━━━━━━━━━━━━━━━━━\n`;
  if (pct !== null) {
    const icon = pct >= 60 ? '🔥' : pct >= 40 ? '📈' : '📉';
    text += `${icon} *Aciertos global: ${pct}% (${wins}/${evaluados.length})*\n`;
  }
  // Desglose por tipo (solo si hay datos de ambos)
  if (evNorm.length > 0 || evAlert.length > 0) {
    text += `\n*Desglose por tipo:*\n`;
    if (evNorm.length > 0) {
      const iconN = pctNorm >= 60 ? '🔥' : pctNorm >= 40 ? '📈' : '📉';
      text += `${iconN} Picks del día: *${pctNorm}%* (${wNorm}/${evNorm.length})\n`;
    }
    if (evAlert.length > 0) {
      const iconA = pctAlert >= 60 ? '⚡🔥' : pctAlert >= 40 ? '⚡📈' : '⚡📉';
      text += `${iconA} Alertas gol en vivo: *${pctAlert}%* (${wAlert}/${evAlert.length})\n`;
    }
  }
  text += `\n`;

  text += `*Detalle de picks:*\n`;
  for (const p of filtered) {
    const icon = p.resultado === 'W' ? '✅' : p.resultado === 'L' ? '❌' : p.resultado === 'V' ? '↩️' : '⏳';
    const score = p.scoresFinal ? ` *(${p.scoresFinal.home}-${p.scoresFinal.away})*` : '';
    const combo = p.esCombinada ? ' 🔗' : '';
    const srcTag = p.source === 'alerta_gol' ? ' ⚡' : '';
    const minTag = p.minutoAlerta ? ` [min ${p.minutoAlerta}]` : '';
    text += `${icon}${combo}${srcTag} ${p.local} vs ${p.visitante}${score}${minTag}\n`;
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
  // 1. Telegram legacy Markdown: **bold** → *bold*
  text = text.replace(/\*\*(.+?)\*\*/gs, '*$1*');

  // 2. Escapar guiones bajos que no forman pares de _italics_ válidos.
  //    Los nombres de equipos/jugadores con guiones bajos (ej: "Al_Ahly") rompen el parser.
  //    Reemplazamos _ que no estén en pares balanceados con una versión literal.
  //    Estrategia simple: si el total de _ sueltos es impar, quitamos todos los _ del texto
  //    para no arriesgar el parse (mejor sin itálicas que un 400 de Telegram).
  const underscoreMatches = text.match(/_[^_\n]+_/g) || [];
  const pairedUnderscores = underscoreMatches.reduce((acc, m) => acc + (m.match(/_/g) || []).length, 0);
  const totalUnderscores  = (text.match(/_/g) || []).length;
  if (totalUnderscores !== pairedUnderscores) {
    // Hay guiones bajos sueltos — eliminar todos para evitar el error de parse
    text = text.replace(/_/g, '');
  }

  // 3. Escapar asteriscos sueltos (no parte de pares *...*).
  //    Contamos los * fuera de pares balanceados; si el total es impar, hay uno suelto.
  const starMatches = text.match(/\*[^*\n]+\*/g) || [];
  const pairedStars = starMatches.reduce((acc, m) => acc + (m.match(/\*/g) || []).length, 0);
  const totalStars  = (text.match(/\*/g) || []).length;
  if (totalStars !== pairedStars) {
    // Asterisco suelto detectado — convertir TODOS los * de formato a texto plano
    // para no arriesgar el parse (mejor sin bold que un 400 de Telegram).
    text = text.replace(/\*([^*\n]+)\*/g, '$1').replace(/\*/g, '');
  }

  return text;
}

/**
 * Envía texto largo en chunks. Si Telegram rechaza por mal Markdown,
 * reintenta ese chunk en texto plano automáticamente (self-healing).
 */
async function sendLong(chatId, text, options = {}) {
  if (options.parse_mode === 'Markdown') text = normalizeMd(text);

  // Envía un único chunk; si falla por markdown, reintenta sin formato.
  const sendChunk = async (chunk) => {
    try {
      await bot.sendMessage(chatId, chunk, options);
    } catch (e) {
      const isMdErr = e.message?.includes('parse entities') ||
                      e.response?.body?.description?.includes('parse entities');
      if (isMdErr && options.parse_mode) {
        console.warn(`⚠️ Markdown parse error en chunk (${chunk.length} chars) — reintentando en texto plano`);
        const plainOpts = { ...options };
        delete plainOpts.parse_mode;
        await bot.sendMessage(chatId, chunk.replace(/[*_`\[\]]/g, ''), plainOpts);
      } else {
        throw e;
      }
    }
  };

  if (text.length <= TG_LIMIT) return sendChunk(text);

  const paragraphs = text.split(/\n\n+/);
  let chunk = '';
  for (const para of paragraphs) {
    const addition = (chunk ? '\n\n' : '') + para;
    if (chunk.length + addition.length > TG_LIMIT) {
      await sendChunk(chunk);
      chunk = para;
    } else {
      chunk += addition;
    }
  }
  if (chunk) await sendChunk(chunk);
}

function buildMatchContext({ fixture, round, homeStanding, awayStanding, totalTeams }) {
  const ctx = {};
  if (round) ctx.jornada = round;
  const n = totalTeams || 20;
  const hRank = homeStanding?.rank;
  const aRank = awayStanding?.rank;
  if (hRank && aRank) {
    const relZone = n - 2; // bottom 3
    if (hRank >= relZone || aRank >= relZone)       ctx.stakes = 'Zona de descenso';
    else if (hRank <= 2   || aRank <= 2)            ctx.stakes = 'Lucha por el título';
    else if (hRank <= 5   || aRank <= 5)            ctx.stakes = 'Clasificación europea';
  }
  if (homeStanding?.points != null && awayStanding?.points != null) {
    ctx.puntosLocal     = homeStanding.points;
    ctx.puntosVisitante = awayStanding.points;
  }
  return ctx;
}

// ─── Business flows ───────────────────────────────────────────────────────────

async function handlePicksHoy(chatId, forceRefresh = false) {
  const today = todayDate();

  // ── Caché: devuelve picks ya generados hoy sin volver a consultar ─────────
  if (!forceRefresh) {
    const cached = getPicksCache('all');
    if (cached) {
      const generadoCol = new Date(cached.generadoAt).toLocaleTimeString('es-CO', {
        timeZone: 'America/Bogota', hour: '2-digit', minute: '2-digit', hour12: true,
      });
      console.log(`📦 Cache hit — picks del día ya generados a las ${generadoCol} (Col)`);
      await bot.sendMessage(
        chatId,
        `📦 _Análisis generado hoy a las ${generadoCol} (Col). Escribe *"Actualizar picks"* para regenerar con datos frescos._`,
        { parse_mode: 'Markdown' }
      );
      return sendLong(chatId, `📅 *PICKS DEL DÍA — ${today}*\n\n${cached.picksText}`, { parse_mode: 'Markdown' });
    }
  }

  await bot.sendMessage(chatId, '🔍 Consultando nuestra base de datos estadística...');
  const allFixtures = await getFixturesByDate(today);
  // Para picks del día: solo partidos NO iniciados (status NS) y no en ligas excluidas
  const STARTED_STATUSES = new Set(['1H','HT','2H','ET','P','BT','LIVE','INT']);
  const fixtures = allFixtures.filter(f =>
    !PICKS_EXCLUDE_LEAGUES.has(f.leagueId) &&
    !STARTED_STATUSES.has(f.status)
  );

  if (fixtures.length === 0) {
    return bot.sendMessage(chatId, `😔 No hay partidos no iniciados en las ligas monitoreadas hoy (${today}).`);
  }

  // ── FASE 1: cuotas — The Odds API (bulk) + API-Football (fallback) ───────────
  // Ordenar todos los partidos disponibles por prioridad de liga
  const oddsPool = [...fixtures]
    .sort((a, b) => (LEAGUE_PRIORITY[b.leagueId] || 0) - (LEAGUE_PRIORITY[a.leagueId] || 0))
    .slice(0, 80); // revisar hasta 80 partidos buscando cuotas

  await bot.sendMessage(chatId, `📊 ${fixtures.length} partidos en ligas monitoreadas. Consultando cuotas...`);

  // Cuotas desde Highlightly (todas las casas disponibles por partido)
  const oddsMap = await prefetchOddsApi(oddsPool, today);

  const withOdds = oddsPool
    .map(f => ({ fixture: f, odds: oddsMap.get(f.fixtureId) || null }))
    .filter(x => x.odds !== null);

  console.log(`✅ Cuotas Highlightly: ${withOdds.length}/${oddsPool.length} partidos`);

  // Si hay pocos partidos con cuotas (<3), derivar cuotas implícitas desde predicciones de la API
  let selected, oddsPreFetched;
  if (withOdds.length >= 3) {
    selected = withOdds.slice(0, 40).map(x => x.fixture);
    oddsPreFetched = new Map(withOdds.slice(0, 40).map(x => [x.fixture.fixtureId, x.odds]));
  } else {
    // Muy pocas cuotas reales → derivar cuotas implícitas desde % de predicción
    console.log(`⚠️ Pocas cuotas reales (${withOdds.length}). Derivando cuotas implícitas desde predicciones API...`);
    const predPool = oddsPool.slice(0, 30);
    const predStage = await Promise.allSettled(predPool.map(f => getApiPrediction(f.fixtureId)));
    const withImplied = predPool
      .map((f, i) => {
        const pred = predStage[i].status === 'fulfilled' ? predStage[i].value : null;
        if (!pred?.percent_home || !pred?.percent_away || !pred?.percent_draw) return null;
        const margin = 1.07;
        const implied = {
          homeWin: +(margin / (pred.percent_home / 100)).toFixed(2),
          draw:    +(margin / (pred.percent_draw / 100)).toFixed(2),
          awayWin: +(margin / (pred.percent_away / 100)).toFixed(2),
          over25:  pred.goals_home != null && pred.goals_away != null
            ? +(margin / Math.max(0.1, 1 - Math.exp(-(pred.goals_home + pred.goals_away)) * (1 + (pred.goals_home + pred.goals_away)))).toFixed(2)
            : null,
          _source: 'predicted',
        };
        if (implied.homeWin < 1.40 || implied.homeWin > 4.0) return null;
        return { fixture: f, odds: implied };
      })
      .filter(Boolean);

    const combined = [...withOdds, ...withImplied];

    if (combined.length === 0) {
      // Sin cuotas ni predicciones: correr con modelo Poisson puro sobre el pool
      selected = oddsPool.slice(0, 40);
      oddsPreFetched = new Map();
      console.log(`⚠️ Sin cuotas ni predicciones — modelo matemático puro para ${selected.length} partidos`);
    } else {
      selected = combined.slice(0, 40).map(x => x.fixture);
      oddsPreFetched = new Map(combined.slice(0, 40).map(x => [x.fixture.fixtureId, x.odds]));
      console.log(`📊 ${withOdds.length} reales + ${withImplied.length} implícitas = ${combined.length} total`);
    }
  }

  await bot.sendMessage(chatId, `📊 ${withOdds.length} cuotas recopiladas | Analizando ${selected.length} partidos con el motor matemático...`);

  // ── FASE 2: stats de equipo solo para partidos con cuotas ────────────────────
  // Para selecciones nacionales (WC, Nations League, Clasificatorias, etc.):
  // getTeamStats solo devuelve 1-2 partidos del torneo actual → usamos getNationalTeamRecentStats
  // que obtiene los últimos 20 partidos en TODAS las competiciones para promedios reales.
  const NATIONAL_LEAGUES_HOY = new Set([1635, 8443, 4188, 5039, 29718, 14400]);
  const statsPairs = selected.flatMap(f => {
    if (NATIONAL_LEAGUES_HOY.has(f.leagueId)) {
      return [
        getNationalTeamRecentStats(f.homeId, 20),
        getNationalTeamRecentStats(f.awayId, 20),
      ];
    }
    return [
      getTeamStats(f.homeId, f.leagueId),
      getTeamStats(f.awayId, f.leagueId),
    ];
  });
  const statsResults = [];
  for (let i = 0; i < statsPairs.length; i += 6) {
    const batch = await Promise.allSettled(statsPairs.slice(i, i + 6));
    statsResults.push(...batch);
    if (i + 6 < statsPairs.length) await new Promise(r => setTimeout(r, 500));
  }

  // Construir enriched con probabilidades extendidas (HT + corners)
  const enriched = selected.map((f, i) => {
    const hStats = statsResults[i * 2].status === 'fulfilled' ? statsResults[i * 2].value : null;
    const aStats = statsResults[i * 2 + 1].status === 'fulfilled' ? statsResults[i * 2 + 1].value : null;

    // ── Lambdas base (promedios de temporada) ──
    const hFor  = parseFloat(hStats?.golesAnotadosHome) || 1.3;
    const hAgt  = parseFloat(hStats?.golesRecibidosHome) || 1.1;
    const aFor  = parseFloat(aStats?.golesAnotadosAway) || 1.0;
    const aAgt  = parseFloat(aStats?.golesRecibidosAway) || 1.3;

    // ── Ajuste por forma reciente (blend 50% temporada + 50% forma ponderada) ──
    // El multiplicador de forma del equipo ajusta su ataque;
    // el del rival ajusta los goles que recibe el propio equipo.
    const hFormFact = formMultiplier(hStats?.forma5);
    const aFormFact = formMultiplier(aStats?.forma5);
    const blend = 0.50; // peso de la forma reciente (vs. media de temporada)
    const hForAdj = hFor * (1 - blend + blend * hFormFact);   // ataque local según forma
    const aForAdj = aFor * (1 - blend + blend * aFormFact);   // ataque visitante según forma
    const hAgtAdj = hAgt * (1 - blend + blend * aFormFact);   // defensa local = cuánto hace el ataque visitante
    const aAgtAdj = aAgt * (1 - blend + blend * hFormFact);   // defensa visit = cuánto hace el ataque local
    const extProbs = calcExtendedProbs(hForAdj, hAgtAdj, aForAdj, aAgtAdj, f.leagueName || '');

    return {
      fixtureId:      f.fixtureId,
      homeId:         f.homeId,    // ← necesario para matching de lesionados
      awayId:         f.awayId,    // ← necesario para matching de lesionados
      leagueId:       f.leagueId,  // ← necesario para tier multiplier en buildPickCandidates
      liga:           f.leagueName,
      country:        f.country,
      local:          f.homeTeam,
      visitante:      f.awayTeam,
      hora:           formatHour(f.date),
      fechaPartido:   f.date,
      statsLocal:     withHomeContext(hStats),
      statsVisitante: withAwayContext(aStats),
      _extendedProbs: extProbs,
      _statsSource:   (hStats && aStats) ? 'real' : hStats ? 'local_only' : aStats ? 'away_only' : 'fallback',
      _formFactors:   { hFormFact, aFormFact }, // útil para debug
    };
  });

  // ── FASE 3: H2H, standings, predicciones y contexto ─────────────────────────
  await bot.sendMessage(chatId, `🔢 Consultando H2H, tabla de posiciones y contexto histórico...`);
  const uniqueLeagueIds = [...new Set(selected.map(f => f.leagueId))];

  // Standings en lotes de 5 para evitar rate limit (20+ llamadas simultáneas causan 429)
  async function fetchStandingsThrottled(leagueIds) {
    const results = [];
    const BATCH = 5;
    for (let i = 0; i < leagueIds.length; i += BATCH) {
      const batch = leagueIds.slice(i, i + BATCH);
      const batchRes = await Promise.allSettled(batch.map(lid => getLeagueStandings(lid)));
      results.push(...batchRes);
      if (i + BATCH < leagueIds.length) await new Promise(r => setTimeout(r, 200));
    }
    return results;
  }

  const [h2hResults, standingsArray, predResults] = await Promise.all([
    Promise.allSettled(selected.map(f => getH2H(f.homeId, f.awayId))),
    fetchStandingsThrottled(uniqueLeagueIds),
    // Predicciones solo para partidos con cuotas (máx 40, priorizados por tier)
    Promise.allSettled(selected.map(f => getApiPrediction(f.fixtureId))),
  ]);

  // Construir mapa de standings por leagueId
  const standingsMap = {};
  const standingsTotalMap = {};
  uniqueLeagueIds.forEach((lid, i) => {
    if (standingsArray[i].status === 'fulfilled') {
      standingsMap[lid] = standingsArray[i].value.teams || standingsArray[i].value;
      standingsTotalMap[lid] = standingsArray[i].value.total || (standingsArray[i].value.teams || standingsArray[i].value).length;
    }
  });

  // Enriquecer con cuotas (ya prefetched), H2H, posición, contexto
  let conOdds = 0;
  for (let i = 0; i < enriched.length; i++) {
    const f = selected[i];
    const odds = oddsPreFetched.get(f.fixtureId) || null;
    if (odds) { enriched[i].cuotasReales = odds; conOdds++; }

    const h2h = h2hResults[i].status === 'fulfilled' ? h2hResults[i].value : [];
    if (h2h && h2h.length > 0) enriched[i].h2h = h2h.slice(0, 5);

    enriched[i].jornada = f.round;
    enriched[i].estadio = f.venue  || null;
    enriched[i].ciudad  = f.city   || null;

    const standings    = standingsMap[f.leagueId] || [];
    const totalEquipos = standingsTotalMap[f.leagueId] || 20;
    const standingLocal = standings.find(s => s.teamId === f.homeId);
    const standingVisit = standings.find(s => s.teamId === f.awayId);
    if (standingLocal) enriched[i].posicionLocal    = standingLocal.rank;
    if (standingVisit) enriched[i].posicionVisitante = standingVisit.rank;
    enriched[i].motivacionLocal     = getTeamMotivation(standingLocal, totalEquipos);
    enriched[i].motivacionVisitante = getTeamMotivation(standingVisit, totalEquipos);
    enriched[i].arbitro = f.referee || null;

    // ── Predicción + venue/árbitro desde /matches/{id} ────────────────────────
    const pred = predResults[i].status === 'fulfilled' ? predResults[i].value : null;
    if (pred) {
      if (pred._venue)   enriched[i].estadio = pred._venue;
      if (pred._city)    enriched[i].ciudad  = pred._city;
      if (pred._referee) enriched[i].arbitro = pred._referee;
      enriched[i].prediccionAPI = pred;
      // Si la API da xG esperados, recalcular probs blending 40% API + 60% nuestro Poisson
      if (pred.goals_home != null && pred.goals_away != null) {
        const blendFactor = 0.40;
        const currentProbs = enriched[i]._extendedProbs;
        // El lambda actual viene de homeLambda/awayLambda en extendedProbs
        const apiHomeLambda = pred.goals_home;
        const apiAwayLambda = pred.goals_away;
        const blendedHomeLambda = currentProbs.homeLambda * (1 - blendFactor) + apiHomeLambda * blendFactor;
        const blendedAwayLambda = currentProbs.awayLambda * (1 - blendFactor) + apiAwayLambda * blendFactor;
        // Recalcular probs extendidas con lambdas más precisos
        const blendedProbs = calcExtendedProbs(
          blendedHomeLambda, currentProbs.homeLambda_agt ?? blendedHomeLambda,
          blendedAwayLambda, currentProbs.awayLambda_agt ?? blendedAwayLambda,
          enriched[i].liga || '',
        );
        enriched[i]._extendedProbs = blendedProbs;
        enriched[i]._lambdaSource = 'blend_api_poisson';
      }
    }

    // ── Contexto rico del partido (ronda, lo que se juega, urgencia) ──────────
    const matchCtx = buildMatchContext({
      fixture:       f,
      round:         f.round,
      homeStanding:  standingLocal,
      awayStanding:  standingVisit,
      totalTeams:    totalEquipos,
      leagueId:      f.leagueId,
      homeLastMatch: null,  // días de descanso se calculan post-selección (solo 3 picks)
      awayLastMatch: null,
    });
    if (Object.keys(matchCtx).length > 0) enriched[i].contextoPartido = matchCtx;
  }

  // ── Estadísticas del árbitro (WorldReferee.com) ──────────────────────────────
  {
    const refNames = enriched.map(e => e.arbitro || null);
    const uniqueRefs = [...new Set(refNames.filter(Boolean))];
    if (uniqueRefs.length > 0) {
      const refResults = await Promise.allSettled(uniqueRefs.map(n => getRefereeCardStats(n)));
      const refMap = {};
      uniqueRefs.forEach((n, i) => {
        if (refResults[i].status === 'fulfilled' && refResults[i].value) refMap[n] = refResults[i].value;
      });
      for (const e of enriched) {
        if (e.arbitro && refMap[e.arbitro]) e.arbitroStats = refMap[e.arbitro];
      }
    }
  }

  // ── Soccer Buddy / ZCode signals ────────────────────────────────────────────
  // Si el scraper tiene datos, los agrega como señal externa. No bloquea si no hay.
  if (_zbStore.size > 0) {
    let zbHits = 0;
    for (const e of enriched) {
      const zb = getZbSignals(e.local, e.visitante);
      if (zb) {
        e.soccerBuddy = {
          _nota: 'Predicciones del modelo ZCode Soccer Buddy (simulación Monte Carlo). Úsalas como señal de confirmación externa.',
          scorePred:     zb.scorePred   || null,
          htScorePred:   zb.htScorePred || null,
          likelyResult:  zb.likelyResult || null,
          inValueBets:   zb.inValueBets  || false,
          btts_pct:      zb.btts         ?? null,
          over15_pct:    zb.over15       ?? null,
          over25_pct:    zb.over25       ?? null,
          over35_pct:    zb.over35       ?? null,
          draw_pct:      zb.draw         ?? null,
          home_win_pct:  zb.home_win     ?? null,
          away_win_pct:  zb.away_win     ?? null,
          ht_over05_pct: zb.ht_over05    ?? null,
          ht_over15_pct: zb.ht_over15    ?? null,
          sh_over05_pct: zb.sh_over05    ?? null,
          sh_over15_pct: zb.sh_over15    ?? null,
        };
        // Eliminar nulls para no inflar el JSON
        Object.keys(e.soccerBuddy).forEach(k => {
          if (k !== '_nota' && e.soccerBuddy[k] === null) delete e.soccerBuddy[k];
        });
        zbHits++;
      }
    }
    console.log(`📡 Soccer Buddy: ${zbHits}/${enriched.length} partidos con señales ZCode`);
  }

  // ── Dropping Odds + Line Reversals ──────────────────────────────────────────
  if (_zdoStore.size > 0 || _zlrStore.size > 0) {
    let mktHits = 0;
    for (const e of enriched) {
      const mkt = getZcodeMarketSignals(e.local, e.visitante);
      if (mkt) { e.mercadoZCode = mkt; mktHits++; }
    }
    if (mktHits > 0) console.log(`📉 Dropping Odds/Line Reversals: ${mktHits} partidos con señales de mercado`);
  }

  // ── Totals Predictor + Power Rankings + Soccer Oscillator + Contrarian Bets ─
  if (_tpStore.size > 0 || _prStore.size > 0 || _soStore.size > 0 || _cbStore.size > 0) {
    let zetHits = 0;
    for (const e of enriched) {
      const extra = getZcodeExtraSignals(e.local, e.visitante);
      if (extra) { e.zcodeExtra = extra; zetHits++; }
    }
    if (zetHits > 0) console.log(`🎯 ZCode Extra (TP/PR/SO/CB): ${zetHits} partidos con señales adicionales`);
  }

  await bot.sendMessage(chatId, `🧮 Motor matemático calculando EV por mercado...`);

  // ── Filtro de calidad ANTES del motor ────────────────────────────────────
  // REGLA: necesitamos datos de AMBOS equipos. Sin datos de uno de los dos,
  // el modelo Poisson usa defaults genéricos → pick inventado → DESCARTADO.
  const enrichedFiltrado = enriched.filter(e => {
    if (e._statsSource === 'fallback') {
      console.log(`❌ DESCARTADO (sin stats de ningún equipo): ${e.local} vs ${e.visitante}`);
      return false;
    }
    if (e._statsSource === 'local_only') {
      console.log(`❌ DESCARTADO (sin stats del local — Poisson inválido): ${e.local} vs ${e.visitante}`);
      return false;
    }
    if (e._statsSource === 'away_only') {
      console.log(`❌ DESCARTADO (sin stats del visitante — Poisson inválido): ${e.local} vs ${e.visitante}`);
      return false;
    }
    return true;
  });

  // ── Debug logging detallado ───────────────────────────────────────────────
  console.log(`📊 DEBUG picks motor:`);
  console.log(`   Partidos totales: ${enriched.length} | Tras filtro calidad: ${enrichedFiltrado.length}`);
  console.log(`   Descartados por sin stats: ${enriched.length - enrichedFiltrado.length}`);
  console.log(`   Con cuotas reales: ${conOdds}/${enrichedFiltrado.length}`);
  const sinStats = enrichedFiltrado.filter(e => e._statsSource !== 'real').length;
  console.log(`   Stats parciales (un equipo): ${sinStats} — stake máximo 5`);

  // ── Selección matemática de picks ────────────────────────────────────────
  const candidates = buildPickCandidates(enrichedFiltrado);
  const topPicks   = selectDiversePicks(candidates, 3);

  // ── Lesionados/sancionados para los picks seleccionados ────────────────────
  // Solo consultamos los 3 fixtures finales → máximo 3 llamadas API extra.
  if (topPicks.length > 0) {
    try {
      const injResults = await Promise.allSettled(
        topPicks.map(pick => getFixtureInjuries(pick.fixtureId))
      );
      injResults.forEach((res, idx) => {
        if (res.status !== 'fulfilled') return;
        const allInjuries = res.value;  // [{nombre, equipoId, equipo, tipo, razon}]
        const pick   = topPicks[idx];
        const match  = enriched.find(e => e.fixtureId === pick.fixtureId);
        if (!match || !allInjuries.length) return;

        pick.lesionadosLocal     = allInjuries.filter(i => i.equipoId === match.homeId);
        pick.lesionadosVisitante = allInjuries.filter(i => i.equipoId === match.awayId);

        // Reducir stake si algún equipo tiene ≥2 bajas confirmadas
        const bajasLocal = pick.lesionadosLocal.length;
        const bajasVisit = pick.lesionadosVisitante.length;
        if (bajasLocal >= 2 || bajasVisit >= 2) {
          const stakeAntes = pick.stake;
          pick.stake = Math.max(5, pick.stake - 1);
          console.log(`🏥 Stake ${stakeAntes}→${pick.stake} por bajas: ${pick.local}(${bajasLocal}) vs ${pick.visitante}(${bajasVisit})`);
        }

        const totalBajas = bajasLocal + bajasVisit;
        if (totalBajas > 0) {
          console.log(`🩹 ${pick.local} vs ${pick.visitante}: ${bajasLocal} bajas local, ${bajasVisit} bajas visitante`);
          pick.lesionadosLocal.forEach(p => console.log(`   🔴 [LOCAL] ${p.nombre} — ${p.tipo}${p.razon ? ': '+p.razon : ''}`));
          pick.lesionadosVisitante.forEach(p => console.log(`   🔴 [VISIT] ${p.nombre} — ${p.tipo}${p.razon ? ': '+p.razon : ''}`));
        }
      });
    } catch (injErr) {
      console.warn('⚠️ Error obteniendo lesionados:', injErr.message);
    }
  }

  // ── Días de descanso para los topPicks (solo 3-6 llamadas extra) ──────────────
  if (topPicks.length > 0) {
    try {
      const restResults = await Promise.allSettled(
        topPicks.flatMap(pick => {
          const match = enriched.find(e => e.fixtureId === pick.fixtureId);
          if (!match) return [Promise.resolve(null), Promise.resolve(null)];
          return [getLastMatchDate(match.homeId), getLastMatchDate(match.awayId)];
        })
      );
      topPicks.forEach((pick, idx) => {
        const homeLastRaw = restResults[idx * 2].status === 'fulfilled' ? restResults[idx * 2].value : null;
        const awayLastRaw = restResults[idx * 2 + 1].status === 'fulfilled' ? restResults[idx * 2 + 1].value : null;
        const today = new Date();
        if (homeLastRaw) {
          const days = Math.round((today - homeLastRaw) / 86400000);
          pick.diasDescansoLocal = days;
          if (days <= 3) {
            pick._alertaCansancioLocal = `⚠️ LOCAL con solo ${days} días de descanso — posible rotación`;
            if (pick.stake > 6) pick.stake -= 1;  // reducir stake si cansancio alto
          }
        }
        if (awayLastRaw) {
          const days = Math.round((today - awayLastRaw) / 86400000);
          pick.diasDescansoVisitante = days;
          if (days <= 3) {
            pick._alertaCansancioVisitante = `⚠️ VISITANTE con solo ${days} días de descanso — posible rotación`;
            if (pick.stake > 6) pick.stake -= 1;
          }
        }
      });
    } catch (restErr) {
      console.warn('⚠️ Error calculando días de descanso:', restErr.message);
    }
  }

  // Log detallado de por qué cada partido pasó o no
  console.log(`🎯 Candidatos válidos: ${candidates.length}`);
  if (candidates.length > 0) {
    candidates.slice(0, 5).forEach(c =>
      console.log(`   ✅ ${c.local} vs ${c.visitante} | ${c.marketLabel} @ ${c.odds} | EV: ${c.ev}% | prob: ${c.prob}%`)
    );
  }
  enriched.forEach(e => {
    const p = e._extendedProbs;
    const o = e.cuotasReales || {};
    if (!o.over25 && !o.bttsYes && !o.cornersOver85) {
      console.log(`   ❌ ${e.local} vs ${e.visitante} — sin cuotas disponibles`);
    } else {
      const hasCand = candidates.some(c => c.fixtureId === e.fixtureId);
      if (!hasCand) console.log(`   ⚠️ ${e.local} vs ${e.visitante} — cuotas OK pero ningún pick superó los umbrales (over25=${o.over25} prob=${p?.over25}%)`);
    }
  });
  console.log(`   Picks seleccionados: ${topPicks.length}`);

  // ── ZCode: ajusta stakes, NUNCA bloquea picks ────────────────────────────────
  // ZCode es señal complementaria. El motor Poisson es la fuente principal.
  // Con cuenta free tier, los datos pueden ser parciales → no pueden silenciar picks.
  //
  // Lógica:
  //   Contradicción fuerte  (≥75% opuesto): stake -2 (mínimo 5)
  //   Contradicción moderada(≥62% opuesto): stake -1
  //   Confirmación fuerte   (≥72% mismo):   stake +1 (máximo 10)
  if (_zbStore.size > 0) {
    for (const pick of topPicks) {
      const zb = getZbSignals(pick.local, pick.visitante);
      if (!zb) continue;
      const m = pick.market;
      let delta = 0;

      // Under 2.5: ZCode dice Over fuerte → baja stake
      if (m === 'under25' && zb.over25_pct !== null) {
        if      (zb.over25_pct >= 75) delta = -2;
        else if (zb.over25_pct >= 62) delta = -1;
      }
      // Over 2.5: ZCode confirma → sube; dice Under → baja
      if (m === 'over25' && zb.over25_pct !== null) {
        if      (zb.over25_pct >= 72) delta = +1;
        else if (zb.over25_pct <= 25) delta = -2;
        else if (zb.over25_pct <= 38) delta = -1;
      }
      // BTTS Sí: ZCode confirma → sube; dice No → baja
      if (m === 'btts' && zb.btts_pct !== null) {
        if      (zb.btts_pct >= 72) delta = +1;
        else if (zb.btts_pct <= 28) delta = -2;
        else if (zb.btts_pct <= 38) delta = -1;
      }
      // BTTS No: ZCode dice Sí fuerte → baja
      if (m === 'bttsNo' && zb.btts_pct !== null) {
        if      (zb.btts_pct >= 75) delta = -2;
        else if (zb.btts_pct >= 62) delta = -1;
      }

      if (delta !== 0) {
        const antes = pick.stake;
        pick.stake = Math.min(10, Math.max(5, pick.stake + delta));
        const dir = delta > 0 ? '✅ confirma' : '⚠️ contradice';
        console.log(`🔮 ZCode ${dir}: ${pick.local} vs ${pick.visitante} [${m}] stake ${antes}→${pick.stake} (delta ${delta>0?'+':''}${delta})`);
      }
    }
  }

  const topPicksFinal = topPicks; // ZCode solo ajusta stakes, nunca filtra

  let picksText;

  if (topPicksFinal.length >= 1) {
    // ── Motor matemático: LLM solo formatea los picks ya seleccionados por Poisson+EV
    picksText = await sonnet(
      PICKS_HOY_FORMATTER_SYSTEM,
      `Fecha: ${today} (hora Colombia)\n\nPICKS SELECCIONADOS POR EL MOTOR MATEMÁTICO — NO añadas ni elimines ninguno:\n\n${JSON.stringify(topPicksFinal, null, 2)}`
    );
  } else {
    // ── Motor no encontró picks con EV validado → sin picks
    const sinCuotas = conOdds === 0;
    console.log(`📡 Motor: 0 picks válidos (sinCuotas=${sinCuotas}, partidos=${enriched.length}) — sin picks hoy`);

    if (sinCuotas) {
      picksText = `⏳ *El análisis no encontró picks de valor suficiente hoy.*\n\nEsto puede ocurrir cuando los partidos del día aún no tienen información completa disponible. Prueba en unas horas o pide el análisis de un partido específico: _"analiza Real Madrid vs Barcelona"_`;
    } else {
      // Hay cuotas pero ningún mercado superó los umbrales de EV — honestidad total
      picksText = `🔍 *El motor analizó ${enriched.length} partidos con cuotas reales y no encontró valor matemático suficiente hoy.*\n\nEsto significa que las casas tienen precios correctos o ligeramente favorables para ellas en todos los mercados disponibles. Mejor no apostar que apostar sin ventaja real.\n\n💡 Puedes pedir el análisis de un partido específico: _"analiza [Equipo A] vs [Equipo B]"_\n\n📲 O intenta de nuevo más tarde si los partidos del día aún no tienen cuotas finales.`;
    }
    // Caché corto cuando no hay picks del motor (30 min) para reintentar pronto
    setPicksCache('all', picksText, []);
    try {
      await sendLong(chatId, `📅 *PICKS DEL DÍA — ${today}*\n\n${picksText}`, { parse_mode: 'Markdown' });
    } catch {
      await sendLong(chatId, `📅 PICKS DEL DÍA — ${today}\n\n${picksText.replace(/[*_`]/g, '')}`);
    }
    return;
  }

  // Validador mínimo: solo rechaza si la cuota es absurda (>3.50 o <1.30)
  const cuotasEnTexto = [...picksText.matchAll(/[Cc]uota\s+m[íi]nima[:\s*]+(\d+[\.,]\d+)/g)].map(m => parseFloat(m[1].replace(',', '.')))
  const cuotaAbsurda = cuotasEnTexto.some(c => c > 3.50 || c < 1.30)
  if (cuotaAbsurda) {
    console.warn(`⚠️ VALIDADOR: cuotas absurdas detectadas [${cuotasEnTexto}] — regenerando`)
    picksText = '⛔ Sin picks de valor real hoy.'
  }

  // Gate pre-publicación: valida y corrige stakes antes de enviar
  const matchesCtxForGate = enriched.map(f => ({ fixtureId: f.fixtureId, local: f.local, visitante: f.visitante, liga: f.liga }));
  picksText = await applyStakeGate(picksText, enriched, matchesCtxForGate);

  // Guardar en caché
  setPicksCache('all', picksText, enriched.map(f => f.fixtureId));
  try {
    await sendLong(chatId, `📅 *PICKS DEL DÍA — ${today}*\n\n${picksText}`, { parse_mode: 'Markdown' });
  } catch {
    await sendLong(chatId, `📅 PICKS DEL DÍA — ${today}\n\n${picksText.replace(/[*_`]/g, '')}`);
  }
  recordPicks(picksText, enriched.map(f => ({ fixtureId: f.fixtureId, local: f.local, visitante: f.visitante, liga: f.liga, fechaPartido: f.fechaPartido }))).catch(e => console.error('recordPicks:', e.message));
}

// ─── Sistema del Día (admin-only) ────────────────────────────────────────────
// Escanea TODOS los partidos de hoy, calcula EV por pick y arma la apuesta de sistema
// óptima (2/3, 3/4, 3/5 etc.) basada en probabilidades Poisson + cuotas reales.
// No aparece en el menú principal — solo accesible con la keyword "sistema hoy".

const SISTEMA_HOY_SYSTEM = `Selecciona los mejores picks y devuelve ÚNICAMENTE el bloque de texto final. Cero explicaciones, cero razonamiento, cero partidos descartados.

CRITERIOS DE SELECCIÓN (aplica internamente, no escribas nada de esto):
- Máximo 1 pick por partido
- Usa solo cuotas del campo cuotasReales. Si cuotasReales=null o no tiene ese mercado → ignora ese partido
- Prob mínima: 52% (desde modelo Poisson o H2H si hay ≥6 registros)
- Cuota mínima: 1.70
- EV = prob × cuota - 1 debe ser > 0
- Toma los 3-5 mejores por EV

SEGÚN CANTIDAD DE PICKS:
- 5 picks → Sistema 3/5 (10 triples)
- 4 picks → Sistema 3/4 (4 triples)
- 3 picks → Sistema 2/3 (3 dobles)
- 2 picks → Doble directo (menciona que faltó 1 para sistema completo)
- 1 pick → Pick individual recomendado

RESPONDE EXACTAMENTE ASÍ, sin añadir nada antes ni después:

━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 *SISTEMA DEL DÍA — [FECHA]*
━━━━━━━━━━━━━━━━━━━━━━━━━━

*1.* [Local] vs [Visitante] — [Liga]
🕐 [Hora] | *[Mercado]* @ *[cuota real]*
Prob: [X]% | EV: [+X.X]%

*2.* [Local] vs [Visitante] — [Liga]
🕐 [Hora] | *[Mercado]* @ *[cuota real]*
Prob: [X]% | EV: [+X.X]%

[...hasta 5 picks]

━━━━━━━━━━━━━━━━━━━━━━━━━━
🔢 *[TIPO DE SISTEMA]*

Pick1 + Pick2: [cuota1] x [cuota2] = *[total]*
[una línea por combinación]

━━━━━━━━━━━━━━━━━━━━━━━━━━
💰 Stake: *[X]%* bankroll por combinación
📈 Si gana todo: *[X]x* retorno
━━━━━━━━━━━━━━━━━━━━━━━━━━`;

async function handleSistemaHoy(chatId) {
  const today = todayDate();
  await bot.sendMessage(chatId, '🔬 *Sistema del Día* — Escaneando todos los partidos disponibles...', { parse_mode: 'Markdown' });

  // 1. Obtener TODOS los fixtures del día (sin filtro de ligas excluidas)
  const allFixtures = await getFixturesByDate(today);
  if (allFixtures.length === 0) {
    return bot.sendMessage(chatId, `😔 No hay partidos hoy (${today}) en las ligas monitoreadas.`);
  }

  // 2. Ordenar por prioridad de liga y tomar top 20 para analizar
  const candidates = [...allFixtures]
    .sort((a, b) => (LEAGUE_PRIORITY[b.leagueId] || 0) - (LEAGUE_PRIORITY[a.leagueId] || 0))
    .slice(0, 20);

  await bot.sendMessage(chatId, `📊 ${allFixtures.length} partidos encontrados. Analizando top ${candidates.length} con estadísticas + cuotas...`);

  // 3. Traer estadísticas en batches
  const statsPairs = candidates.flatMap(f => [
    getTeamStats(f.homeId, f.leagueId),
    getTeamStats(f.awayId, f.leagueId),
  ]);
  const statsResults = [];
  for (let i = 0; i < statsPairs.length; i += 4) {
    const batch = await Promise.allSettled(statsPairs.slice(i, i + 4));
    statsResults.push(...batch);
    if (i + 4 < statsPairs.length) await new Promise(r => setTimeout(r, 1000));
  }

  // 4. Enriquecer con Poisson + cuotas reales + H2H para partidos sin stats
  const h2hResults = await Promise.allSettled(
    candidates.map(f => getH2H(f.homeId, f.awayId))
  );

  const enriched = candidates.map((f, i) => {
    const homeStats = statsResults[i * 2].status === 'fulfilled' ? statsResults[i * 2].value : null;
    const awayStats = statsResults[i * 2 + 1].status === 'fulfilled' ? statsResults[i * 2 + 1].value : null;
    const h2h = h2hResults[i].status === 'fulfilled' ? h2hResults[i].value : [];
    const probBlock = buildProbBlock(homeStats, awayStats, h2h, f.leagueId);
    return {
      fixtureId:    f.fixtureId,
      liga:         f.leagueName,
      local:        f.homeTeam,
      visitante:    f.awayTeam,
      hora:         formatHour(f.date),
      statsLocal:   withHomeContext(homeStats),
      statsVisitante: withAwayContext(awayStats),
      h2h:          h2h.slice(0, 8), // últimos 8 H2H
      ...(probBlock && { probabilidadesCalculadas: probBlock }),
    };
  });

  // 5. Cuotas reales para calcular EV real
  await bot.sendMessage(chatId, '📈 Consultando cuotas pre-partido...');
  const oddsResults = await Promise.allSettled(candidates.map(f => getRealOdds(f.fixtureId)));
  for (let i = 0; i < enriched.length; i++) {
    const odds = oddsResults[i].status === 'fulfilled' ? oddsResults[i].value : null;
    if (odds) enriched[i].cuotasReales = odds;
  }

  await bot.sendMessage(chatId, '🧮 Calculando sistema óptimo...');

  const sistemaText = await sonnet(
    SISTEMA_HOY_SYSTEM,
    `Fecha: ${today}. DATOS de partidos disponibles. Recuerda: usa SOLO cuotas del campo cuotasReales (no inventes), máximo 1 pick por partido, cuota mínima 1.70.\n\nDATA:\n${JSON.stringify(enriched, null, 2)}`
  );

  // Enviar con fallback a texto plano si Markdown falla
  try {
    await sendLong(chatId, sistemaText, { parse_mode: 'Markdown' });
  } catch (e) {
    console.error('Sistema sendLong Markdown error, retrying plain:', e.message);
    await sendLong(chatId, sistemaText.replace(/[*_`]/g, ''));
  }
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
  const SKIP_STATUSES = new Set(['1H','HT','2H','ET','P','BT','LIVE','INT','FT','AET','PEN','AWD','WO']);

  let fixtures = [];
  {
    const allF = await getFixturesByDate(today);
    fixtures = allF.filter(f => {
      if (SKIP_STATUSES.has(f.status)) return false;
      if (leagueId) return f.leagueId === leagueId;
      return (f.leagueName || '').toLowerCase().includes(leagueName.toLowerCase());
    });
    console.log(`🔎 ${displayName}: ${fixtures.length} fixtures encontrados`);
  }

  console.log(`📊 Partidos encontrados para ${displayName}: ${fixtures.length}`);

  if (fixtures.length === 0) {
    return bot.sendMessage(chatId, `😔 No hay partidos de *${displayName}* programados para hoy.`, { parse_mode: 'Markdown' });
  }

  await bot.sendMessage(chatId, `📊 ${fixtures.length} partido(s) de *${displayName}* encontrados. Recopilando estadísticas...`, { parse_mode: 'Markdown' });

  // Fetch team stats en batches de 4
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

  // Enriquecer con probabilidades extendidas
  const enriched = fixtures.map((f, i) => {
    const hStats = statsResults[i * 2]?.status === 'fulfilled' ? statsResults[i * 2].value : null;
    const aStats = statsResults[i * 2 + 1]?.status === 'fulfilled' ? statsResults[i * 2 + 1].value : null;
    const hFor   = parseFloat(hStats?.golesAnotadosHome) || 0;
    const hAgt   = parseFloat(hStats?.golesRecibidosHome) || 0;
    const aFor   = parseFloat(aStats?.golesAnotadosAway) || 0;
    const aAgt   = parseFloat(aStats?.golesRecibidosAway) || 0;
    const extProbs = (hFor > 0 || aFor > 0) ? calcExtendedProbs(hFor, hAgt, aFor, aAgt, f.leagueName || '') : null;
    return {
      fixtureId:      f.fixtureId,
      liga:           f.leagueName,
      country:        f.country,
      local:          f.homeTeam,
      visitante:      f.awayTeam,
      hora:           formatHour(f.date),
      fechaPartido:   f.date,
      statsLocal:     withHomeContext(hStats),
      statsVisitante: withAwayContext(aStats),
      _extendedProbs: extProbs,
    };
  });

  // Cuotas reales desde Highlightly
  await bot.sendMessage(chatId, `📈 Consultando cuotas...`);
  const oddsMapL = await prefetchOddsApi(fixtures, today);
  for (const e of enriched) {
    const odds = oddsMapL.get(e.fixtureId) || null;
    if (odds) e.cuotasReales = odds;
  }

  await bot.sendMessage(chatId, `🧮 Motor matemático calculando EV...`);

  // Filtro de calidad: descartar partidos sin datos reales de ningún equipo
  const enrichedFiltradoL = enriched.map(e => ({
    ...e,
    _statsSource: (e.statsLocal && e.statsVisitante) ? 'real'
      : e.statsLocal ? 'local_only'
      : e.statsVisitante ? 'away_only'
      : 'fallback',
  })).filter(e => {
    if (e._statsSource === 'fallback') {
      console.log(`❌ Liga: DESCARTADO sin stats: ${e.local} vs ${e.visitante}`);
      return false;
    }
    if (e._statsSource === 'local_only' || e._statsSource === 'away_only') e._maxStake = 5;
    return true;
  });

  // Selección matemática de picks
  const candidatesL = buildPickCandidates(enrichedFiltradoL);
  const topPicksL   = selectDiversePicks(candidatesL, 3);
  console.log(`🎯 ${displayName} — candidatos EV+: ${candidatesL.length} | seleccionados: ${topPicksL.length}`);

  let picksText;
  if (topPicksL.length >= 2) {
    picksText = await sonnet(
      PICKS_HOY_FORMATTER_SYSTEM,
      `Fecha: ${today} | Liga: ${displayName}\n\nPICKS SELECCIONADOS — NO añadas ni elimines:\n\n${JSON.stringify(topPicksL, null, 2)}`
    );
  } else {
    picksText = await sonnet(
      TIPSTER_SYSTEM,
      `Partidos de ${displayName} del día ${today}. DATOS REALES:\n\n${JSON.stringify(enriched.map(e => ({ ...e, _extendedProbs: undefined })), null, 2)}\n\nEmite picks de valor para esta liga. REGLAS IRROMPIBLES: (1) CUOTA MÍNIMA ABSOLUTA 1.65 — cualquier pick con cuota menor se DESCARTA, (2) PROHIBIDO DNB, (3) PROHIBIDO Over 1.5 FT, (4) PROHIBIDO mostrar EV%, probabilidades internas o razones de exclusión. Si no hay picks con valor real, responde SOLO: ⛔ Sin picks de valor real hoy.`
    );
  }

  // Gate pre-publicación: valida y corrige stakes antes de enviar
  const matchesCtxLigaGate = enriched.map(f => ({ fixtureId: f.fixtureId, local: f.local, visitante: f.visitante, liga: f.liga }));
  picksText = await applyStakeGate(picksText, enriched, matchesCtxLigaGate);

  // Guardar en caché
  setPicksCache(cacheScope, picksText, enriched.map(f => f.fixtureId));
  await sendLong(chatId, `📅 *${displayName} — ${today}*\n\n${picksText}`, { parse_mode: 'Markdown' });
  recordPicks(picksText, enriched.map(f => ({ fixtureId: f.fixtureId, local: f.local, visitante: f.visitante, liga: f.liga, fechaPartido: f.fechaPartido }))).catch(e => console.error('recordPicks:', e.message));
}

async function handlePartido(chatId, teamName, countryHint = '', _teamDataOverride = null) {
  let teamData = _teamDataOverride;

  if (!teamData) {
    await bot.sendMessage(chatId, `🔍 Buscando *${teamName}* en nuestra base de datos...`, { parse_mode: 'Markdown' });
    teamData = await findTeamWithButtons(chatId, teamName, countryHint, { intencion: 'partido_especifico' });
    if (!teamData) return bot.sendMessage(chatId, `❌ No encontré el equipo "${teamName}" en nuestra base de datos.`);
    if (teamData === 'PENDING') return;
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

  // Ligas europeas (CL/EL/ECL): también buscar stats de liga doméstica para mejor baseline
  const isEuropean      = [2486, 3337, 722432].includes(leagueId);
  // Mundial y competiciones de selecciones nacionales (IDs de Highlightly)
  const NATIONAL_LEAGUES_SET = new Set([1635, 8443, 4188, 5039, 29718, 14400]);
  const isNationalTeamMatch  = NATIONAL_LEAGUES_SET.has(leagueId);

  const fixtureDate = nextRaw.fixture.date.split('T')[0];

  // Fase 1: todas las llamadas paralelas independientes
  const requests = [
    getH2H(homeId, awayId),                                    // 0
    getTeamStats(homeId, leagueId),                            // 1
    getTeamStats(awayId, leagueId),                            // 2
    getLineups(nextRaw.fixture.id),                            // 3
    getInjuries(homeId, leagueId),                             // 4
    getInjuries(awayId, leagueId),                             // 5
    getApiPrediction(nextRaw.fixture.id),                      // 6
    getLeagueStandings(leagueId).catch(() => ({ teams: [] })), // 7
    getTeamLastFixtures(homeId, 20),                           // 8
    getTeamLastFixtures(awayId, 20),                           // 9
  ];
  if (isLive) {
    requests.push(getFixtureStatistics(nextRaw.fixture.id)); // 11
    requests.push(getFixtureEvents(nextRaw.fixture.id));     // 12
  }
  if (isEuropean) {
    requests.push(
      Promise.any([119924,67162,33973,115669,52695,75672,80778].map(lid => getTeamStats(homeId, lid).then(s => s ? s : Promise.reject()))).catch(() => null),
      Promise.any([119924,67162,33973,115669,52695,75672,80778].map(lid => getTeamStats(awayId, lid).then(s => s ? s : Promise.reject()))).catch(() => null)
    );
  }
  // Para selecciones nacionales NO se disparan requests extra aquí:
  // getTeamStats() ya maneja internamente la cascada de 11 competiciones (WC, Qualifiers,
  // Nations League, Friendlies, Euros, Copa América) con delay entre intentos,
  // y como último recurso llama a getNationalTeamRecentStats().
  // Disparar llamadas adicionales en paralelo solo provocaría rate-limit en API-Football.

  const results = await Promise.allSettled(requests);
  const h2hRes             = results[0];
  const homeStatsRes       = results[1];
  const awayStatsRes       = results[2];
  const lineupsRes         = results[3];
  const injHomeRes         = results[4];
  const injAwayRes         = results[5];
  const predRes            = results[6];
  const standingsRes       = results[7];
  const homeLastFixturesRes = results[8];
  const awayLastFixturesRes = results[9];
  const liveStatsRes       = isLive ? results[10] : null;
  const liveEventsRes      = isLive ? results[11] : null;
  const euroBase           = isLive ? 12 : 10;
  const homedomRes         = isEuropean ? results[euroBase]     : null;
  const awaydomRes         = isEuropean ? results[euroBase + 1] : null;

  const h2hData         = h2hRes.status === 'fulfilled'        ? h2hRes.value        : [];
  const homeStatsData   = homeStatsRes.status === 'fulfilled'   ? homeStatsRes.value   : null;
  const awayStatsData   = awayStatsRes.status === 'fulfilled'   ? awayStatsRes.value   : null;
  const lineupsData     = lineupsRes?.status === 'fulfilled'    ? lineupsRes.value    : null;
  const injHomeData     = injHomeRes?.status === 'fulfilled'    ? injHomeRes.value    : [];
  const injAwayData     = injAwayRes?.status === 'fulfilled'    ? injAwayRes.value    : [];
  const predData        = predRes?.status === 'fulfilled'       ? predRes.value       : null;
  const standingsData   = standingsRes?.status === 'fulfilled'  ? standingsRes.value  : { teams: [] };
  const homeLastFixtures = homeLastFixturesRes?.status === 'fulfilled' ? homeLastFixturesRes.value : [];
  const awayLastFixtures = awayLastFixturesRes?.status === 'fulfilled' ? awayLastFixturesRes.value : [];
  const liveStatsData   = (isLive && liveStatsRes?.status === 'fulfilled') ? liveStatsRes.value : null;
  const rawEventsData   = (isLive && liveEventsRes?.status === 'fulfilled') ? liveEventsRes.value : [];
  const liveEventsData  = rawEventsData.length > 0 ? summarizeEvents(rawEventsData, homeTeam, awayTeam) : null;
  const homeDomStats    = isEuropean && homedomRes?.status === 'fulfilled' ? homedomRes.value : null;
  const awayDomStats    = isEuropean && awaydomRes?.status === 'fulfilled' ? awaydomRes.value : null;
  // Para selecciones: getTeamStats ya devuelve la mejor fuente disponible después de su cascada
  // (WC → Qualifiers → Friendlies → Nations League → Euros → últimos 20 partidos)
  const homeBaseForNat  = homeStatsData;
  const awayBaseForNat  = awayStatsData;

  const apiReferee = nextRaw.fixture.referee || null;
  const referee = predData?._referee || apiReferee || null;
  const refereeStats = referee ? await getRefereeCardStats(referee).catch(() => null) : null;

  // Standings: extraer posición y motivación de cada equipo
  const homeStanding = standingsData.teams.find(t => t.teamId === homeId) || null;
  const awayStanding = standingsData.teams.find(t => t.teamId === awayId) || null;
  const totalTeams   = standingsData.total || standingsData.teams.length || 20;
  const motivLocal   = getTeamMotivation(homeStanding, totalTeams);
  const motivVisit   = getTeamMotivation(awayStanding, totalTeams);

  // Detectar contexto de eliminatoria (CL/EL Knockout o ronda específica)
  const round = nextRaw.league?.round || '';
  const isKnockout = /quarter|semi|final|round of|octavos|cuartos|semis|knockout/i.test(round);
  const isSingleLegFinal = /\bfinal\b/i.test(round);
  let contextoEliminatoria = null;

  // Finales de partido único (FA Cup, Copa del Rey, etc.) — sin primera pata
  if (isSingleLegFinal) {
    const marcadorActual = isLive
      ? `${nextRaw.goals?.home ?? 0}-${nextRaw.goals?.away ?? 0}`
      : null;
    const esEmpate0_0AlHT = isLive &&
      nextRaw.fixture?.status?.short === 'HT' &&
      (nextRaw.goals?.home ?? 0) === 0 &&
      (nextRaw.goals?.away ?? 0) === 0;
    contextoEliminatoria = {
      ronda: round,
      tipoPartido: 'FINAL DE COPA — partido único, no hay primera pata ni global',
      implicacionTactica: 'Ambos equipos priorizan no perder sobre atacar. Alta probabilidad de prórroga si el marcador sigue igualado. Las estadísticas de goles de temporada NO aplican directamente — las finales son tácticamente únicas.',
      alertaGoles: esEmpate0_0AlHT
        ? '⚠️ FINAL 0-0 AL DESCANSO: probabilidad de prórroga muy alta. PROHIBIDO recomendar Over 2.5 o Over 3.5 FT. Busca Under 2.5, BTTS No, corners o tarjetas.'
        : marcadorActual
        ? `Marcador actual: ${marcadorActual}. Partido único — sin posibilidad de clasificarse en global.`
        : 'Partido único de final — no hay segunda oportunidad. Táctica extremadamente conservadora esperada.',
    };
  }

  if (isKnockout && !isSingleLegFinal) {
    // Buscar la ida en el H2H (partido reciente de la misma competición entre estos equipos)
    const primeraPata = h2hData.find(m =>
      m.liga === nextRaw.league.name &&
      new Date(m.fecha) < new Date(nextRaw.fixture.date)
    );
    if (primeraPata) {
      const golesHome1 = primeraPata.local === homeTeam ? primeraPata.golesLocal : primeraPata.golesVisitante;
      const golesAway1 = primeraPata.local === homeTeam ? primeraPata.golesVisitante : primeraPata.golesLocal;
      const globalHome = (golesHome1 || 0);
      const globalAway = (golesAway1 || 0);
      const ventajaGlobal = globalHome > globalAway
        ? `${homeTeam} gana el global ${globalHome}-${globalAway}`
        : globalAway > globalHome
        ? `${awayTeam} gana el global ${globalAway}-${globalHome}`
        : `Global empatado ${globalHome}-${globalAway}`;
      const necesita = globalHome > globalAway
        ? `${awayTeam} necesita al menos ${globalHome - globalAway + 1} goles sin respuesta (o ${globalHome - globalAway} para prórroga)`
        : globalAway > globalHome
        ? `${homeTeam} necesita al menos ${globalAway - globalHome + 1} goles sin respuesta`
        : 'Cualquier victoria directa clasifica';
      contextoEliminatoria = {
        ronda: round,
        primeraPata: `${primeraPata.local} ${primeraPata.golesLocal}-${primeraPata.golesVisitante} ${primeraPata.visitante}`,
        marcadorGlobal: ventajaGlobal,
        golesNecesitados: necesita,
        implicacionTactica: globalHome > globalAway
          ? `${homeTeam} puede cerrarse atrás y vivir del contragolpe. ${awayTeam} presiona al máximo desde min 1.`
          : globalAway > globalHome
          ? `${awayTeam} puede cerrarse atrás. ${homeTeam} presiona al máximo.`
          : `Partido abierto — cualquier gol cambia la táctica.`,
      };
    }
  }

  // Calcular probabilidades con modelo de Poisson
  // Europeas → stats domésticas; Selecciones → combinar todas las fuentes; Resto → stats de temporada
  const homeBase = homeDomStats || (isNationalTeamMatch ? homeBaseForNat : homeStatsData);
  const awayBase = awayDomStats || (isNationalTeamMatch ? awayBaseForNat : awayStatsData);
  const homeFallback = computeAvgFromFixtures(homeId, homeLastFixtures);
  const awayFallback = computeAvgFromFixtures(awayId, awayLastFixtures);
  let probBlock = buildProbBlock(homeBase, awayBase, h2hData, leagueId, homeFallback, awayFallback);
  if (isSingleLegFinal && probBlock) {
    // Reducir un 30% los xG para reflejar que las finales producen ~1.8 goles de media vs ~2.7 en liga
    const FINAL_FACTOR = 0.70;
    probBlock = {
      ...probBlock,
      xGLocal:    probBlock.xGLocal    != null ? +(probBlock.xGLocal    * FINAL_FACTOR).toFixed(2) : null,
      xGVisitante: probBlock.xGVisitante != null ? +(probBlock.xGVisitante * FINAL_FACTOR).toFixed(2) : null,
      _ajusteFinal: `Lambdas reducidas ${Math.round((1 - FINAL_FACTOR) * 100)}% por ser final de copa (partido único, táctica conservadora). Las probabilidades de Over son más bajas que en liga regular.`,
    };
  }

  // Momentum y proyecciones en vivo
  const momentum   = isLive ? calcLiveMomentum(liveStatsData, homeTeam, awayTeam) : null;
  const elapsed    = nextRaw.fixture?.status?.elapsed || 0;
  const homeCorners= liveStatsData ? (homeStats(liveStatsData)?.['Corner Kicks'] ?? 0) : 0;
  const awayCorners= liveStatsData ? (awayStats(liveStatsData)?.['Corner Kicks'] ?? 0) : 0;
  const liveHomeGoals = nextRaw.goals?.home ?? 0;
  const liveAwayGoals = nextRaw.goals?.away ?? 0;
  const totalCorners = homeCorners + awayCorners;
  const cornersProj= isLive && elapsed > 0
    ? calcCornersProjection(totalCorners, elapsed, liveHomeGoals, liveAwayGoals)
    : null;
  const homeCards  = liveStatsData
    ? ((homeStats(liveStatsData)?.['Yellow Cards'] ?? 0) + (homeStats(liveStatsData)?.['Red Cards'] ?? 0))
    : 0;
  const awayCards  = liveStatsData
    ? ((awayStats(liveStatsData)?.['Yellow Cards'] ?? 0) + (awayStats(liveStatsData)?.['Red Cards'] ?? 0))
    : 0;
  const totalCards = homeCards + awayCards;
  const cardsProj  = isLive && elapsed > 0
    ? calcCardsProjection(totalCards, elapsed, homeCards, awayCards)
    : null;
  // Líneas con valor real (prob 20-80%) para goles, corners y tarjetas
  const chaseFactor      = cornersProj?.chaseFactor ?? 1.08;
  const regressionFactor = cardsProj?.regressionFactor ?? 0.85;
  const totalLiveGoals   = liveHomeGoals + liveAwayGoals;
  const homeFor = homeBase?.golesAnotadosHome ? parseFloat(homeBase.golesAnotadosHome) : 1.3;
  const awayFor = awayBase?.golesAnotadosAway ? parseFloat(awayBase.golesAnotadosAway) : 1.0;
  const goalLines    = isLive && elapsed > 0
    ? calcLiveGoalLines(totalLiveGoals, elapsed, homeFor, awayFor)
    : null;
  const cornersLines = isLive && elapsed > 0
    ? calcLiveCornersLines(totalCorners, elapsed, chaseFactor)
    : null;
  const cardsLines   = isLive && elapsed > 0
    ? calcLiveCardsLines(totalCards, elapsed, regressionFactor)
    : null;

  const analysisData = {
    _aviso: 'TODAS las estadísticas de este JSON son promedios de TEMPORADA COMPLETA en TODAS las competiciones. NUNCA las etiquetes como estadísticas de una copa o competición específica. SOLO puedes citar números que aparezcan literalmente en este JSON — si un dato no está aquí, escribe "sin datos disponibles".',
    partido: {
      liga:      nextRaw.league.name,
      ronda:     round || null,
      pais:      nextRaw.league.country,
      fecha:     nextRaw.fixture.date.split('T')[0],
      hora:      formatHour(nextRaw.fixture.date),
      local:     homeTeam,
      visitante: awayTeam,
      enVivo:    isLive,
      minuto:    elapsed || null,
      marcador:  isLive ? `${nextRaw.goals?.home ?? 0}-${nextRaw.goals?.away ?? 0}` : null,
      arbitro:   referee,
      estadio:   predData?._venue || nextRaw.fixture.venue?.name || null,
      ciudad:    predData?._city  || nextRaw.fixture.venue?.city  || null,
      grupo:     null,
      jornada:   round || null,
    },
    ...(refereeStats && { arbitroStats: refereeStats }),
    contextoPorPartido: {
      queSeJuegaLocal:    motivLocal.texto,
      queSeJuegaVisitante: motivVisit.texto,
      posicionLocal:      homeStanding ? `${homeStanding.rank}º — ${homeStanding.points} pts` : 'sin datos de clasificación',
      posicionVisitante:  awayStanding ? `${awayStanding.rank}º — ${awayStanding.points} pts` : 'sin datos de clasificación',
      jornadasRestantes:  motivLocal.jornadas_restantes ?? null,
      ...(isSingleLegFinal && { esPartidoUnico: 'FINAL DE COPA — partido único, no hay vuelta. Presión máxima para ambos.' }),
    },
    ...(predData && { prediccionAPIFootball: predData }),
    h2h:            h2hData,
    bttsEnH2H:      h2hData.filter(m => m.btts).length,
    // Para selecciones: homeStatsData ya contiene la mejor fuente disponible (ver getTeamStats cascade).
    // El campo 'nota' dentro de stats indica si es referencia de otra competición o de últimos 20 partidos.
    statsLocal:     homeStatsData
      ? withHomeContext({ ...homeStatsData, _fuente: homeStatsData.nota || 'datos del torneo actual' })
      : null,
    statsVisitante: awayStatsData
      ? withAwayContext({ ...awayStatsData, _fuente: awayStatsData.nota || 'datos del torneo actual' })
      : null,
    ...(homeDomStats && { statsLocalLigaDomestica:     { ...homeDomStats,     _fuente: 'promedio liga doméstica' } }),
    ...(awayDomStats && { statsVisitanteLigaDomestica: { ...awayDomStats,     _fuente: 'promedio liga doméstica' } }),
    ultimosPartidosLocal:     homeLastFixtures,
    ultimosPartidosVisitante: awayLastFixtures,
    estadisticasVivo: liveStatsData,
    ...(liveEventsData && { eventosPartido: liveEventsData }),  // ← goles con jugador, tarjetas, cambios
    ...(lineupsData && lineupsData.length > 0 && { alineaciones: lineupsData }),
    ...(injHomeData && injHomeData.length > 0 && { lesionadosLocal: injHomeData }),
    ...(injAwayData && injAwayData.length > 0 && { lesionadosVisitante: injAwayData }),
    ...(contextoEliminatoria && { contextoEliminatoria }),
    ...(probBlock    && { probabilidadesCalculadas: probBlock }),
    ...(momentum     && { momentumEnVivo: momentum }),
    ...(cornersProj  && { proyeccionCorners: cornersProj }),
    ...(cardsProj    && { proyeccionTarjetas: cardsProj }),
    ...(goalLines    && { lineasGolesVivo:    goalLines }),
    ...(cornersLines && { lineasCornersVivo:  cornersLines }),
    ...(cardsLines   && { lineasCardsVivo:    cardsLines }),
  };

  await bot.sendMessage(chatId, '⚡ Procesando análisis profundo...');

  // ── En vivo → INPLAY_SYSTEM (necesita contexto del partido activo) ──────────
  if (isLive) {
    const liveOdds = await getLiveOdds(nextRaw.fixture.id).catch(() => null);
    if (liveOdds) {
      analysisData.cuotasVivo = liveOdds;
    } else {
      analysisData.cuotasVivo = null;
      // Sin cuotas en vivo: el bot igual da picks con cuota sugerida mínima.
      // El usuario verifica en su casa si hay valor — necesita la DIRECCIÓN, no el número exacto.
    }
    const analysis = await sonnet(
      INPLAY_SYSTEM,
      `Analiza este partido EN VIVO:\n\n${JSON.stringify(analysisData, null, 2)}`
    );
    try {
      await sendLong(chatId, `🎯 *${homeTeam} vs ${awayTeam}*\n\n${analysis}`, { parse_mode: 'Markdown' });
    } catch {
      await sendLong(chatId, `🎯 ${homeTeam} vs ${awayTeam}\n\n${analysis.replace(/[*_`]/g, '')}`);
    }
    recordPicks(analysis, [{ fixtureId: nextRaw.fixture.id, local: homeTeam, visitante: awayTeam, liga: nextRaw.league.name, fechaPartido: nextRaw.fixture.date }]).catch(e => console.error('recordPicks:', e.message));
    return;
  }

  // ── Pre-partido → motor JS selecciona picks de valor, luego análisis profundo ─
  const realOdds = await getRealOdds(nextRaw.fixture.id).catch(() => null);
  if (realOdds) analysisData.cuotasReales = realOdds;

  // Usar la mejor fuente disponible para el motor de probabilidades
  const _bestHomeStats = homeDomStats || homeBaseForNat || homeStatsData;
  const _bestAwayStats = awayDomStats || awayBaseForNat || awayStatsData;
  const hFor = parseFloat(_bestHomeStats?.golesAnotadosHome) || 1.3;
  const hAgt = parseFloat(_bestHomeStats?.golesRecibidosHome) || 1.1;
  const aFor = parseFloat(_bestAwayStats?.golesAnotadosAway) || 1.0;
  const aAgt = parseFloat(_bestAwayStats?.golesRecibidosAway) || 1.3;
  const extProbs = calcExtendedProbs(hFor, hAgt, aFor, aAgt, nextRaw.league?.name || '');

  const fixtureForEngine = {
    fixtureId:      nextRaw.fixture.id,
    liga:           nextRaw.league.name,
    country:        nextRaw.league.country,
    local:          homeTeam,
    visitante:      awayTeam,
    hora:           formatHour(nextRaw.fixture.date),
    statsLocal:     withHomeContext(homeDomStats || homeStatsData),
    statsVisitante: withAwayContext(awayDomStats || awayStatsData),
    _extendedProbs: extProbs,
    cuotasReales:   realOdds || undefined,
  };

  const partidoCandidates = buildPickCandidates([fixtureForEngine]);
  const partidoPicks      = selectDiversePicks(partidoCandidates, 3);
  console.log(`🎯 handlePartido — candidatos JS: ${partidoCandidates.length} | picks: ${partidoPicks.length} | cuotas: ${realOdds ? 'sí' : 'no'} | knockout: ${isKnockout}`);

  const season = LEAGUE_SEASONS[leagueId] || 2025;
  let analysis;
  // Para partido individual siempre análisis profundo con todos los datos.
  // El motor JS pre-calcula candidatos que se incluyen en analysisData como
  // referencia matemática, pero el LLM elige los 3 mejores con datos reales.
  if (partidoPicks.length >= 1) {
    analysisData.picksMotorJS = partidoPicks; // referencia, el LLM puede usarlos o enriquecerlos
  }
  analysis = await sonnet(
    PARTIDO_DEEP_SYSTEM,
    `Analiza este partido en profundidad (temporada ${season}):\n\n${JSON.stringify(analysisData, null, 2)}`
  );

  try {
    await sendLong(chatId, `🎯 *${homeTeam} vs ${awayTeam}*\n\n${analysis}`, { parse_mode: 'Markdown' });
  } catch {
    await sendLong(chatId, `🎯 ${homeTeam} vs ${awayTeam}\n\n${analysis.replace(/[*_`]/g, '')}`);
  }
  recordPicks(analysis, [{ fixtureId: nextRaw.fixture.id, local: homeTeam, visitante: awayTeam, liga: nextRaw.league.name, fechaPartido: nextRaw.fixture.date }]).catch(e => console.error('recordPicks:', e.message));
}

async function handleVivo(chatId, leagueId = null, leagueName = null) {
  const displayName = leagueName || 'todas las ligas';
  await bot.sendMessage(chatId, `📡 Obteniendo datos en tiempo real (${displayName})...`);

  let liveFixtures = await getLiveFixtures(leagueId);
  // En picks en vivo automáticos (sin liga específica), excluir ligas con mal historial
  if (!leagueId) {
    liveFixtures = liveFixtures.filter(f => !PICKS_EXCLUDE_LEAGUES.has(f.leagueId));
  }
  console.log(`📊 Partidos en vivo encontrados: ${liveFixtures.length}`);

  if (liveFixtures.length === 0) {
    const msg = leagueName
      ? `😔 No hay partidos de *${leagueName}* en vivo ahora mismo.`
      : '😔 No hay partidos en curso en las ligas monitoreadas ahora mismo.';
    return bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
  }

  await bot.sendMessage(chatId, `⏳ *${liveFixtures.length}* partido(s) en vivo detectados. Recopilando estadísticas...`, { parse_mode: 'Markdown' });

  // Get live stats + eventos + team stats históricos para todos los partidos en paralelo
  const toAnalyze = liveFixtures.slice(0, 4);
  const today = new Date().toISOString().split('T')[0];
  const NATIONAL_LEAGUES_VIVO = new Set([1635, 8443, 4188, 5039, 29718, 14400]);
  const [liveStatsResults, liveEventsResults, homeStatsResults, awayStatsResults] = await Promise.all([
    Promise.allSettled(toAnalyze.map(f => getFixtureStatistics(f.fixtureId))),
    Promise.allSettled(toAnalyze.map(f => getFixtureEvents(f.fixtureId))),
    Promise.allSettled(toAnalyze.map(f => NATIONAL_LEAGUES_VIVO.has(f.leagueId) ? getNationalTeamRecentStats(f.homeId, 20) : getTeamStats(f.homeId, f.leagueId))),
    Promise.allSettled(toAnalyze.map(f => NATIONAL_LEAGUES_VIVO.has(f.leagueId) ? getNationalTeamRecentStats(f.awayId, 20) : getTeamStats(f.awayId, f.leagueId))),
  ]);

  // Standings por liga única
  const uniqueLeagueIdsVivo = [...new Set(toAnalyze.map(f => f.leagueId))];
  const standingsVivo = await Promise.allSettled(uniqueLeagueIdsVivo.map(lid => getLeagueStandings(lid)));
  const standingsMapVivo = {};
  uniqueLeagueIdsVivo.forEach((lid, i) => {
    if (standingsVivo[i].status === 'fulfilled') standingsMapVivo[lid] = standingsVivo[i].value;
  });

  const enriched = toAnalyze.map((f, i) => {
    const liveStats  = liveStatsResults[i].status  === 'fulfilled' ? liveStatsResults[i].value  : null;
    const rawEvents  = liveEventsResults[i].status === 'fulfilled' ? liveEventsResults[i].value  : [];
    const hStats     = homeStatsResults[i].status  === 'fulfilled' ? homeStatsResults[i].value   : null;
    const aStats     = awayStatsResults[i].status  === 'fulfilled' ? awayStatsResults[i].value   : null;
    const elapsed    = f.elapsed || 0;
    const standingsLeague = standingsMapVivo[f.leagueId] || { teams: [], total: 20 };
    const homeStanding = standingsLeague.teams?.find?.(t => t.teamId === f.homeId) || null;
    const awayStanding = standingsLeague.teams?.find?.(t => t.teamId === f.awayId) || null;
    const totalTeamsV  = standingsLeague.total || standingsLeague.teams?.length || 20;

    const momentum    = calcLiveMomentum(liveStats, f.homeTeam, f.awayTeam);
    const homeCorners = liveStats ? (homeStats(liveStats)?.['Corner Kicks'] ?? 0) : 0;
    const awayCorners = liveStats ? (awayStats(liveStats)?.['Corner Kicks'] ?? 0) : 0;
    const totalCornersV = homeCorners + awayCorners;
    const cornersProj = elapsed > 0
      ? calcCornersProjection(totalCornersV, elapsed, f.homeGoals ?? 0, f.awayGoals ?? 0)
      : null;
    const homeCards   = liveStats ? ((homeStats(liveStats)?.['Yellow Cards']??0)+(homeStats(liveStats)?.['Red Cards']??0)) : 0;
    const awayCards   = liveStats ? ((awayStats(liveStats)?.['Yellow Cards']??0)+(awayStats(liveStats)?.['Red Cards']??0)) : 0;
    const totalCardsV = homeCards + awayCards;
    const cardsProj   = elapsed > 0
      ? calcCardsProjection(totalCardsV, elapsed, homeCards, awayCards)
      : null;
    const totalGoalsV    = (f.homeGoals ?? 0) + (f.awayGoals ?? 0);
    const hForV = hStats?.golesAnotadosHome ? parseFloat(hStats.golesAnotadosHome) : 1.3;
    const aForV = aStats?.golesAnotadosAway ? parseFloat(aStats.golesAnotadosAway) : 1.0;
    const goalLinesV     = elapsed > 0
      ? calcLiveGoalLines(totalGoalsV, elapsed, hForV, aForV)
      : null;
    const cornersLinesV  = elapsed > 0
      ? calcLiveCornersLines(totalCornersV, elapsed, cornersProj?.chaseFactor ?? 1.08)
      : null;
    const cardsLinesV    = elapsed > 0
      ? calcLiveCardsLines(totalCardsV, elapsed, cardsProj?.regressionFactor ?? 0.85)
      : null;
    const eventosResumen = rawEvents.length > 0 ? summarizeEvents(rawEvents, f.homeTeam, f.awayTeam) : null;

    return {
      _aviso: 'Stats históricas son promedios de temporada completa — NUNCA las etiquetes como stats de una copa o competición específica. Solo cita números literalmente presentes en este JSON.',
      _competicion: `IMPORTANTE: el nombre de esta competición es "${f.leagueName}" — usa EXACTAMENTE este nombre en el campo [Liga/Copa] del formato. NUNCA lo cambies.`,
      ...f,
      marcador: `${f.homeGoals ?? 0}-${f.awayGoals ?? 0}`,
      arbitro: f.referee || null,
      contextoPorPartido: {
        queSeJuegaLocal:     getTeamMotivation(homeStanding, totalTeamsV).texto,
        queSeJuegaVisitante: getTeamMotivation(awayStanding, totalTeamsV).texto,
        posicionLocal:       homeStanding ? `${homeStanding.rank}º — ${homeStanding.points} pts` : 'sin datos',
        posicionVisitante:   awayStanding ? `${awayStanding.rank}º — ${awayStanding.points} pts` : 'sin datos',
      },
      statsLocal:     hStats ? { ...hStats, _fuente: 'promedio temporada completa' } : null,
      statsVisitante: aStats ? { ...aStats, _fuente: 'promedio temporada completa' } : null,
      estadisticasVivo:  liveStats,
      eventosPartido:    eventosResumen,   // ← goles con jugador, tarjetas activas, cambios
      ...(momentum      && { momentumEnVivo:       momentum }),
      ...(cornersProj   && { proyeccionCorners:    cornersProj }),
      ...(cardsProj     && { proyeccionTarjetas:   cardsProj }),
      ...(goalLinesV    && { lineasGolesVivo:       goalLinesV }),
      ...(cornersLinesV && { lineasCornersVivo:     cornersLinesV }),
      ...(cardsLinesV   && { lineasCardsVivo:       cardsLinesV }),
    };
  });

  // Estadísticas del árbitro en vivo (WorldReferee.com)
  {
    const uniqueRefsVivo = [...new Set(enriched.map(e => e.arbitro).filter(Boolean))];
    if (uniqueRefsVivo.length > 0) {
      const refResultsVivo = await Promise.allSettled(uniqueRefsVivo.map(n => getRefereeCardStats(n)));
      const refMapVivo = {};
      uniqueRefsVivo.forEach((n, i) => {
        if (refResultsVivo[i].status === 'fulfilled' && refResultsVivo[i].value) refMapVivo[n] = refResultsVivo[i].value;
      });
      for (const e of enriched) {
        if (e.arbitro && refMapVivo[e.arbitro]) e.arbitroStats = refMapVivo[e.arbitro];
      }
    }
  }

  // Cuotas en vivo reales para cada partido
  await bot.sendMessage(chatId, '📈 Consultando cuotas en vivo...');
  const liveOddsResults = await Promise.allSettled(toAnalyze.map(f => getLiveOdds(f.fixtureId)));
  for (let i = 0; i < enriched.length; i++) {
    const lo = liveOddsResults[i].status === 'fulfilled' ? liveOddsResults[i].value : null;
    if (lo) {
      enriched[i].cuotasVivo = lo;
      // Filtro de valor: marcar mercados con cuota < 1.50 como sin valor (Claude los ignora)
      const LIVE_MIN_ODDS = 1.50;
      const sinValor = Object.entries(lo)
        .filter(([k, v]) => k !== 'source' && typeof v === 'number' && v < LIVE_MIN_ODDS)
        .map(([k]) => k);
      if (sinValor.length > 0) {
        enriched[i].cuotasVivo._mercadosSinValor = sinValor;
        enriched[i].cuotasVivo._nota = `Mercados con cuota < ${LIVE_MIN_ODDS} (sin valor): ${sinValor.join(', ')}. PROHIBIDO recomendar estos mercados.`;
        console.log(`⚠️ Live odds sin valor (< ${LIVE_MIN_ODDS}): ${enriched[i].homeTeam} vs ${enriched[i].awayTeam} — ${sinValor.join(', ')}`);
      }
    } else {
      enriched[i].cuotasVivo = null;
    }
  }

  await bot.sendMessage(chatId, '🎯 Identificando picks de valor...');
  const analysis = await sonnet(
    INPLAY_SYSTEM,
    `DATOS REALES EN VIVO:\n\n${JSON.stringify(enriched, null, 2)}\n\nMáximo 3 picks en total. REGLA IRROMPIBLE DE CUOTA: cuota mínima 1.50 para cualquier pick. Si cuotasVivo es null → solo recomienda mercados cuya cuota estimada (lineasConValor) sea > 1.50; si no hay ninguno → escribe ⛔ Sin picks de valor en este momento. PROHIBIDO dar picks con cuota obvia (< 1.50) aunque el análisis lo favorezca.`
  );
  try {
    await sendLong(chatId, `🔴 *PICKS EN VIVO${leagueName ? ' — ' + leagueName : ''}*\n\n${analysis}`, { parse_mode: 'Markdown' });
  } catch {
    await sendLong(chatId, `🔴 PICKS EN VIVO${leagueName ? ' — ' + leagueName : ''}\n\n${analysis.replace(/[*_`]/g, '')}`);
  }
  recordPicks(analysis, enriched.map(f => ({ fixtureId: f.fixtureId, local: f.homeTeam, visitante: f.awayTeam, liga: f.leagueName, fechaPartido: f.date }))).catch(e => console.error('recordPicks:', e.message));
}

// ─── Alerta de Gol ────────────────────────────────────────────────────────────

async function handleAlertaGol(chatId) {
  await bot.sendMessage(chatId, '⚡ Escaneando partidos en vivo en busca de oportunidades de gol...');

  // 1. Obtener todos los partidos en vivo
  const liveRaw = await fetchLiveRaw();
  // Sin filtro de liga — analiza cualquier partido activo en vivo
  const liveActive = liveRaw.filter(f =>
    ['First half', 'Second half', 'Extra time'].includes(f.state?.description)
  );

  if (liveActive.length === 0) {
    return bot.sendMessage(chatId, '😔 No hay partidos en vivo ahora mismo. Inténtalo cuando haya partidos activos.');
  }

  await bot.sendMessage(chatId, `🔍 *${liveActive.length}* partido(s) activo(s). Calculando probabilidades de gol...`, { parse_mode: 'Markdown' });

  // 2. Obtener stats en vivo + eventos + históricas en paralelo
  const candidates = liveActive.slice(0, 20);
  const [liveStatsResults, liveEventsResults, homeStatsResults, awayStatsResults] = await Promise.all([
    Promise.allSettled(candidates.map(f => getFixtureStatistics(f.id))),
    Promise.allSettled(candidates.map(f => getFixtureEvents(f.id))),
    Promise.allSettled(candidates.map(f => getTeamStats(f.homeTeam.id, f.league?.id))),
    Promise.allSettled(candidates.map(f => getTeamStats(f.awayTeam.id, f.league?.id))),
  ]);

  // 3. Calcular alerta de gol para cada partido
  const alerts = [];
  for (let i = 0; i < candidates.length; i++) {
    const f = candidates[i];
    const parsed        = parseFixture(f);
    const liveStats     = liveStatsResults[i].status  === 'fulfilled' ? liveStatsResults[i].value  : null;
    const rawEvents     = liveEventsResults[i].status === 'fulfilled' ? liveEventsResults[i].value  : [];
    const homeStatsData = homeStatsResults[i].status  === 'fulfilled' ? homeStatsResults[i].value   : null;
    const awayStatsData = awayStatsResults[i].status  === 'fulfilled' ? awayStatsResults[i].value   : null;

    const alert = calcGoalAlert(parsed, liveStats, homeStatsData, awayStatsData);
    if (alert && alert.pGoal >= 55) {
      // Adjuntar resumen de eventos al alert para enriquecer el contexto de Claude
      const evResumen = rawEvents.length > 0 ? summarizeEvents(rawEvents, parsed.homeTeam, parsed.awayTeam) : null;
      if (evResumen) alert.eventosPartido = evResumen;
      alerts.push(alert);
    }
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

  // Guardar picks para tracking de aciertos
  saveAlertaGolPicks(top);
}

async function handleEspecifica(chatId, intent) {
  const { equipo, pregunta_especifica, mercado, tiempo, contexto } = intent;

  if (!equipo) {
    return bot.sendMessage(chatId, '¿De qué equipo te interesa la estadística o análisis?');
  }

  await bot.sendMessage(chatId, `🔍 Analizando tu pregunta...`);

  const teamData = await findTeamWithButtons(chatId, equipo, intent.liga || '', intent);
  if (!teamData) return bot.sendMessage(chatId, `❌ No encontré el equipo "${equipo}" en nuestra base de datos.`);
  if (teamData === 'PENDING') return;

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
  const [h2hRes, homeStatsRes, awayStatsRes, predRes2, standRes2, lineupsRes2, injHomeRes2, injAwayRes2] = await Promise.allSettled([
    getH2H(homeId, awayId),
    getTeamStats(homeId, leagueId),
    getTeamStats(awayId, leagueId),
    getApiPrediction(nextRaw.fixture.id),
    getLeagueStandings(leagueId),
    getLineups(nextRaw.fixture.id),
    getInjuries(homeId, leagueId),
    getInjuries(awayId, leagueId),
  ]);

  const h2hData2       = h2hRes.status === 'fulfilled'       ? h2hRes.value       : [];
  const homeStatsData2 = homeStatsRes.status === 'fulfilled'  ? homeStatsRes.value  : null;
  const awayStatsData2 = awayStatsRes.status === 'fulfilled'  ? awayStatsRes.value  : null;
  const predData2      = predRes2?.status === 'fulfilled'     ? predRes2.value      : null;
  const standData2     = standRes2?.status === 'fulfilled'    ? standRes2.value     : { teams: [], total: 20 };
  const lineupsData2   = lineupsRes2?.status === 'fulfilled'  ? lineupsRes2.value   : null;
  const injHome2       = injHomeRes2?.status === 'fulfilled'  ? injHomeRes2.value   : [];
  const injAway2       = injAwayRes2?.status === 'fulfilled'  ? injAwayRes2.value   : [];
  const apiRef2 = nextRaw.fixture.referee || null;
  const referee2 = predData2?._referee || apiRef2 || null;
  const refereeStats2 = referee2 ? await getRefereeCardStats(referee2).catch(() => null) : null;

  const homeStanding2  = standData2.teams?.find(t => t.teamId === homeId) || null;
  const awayStanding2  = standData2.teams?.find(t => t.teamId === awayId) || null;
  const totalTeams2    = standData2.total || standData2.teams?.length || 20;
  const probBlock2     = buildProbBlock(homeStatsData2, awayStatsData2, h2hData2);

  const analysisData = {
    _aviso: 'SOLO cita números literalmente presentes en este JSON. Stats son promedios de temporada completa — NUNCA las etiquetes como de una copa o competición específica.',
    partido: {
      liga:      nextRaw.league.name,
      ronda:     nextRaw.league?.round || null,
      fecha:     fixtureDate2,
      hora:      formatHour(nextRaw.fixture.date),
      local:     homeTeam,
      visitante: awayTeam,
      arbitro:   referee2,
      estadio:   predData2?._venue || nextRaw.fixture.venue?.name || null,
      ciudad:    predData2?._city  || nextRaw.fixture.venue?.city  || null,
      grupo:     null,
      jornada:   nextRaw.league?.round || null,
    },
    ...(refereeStats2 && { arbitroStats: refereeStats2 }),
    contextoPorPartido: {
      queSeJuegaLocal:     getTeamMotivation(homeStanding2, totalTeams2).texto,
      queSeJuegaVisitante: getTeamMotivation(awayStanding2, totalTeams2).texto,
      posicionLocal:       homeStanding2 ? `${homeStanding2.rank}º — ${homeStanding2.points} pts` : 'sin datos',
      posicionVisitante:   awayStanding2 ? `${awayStanding2.rank}º — ${awayStanding2.points} pts` : 'sin datos',
    },
    equipoConsultado: teamFull,
    rolEnPartido: isHome ? 'LOCAL' : 'VISITANTE',
    h2h:          h2hData2,
    bttsEnH2H:    h2hData2.filter(m => m.btts).length,
    statsLocal:     homeStatsData2 ? withHomeContext({ ...homeStatsData2, _fuente: 'promedio temporada completa' }) : null,
    statsVisitante: awayStatsData2 ? withAwayContext({ ...awayStatsData2, _fuente: 'promedio temporada completa' }) : null,
    ...(predData2    && { prediccionAPIFootball: predData2 }),
    ...(lineupsData2 && lineupsData2.length > 0 && { alineaciones: lineupsData2 }),
    ...(injHome2     && injHome2.length > 0 && { lesionadosLocal: injHome2 }),
    ...(injAway2     && injAway2.length > 0 && { lesionadosVisitante: injAway2 }),
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
  // ── Modo fecha: "rachas de hoy" / "rachas de mañana" ──────────────────────
  const pd = (intent.period || '').toLowerCase().replace('ñ', 'n');
  const q  = (intent.pregunta_especifica || '').toLowerCase();
  const isHoy    = pd === 'hoy'    || /\bhoy\b/.test(q);
  const isManana = pd === 'manana' || /\bma[nñ]ana\b/.test(q);
  if (isHoy || isManana) {
    // ── Fecha en Bogotá (UTC-5) — sin depender del timezone del servidor ──────
    const todayBogota = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
    let dateStr = todayBogota;
    let label = 'de hoy';
    if (isManana) {
      // Sumar 1 día usando UTC puro para evitar DST/server-timezone issues
      const [y, m, d] = todayBogota.split('-').map(Number);
      dateStr = new Date(Date.UTC(y, m - 1, d + 1)).toISOString().split('T')[0];
      label = 'de mañana';
    }
    return handleRachasFecha(chatId, dateStr, label);
  }

  const venueRaw = intent.venue || 'all';
  const venueParam = venueRaw === 'home' ? 'home' : venueRaw === 'away' ? 'away' : null;
  const venueLabel = { home: 'en casa', away: 'de visita', all: 'en total' }[venueRaw] || 'en total';
  const MIN_STREAK = 5;

  // ── Modo equipo específico ──────────────────────────────────────────────────
  if (intent.equipo) {
    await bot.sendMessage(chatId, `🔍 Buscando rachas de *${intent.equipo}*...`, { parse_mode: 'Markdown' });

    let resolvedTeamData = intent._teamData || null;
    if (!resolvedTeamData) {
      const teamData = await findTeamWithButtons(chatId, intent.equipo, intent.liga || '', { ...intent, intencion: 'rachas' });
      if (!teamData) return bot.sendMessage(chatId, `😔 No encontré el equipo *${intent.equipo}*.`, { parse_mode: 'Markdown' });
      if (teamData === 'PENDING') return;
      resolvedTeamData = teamData;
    }

    const teamId   = resolvedTeamData.team.id;
    const teamName = resolvedTeamData.team.name;

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
      '¿Qué rachas quieres ver?\n\n*Por fecha:*\n• *rachas de hoy* → equipos con rachas en partidos de hoy\n• *rachas de mañana* → idem para mañana\n\n*Por equipo:*\n• *rachas Real Madrid*\n• *rachas Atlético de Madrid*\n\n*Por liga:*\n• *rachas Premier League*\n• *rachas en casa Serie A*\n• *rachas de visita Bundesliga*',
      { parse_mode: 'Markdown' }
    );
  }

  const leagueName = LEAGUE_MAP[leagueId]?.name || intent.liga;
  await bot.sendMessage(chatId, `🔍 Analizando rachas en *${leagueName}* ${venueLabel}...`, { parse_mode: 'Markdown' });

  const standingsResult = await getLeagueStandings(leagueId);
  const teams = standingsResult.teams || standingsResult;
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

// ─── Rachas por Fecha ─────────────────────────────────────────────────────────
// Ligas incluidas en "rachas de hoy/mañana" (top-tier para mantener velocidad)
const RACHAS_FECHA_LEAGUES = new Set([
  33973,34824,   // Premier League + Championship
  119924,120775, // La Liga + LaLiga2
  115669,116520, // Serie A + Serie B
  67162,68013,   // Bundesliga + 2.Bundesliga
  52695,53546,   // Ligue 1 + Ligue 2
  2486,3337,722432, // UCL + UEL + UECL
  75672,80778,   // Eredivisie + Primeira Liga
  61205,         // Brasileirao
  10145,11847,   // Copa Sudamericana + Copa Libertadores
  109712,        // Liga Argentina
  204173,        // Liga Betplay Colombia
  223746,        // Liga MX
  173537,        // Süper Lig
]);

// Simple in-memory cache for getTeamLastFixtures (TTL 4h)
const _teamFxCache = new Map();
async function getTeamLastFixturesCached(teamId, last = 12) {
  const key = `${teamId}_${last}`;
  const c = _teamFxCache.get(key);
  if (c && (Date.now() - c.ts) < 4 * 3600 * 1000) return c.data;
  const data = await getTeamLastFixtures(teamId, last);
  _teamFxCache.set(key, { data, ts: Date.now() });
  return data;
}

async function handleRachasFecha(chatId, dateStr, label) {
  await bot.sendMessage(chatId, `🔍 Obteniendo partidos ${label} (${dateStr})...`);

  // API returns fixtures by UTC date; we need to verify each fixture's Bogotá date
  // (e.g. a game at 7:30 PM Colombia = 12:30 AM UTC next day → wrong UTC bucket)
  const allRaw = await fetchFixturesByDate(dateStr).catch(() => []);

  const fixtures = allRaw
    .filter(f => {
      if (!RACHAS_FECHA_LEAGUES.has(f.league?.id)) return false;
      const fxBogotaDate = new Date(f.date)
        .toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
      return fxBogotaDate === dateStr;
    })
    .map(parseFixture);

  if (fixtures.length === 0) {
    return bot.sendMessage(chatId, `😔 No hay partidos ${label} en las ligas monitoreadas.`);
  }

  // Cap at 24 fixtures to keep API usage reasonable (~48 team calls)
  const topFixtures = fixtures.slice(0, 24);
  await bot.sendMessage(chatId,
    `⏳ *${topFixtures.length}* partidos ${label} encontrados. Calculando rachas de cada equipo...`,
    { parse_mode: 'Markdown' }
  );

  // Unique teams
  const teamMap = new Map();
  for (const f of topFixtures) {
    teamMap.set(f.homeId, f.homeTeam);
    teamMap.set(f.awayId, f.awayTeam);
  }
  const teamList = [...teamMap.entries()]; // [[id, name], ...]

  // Fetch fixtures for all teams in batches of 6 (parallel)
  const teamStreaks = new Map();
  const MIN_STREAK = 4;
  for (let i = 0; i < teamList.length; i += 6) {
    const batch = teamList.slice(i, i + 6);
    const results = await Promise.allSettled(batch.map(async ([tid]) => {
      const fx = await getTeamLastFixturesCached(tid, 12);
      const streaks = calcTeamStreaks(fx, tid);
      return { teamId: tid, streaks };
    }));
    for (const r of results) {
      if (r.status === 'fulfilled') {
        teamStreaks.set(r.value.teamId, r.value.streaks);
      }
    }
  }

  // Build output grouped by fixture
  const fixtureResults = [];
  for (const f of topFixtures) {
    const homeStr = teamStreaks.get(f.homeId) || {};
    const awayStr = teamStreaks.get(f.awayId) || {};

    const homeActive = Object.entries(homeStr)
      .filter(([, v]) => v.current >= MIN_STREAK)
      .sort(([, a], [, b]) => b.current - a.current)
      .slice(0, 4);
    const awayActive = Object.entries(awayStr)
      .filter(([, v]) => v.current >= MIN_STREAK)
      .sort(([, a], [, b]) => b.current - a.current)
      .slice(0, 4);

    if (homeActive.length > 0 || awayActive.length > 0) {
      fixtureResults.push({ f, homeActive, awayActive });
    }
  }

  if (fixtureResults.length === 0) {
    return bot.sendMessage(chatId,
      `📊 No hay equipos con rachas de ${MIN_STREAK}+ partidos consecutivos en los partidos ${label}.\n\nIntenta pedirlo con más ligas: *rachas La Liga*, *rachas Premier League*, etc.`,
      { parse_mode: 'Markdown' }
    );
  }

  // Sort: most combined streaks first
  fixtureResults.sort((a, b) =>
    (b.homeActive.length + b.awayActive.length) - (a.homeActive.length + a.awayActive.length)
  );

  // Emoji for streak length
  const streakEmoji = n => n >= 8 ? '🔥🔥' : n >= 6 ? '🔥' : '✅';

  let text = `🔥 *RACHAS ${label.toUpperCase()} — ${dateStr}*\n`;
  text += `📊 Mín. ${MIN_STREAK} partidos consecutivos | ${fixtureResults.length} partidos con rachas activas\n`;
  text += `━━━━━━━━━━━━━━━━━━━\n\n`;

  for (const { f, homeActive, awayActive } of fixtureResults) {
    const hour = formatHour(f.date);
    const league = LEAGUE_MAP[f.leagueId]?.name || 'Liga';
    text += `⚽ *${f.homeTeam} vs ${f.awayTeam}*\n`;
    text += `🏆 ${league} | ⏰ ${hour}\n`;
    if (homeActive.length > 0) {
      text += `🏠 ${f.homeTeam}:\n`;
      for (const [, v] of homeActive) {
        text += `   ${streakEmoji(v.current)} ${v.label} — *${v.current}* partidos\n`;
      }
    }
    if (awayActive.length > 0) {
      text += `✈️ ${f.awayTeam}:\n`;
      for (const [, v] of awayActive) {
        text += `   ${streakEmoji(v.current)} ${v.label} — *${v.current}* partidos\n`;
      }
    }
    text += `\n`;
  }

  text += `━━━━━━━━━━━━━━━━━━━\n`;
  text += `📊 Basado en últimos 12 partidos por equipo | Solo muestra rachas de ${MIN_STREAK}+`;
  return sendLong(chatId, text, { parse_mode: 'Markdown' });
}

// ─── Soccer Buddy / ZCode ────────────────────────────────────────────────────
//
// Estrategia: cookie-based auth (Google SSO no se puede automatizar en headless).
// El usuario extrae cookies de su browser 1 vez y las pega en Railway env ZCODE_COOKIES.
//
// Dos modos:
//   1. DESCUBRIMIENTO (/zcode-debug en Telegram) — intercepta red, saca screenshot,
//      vuelca HTML → nos dice exactamente qué API usa el site y cómo es el DOM.
//   2. PRODUCCIÓN — scrape tabla, acumula señales cada ZB_INTERVAL ms.
//
// Señales que se extraen (incluso de celdas bloqueadas):
//   - inValueBets: en qué listas aparece el partido (ht_over05, btts, over25, etc.)
//   - scorePred: predicción de resultado ("1:1", "1:2", …)
//   - btts, over15, over25, ht_over05, ht_over15, sh_over05: % si visible, null si bloqueado
// ─────────────────────────────────────────────────────────────────────────────

const puppeteer = require('puppeteer');

const ZB_URL      = 'https://zcodesystem.com/soccerbuddy/';
const ZB_INTERVAL = 12 * 60 * 1000;   // re-scrape cada 12 min
const _zbStore    = new Map();         // key: normHome+'|'+normAway → signals
let   _zbLastRun  = 0;
let   _zbApiEndpoints = [];            // endpoints descubiertos

// ── ZCode Dropping Odds & Line Reversals store ────────────────────────────────
const _zdoStore  = new Map(); // key: normHome+'|||'+normAway → { drop1, drop2, ... }
const _zlrStore  = new Map(); // key: normHome+'|||'+normAway → { reversal, ... }
let   _zdoLastRun = 0;
const ZDO_INTERVAL = 15 * 60 * 1000; // cada 15 min

// ── ZCode extra tools stores ──────────────────────────────────────────────────
const _tpStore   = new Map(); // Totals Predictor: normHome+'|||'+normAway → { predictedTotal, confidence }
const _prStore   = new Map(); // Power Rankings:   _zbNorm(team)           → { rating, rank, trend }
const _soStore   = new Map(); // Soccer Oscillator: _zbNorm(team)          → { oscillator, hot }
const _cbStore   = new Map(); // Contrarian Bets:   normHome+'|||'+normAway → { contrarianSide, publicPct }
const _evStore   = new Map(); // EV Tool:           normHome+'|||'+normAway → { rank, evScore }
let   _zetLastRun = 0;
const ZET_INTERVAL = 20 * 60 * 1000; // extra tools cada 20 min

// Construir cookie string desde ZCODE_COOKIES env
function _zdoCookieStr() {
  const raw = (process.env.ZCODE_COOKIES || '').trim();
  if (!raw) return '';
  try {
    const parsed = JSON.parse(raw);
    const arr = Array.isArray(parsed) ? parsed : (parsed.cookies || []);
    return arr.map(c => `${c.name}=${c.value}`).join('; ');
  } catch {
    return raw; // ya es string de cookies
  }
}

function _zdoHeaders() {
  return {
    'Cookie': _zdoCookieStr(),
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://zcodesystem.com/',
    'X-Requested-With': 'XMLHttpRequest',
    'Accept': 'application/json, text/javascript, */*',
  };
}

// Parsear HTML de tabla ZCode (Line Reversals / GamesTable) → array de señales por partido
// Extrae: equipos, % de dinero sharp, cuotas de apertura y cierre, y % de drop de cuota.
function _parseZcodeGameTable(html) {
  const signals = [];
  if (!html) return signals;

  const teamPctPattern = /([A-Za-zÀ-ÿ\s\-\.]{3,30}?)\s+(\d{1,3})%/g;

  // Dividir en bloques de partido (cada GamesTableRow = 1 partido)
  const gameBlocks = html.split(/class="GamesTableRow[^"]*"/).slice(1);

  for (const block of gameBlocks) {
    const text = block.replace(/<[^>]+>/g, ' ').replace(/&[a-z]+;/g, ' ').replace(/\s+/g, ' ').trim();

    // Equipos con sus % de dinero
    const pcts = [...text.matchAll(teamPctPattern)]
      .map(m => ({ team: m[1].trim(), pct: parseInt(m[2]) }))
      .filter(t => t.team.length > 2 && t.team.length < 35 && !/^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|\d)/i.test(t.team));

    // Todas las cuotas decimales del bloque (pueden ser open1, close1, open2, close2)
    const odds = [...text.matchAll(/\b(\d+\.\d{2})\b/g)].map(m => parseFloat(m[1]));

    if (pcts.length < 2 || odds.length < 2) continue;

    // Cuotas de apertura y cierre para cada equipo
    // Si hay ≥4 odds: [openTeam1, closeTeam1, openTeam2, closeTeam2]
    // Si hay 2 odds: current odds de cada equipo
    const open1  = odds.length >= 4 ? odds[0] : null;
    const close1 = odds.length >= 4 ? odds[1] : odds[0];
    const open2  = odds.length >= 4 ? odds[2] : null;
    const close2 = odds.length >= 4 ? odds[3] : odds[1];

    // % de drop: caída de cuota desde apertura → indica dinero entrando en ese equipo
    const drop1 = open1 && open1 > close1 ? +((open1 - close1) / open1 * 100).toFixed(1) : 0;
    const drop2 = open2 && open2 > close2 ? +((open2 - close2) / open2 * 100).toFixed(1) : 0;

    // Sharp side = equipo con más % de dinero de apostadores sharp (invertido al % público)
    const sharpSide = pcts[0].pct < pcts[1].pct ? pcts[1].team : pcts[0].team;

    signals.push({
      team1: pcts[0].team, pct1: pcts[0].pct, oddOpen1: open1, oddClose1: close1, drop1,
      team2: pcts[1].team, pct2: pcts[1].pct, oddOpen2: open2, oddClose2: close2, drop2,
      sharpSide,
    });
  }
  return signals;
}

// Helper: GET directo a un endpoint ZCode con cookies.
// Si el endpoint devuelve 404, primero carga la página padre (warm-up) y reintenta.
async function _zcodeGet(path, params = {}, options = {}) {
  const fullUrl  = path.startsWith('http') ? path : `https://zcodesystem.com${path}`;
  const referer  = options.referer || 'https://zcodesystem.com/';
  const headers  = { ..._zdoHeaders(), 'Referer': referer };
  const axCfg    = { headers, params, timeout: 18000, validateStatus: s => s < 500 };

  // Intento 1: GET directo
  let resp = await axios.get(fullUrl, axCfg);
  let body = resp.data;
  let bodyStr = typeof body === 'string' ? body : JSON.stringify(body);

  // Si la respuesta es 404 o parece una página de error, hacer warm-up de la página padre y reintentar
  const is404     = resp.status === 404 || bodyStr.includes('404 Not Found');
  const isError   = /sign.?in|log.?in|password|403 Forbidden|Access Denied/i.test(bodyStr.slice(0, 400));

  if ((is404 || isError) && referer !== 'https://zcodesystem.com/') {
    console.log(`🔍 ${path}: ${resp.status} → warm-up ${referer}...`);
    // Cargar la página padre para inicializar la sesión PHP
    await axios.get(referer, { headers: { ..._zdoHeaders(), 'Referer': 'https://zcodesystem.com/' }, timeout: 18000 })
      .catch(() => {});
    await new Promise(r => setTimeout(r, 1000));

    // Reintentar GET y luego POST
    for (const method of ['get', 'post']) {
      const r2 = method === 'get'
        ? await axios.get(fullUrl, axCfg).catch(() => null)
        : await axios.post(fullUrl, new URLSearchParams(params).toString(), {
            headers: { ...headers, 'Content-Type': 'application/x-www-form-urlencoded' },
            timeout: 18000, validateStatus: s => s < 500,
          }).catch(() => null);

      if (!r2) continue;
      const b2 = r2.data;
      const s2 = typeof b2 === 'string' ? b2 : JSON.stringify(b2);
      console.log(`🔍 ${path}: ${method.toUpperCase()} retry [${r2.status}] ${s2.length}b → ${s2.slice(0,200).replace(/\s+/g,' ')}`);
      if (r2.status < 400 && s2.length > 100 && !s2.includes('404 Not Found')) return b2;
    }
    return null;
  }

  return body;
}

// Parsear HTML de EV Tool (tabla MainTable) → Map de partido → { rank, evScore }
function _parseEvToolHtml(html) {
  const results = new Map();
  if (!html || !html.includes('MainTable')) return results;
  const rows = html.split(/<tr[\s>]/i).slice(1);
  let rank = 0;
  for (const row of rows) {
    if (/<th/i.test(row)) continue;
    const text = row.replace(/<[^>]+>/g, ' ').replace(/&[a-z]+;/g, ' ').replace(/\s+/g, ' ').trim();
    if (text.length < 10) continue;
    const vsMatch = text.match(/([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-\.]{2,25})\s+(?:@|vs\.?)\s+([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-\.]{2,25})/i);
    if (!vsMatch) continue;
    const evMatch = text.match(/([+-]?\d+(?:\.\d+)?)/);
    rank++;
    const home = vsMatch[1].trim();
    const away = vsMatch[2].trim();
    results.set(_zbNorm(home) + '|||' + _zbNorm(away), { home, away, rank, evScore: evMatch ? parseFloat(evMatch[1]) : null });
  }
  return results;
}

async function runZcodeMarketScrape() {
  if (Date.now() - _zdoLastRun < ZDO_INTERVAL) return;
  if (!process.env.ZCODE_COOKIES) return;

  // Los endpoints linemovinggameslistn1.php y gameslist.php requieren una sesión PHP
  // inicializada por JavaScript del browser. Se usan Puppeteer + interceptación de red.
  let browser;
  try {
    browser = await _zbBrowser();
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');
    await _zbSetCookies(page);

    // ── 1. Line Reversals + Dropping Odds — extraer del DOM ──────────────────────
    try {
      await page.goto('https://zcodesystem.com/line_reversals', { waitUntil: 'networkidle2', timeout: 40000 });
      // Esperar a que el JS del site cargue los datos en el DOM
      await new Promise(r => setTimeout(r, 3000));

      const lrHtml = await page.evaluate(() => {
        const t = document.querySelector('.GamesTable, [class*="GamesTable"]');
        return t ? t.outerHTML : '';
      }).catch(() => '');

      if (lrHtml && lrHtml.includes('GamesTable')) {
        console.log(`🔍 LR DOM [${lrHtml.length}b] preview: ${lrHtml.slice(0,400).replace(/\s+/g,' ')}`);
        const signals = _parseZcodeGameTable(lrHtml);
        _zlrStore.clear();
        _zdoStore.clear();
        for (const s of signals) {
          _zlrStore.set(_zbNorm(s.team1) + '|||' + _zbNorm(s.team2), s);
          const addDrop = (team, drop, oddBefore, oddAfter, pct) => {
            if (!team || drop < 2) return;
            const k = _zbNorm(team) + '|||';
            const ex = _zdoStore.get(k);
            if (!ex || drop > ex.drop) _zdoStore.set(k, { team, drop, oddBefore, oddAfter, publicPct: pct ?? 0 });
          };
          addDrop(s.team1, s.drop1, s.oddOpen1, s.oddClose1, s.pct1);
          addDrop(s.team2, s.drop2, s.oddOpen2, s.oddClose2, s.pct2);
        }
        console.log(`🔄 Line Reversals: ${signals.length} partidos | 📉 Dropping Odds: ${_zdoStore.size} equipos`);
      } else {
        const preview = await page.evaluate(() => document.body?.innerText?.slice(0,200) || '').catch(() => '');
        console.warn(`Line Reversals: sin GamesTable. Page: ${preview.replace(/\s+/g,' ')}`);
      }
    } catch (e) { console.warn('Line Reversals nav err:', e.message); }

    // ── 2. EV Tool — extraer del DOM ─────────────────────────────────────────────
    try {
      await page.goto('https://zcodesystem.com/evtool/', { waitUntil: 'networkidle2', timeout: 40000 });
      await new Promise(r => setTimeout(r, 3000));

      const evHtml = await page.evaluate(() => {
        const t = document.querySelector('#MainTable, table.DataTableGr, [class*="DataTable"]');
        return t ? t.outerHTML : '';
      }).catch(() => '');

      if (evHtml && (evHtml.includes('MainTable') || evHtml.includes('<tr'))) {
        const parsed = _parseEvToolHtml(evHtml.includes('MainTable') ? evHtml : `<table id="MainTable">${evHtml}</table>`);
        _evStore.clear();
        for (const [k, v] of parsed) _evStore.set(k, { ...v, _ts: Date.now() });
        console.log(`📈 EV Tool: ${_evStore.size} partidos con EV score`);
      } else {
        const preview = await page.evaluate(() => document.body?.innerText?.slice(0,200) || '').catch(() => '');
        console.warn(`EV Tool: sin MainTable. Page: ${preview.replace(/\s+/g,' ')}`);
      }
    } catch (e) { console.warn('EV Tool nav err:', e.message); }

    await page.close();
    _zdoLastRun = Date.now();
  } catch (e) {
    console.warn('runZcodeMarketScrape err:', e.message);
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}

// ── ZCode Extra Tools: Power Rankings (axios) + Totals Predictor, Soccer Oscillator, Contrarian (Puppeteer) ──
async function runZcodeExtraTools() {
  if (Date.now() - _zetLastRun < ZET_INTERVAL) return;
  if (!process.env.ZCODE_COOKIES) return;

  // ── Power Rankings — axios directo a /powerrankings.php ──────────────────────
  try {
    const html = await _zcodeGet('/powerrankings.php', {}, { referer: 'https://zcodesystem.com/power_rankings.php' });
    const prHtml = typeof html === 'string' ? html : (typeof html === 'object' ? JSON.stringify(html) : '');

    if (prHtml && prHtml.includes('Power Ranking')) {
      // Parsear filas de tabla: Rank | Team | Rating | Trend
      const rows = prHtml.split(/<tr[\s>]/i).slice(1);
      _prStore.clear();
      let prCount = 0;
      for (const row of rows) {
        if (/<th/i.test(row)) continue;
        const text = row.replace(/<[^>]+>/g, ' ').replace(/&[a-z]+;/g, ' ').replace(/\s+/g, ' ').trim();
        if (text.length < 5) continue;

        // Rating: número entre 0 y 200 (típicamente 50-150 en ZCode)
        const ratingMatch = text.match(/\b(\d{1,3}(?:\.\d+)?)\b/g);
        const ratings = ratingMatch ? ratingMatch.map(Number).filter(n => n >= 30 && n <= 200) : [];
        if (ratings.length === 0) continue;
        const rating = ratings[0];

        // Equipo: primer texto largo que no sea números
        const teamMatch = text.match(/([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-\.]{2,30})/);
        if (!teamMatch) continue;
        const team = teamMatch[1].trim();
        if (team.length < 3 || /^(rank|team|power|rating|score|pts|pos)/i.test(team)) continue;

        const trend = /\+\d|▲|↑/.test(text) ? 'up' : /-\d|▼|↓/.test(text) ? 'down' : 'neutral';
        const key = _zbNorm(team);
        if (key.length < 2) continue;
        _prStore.set(key, { team, rating, rank: prCount + 1, trend, _ts: Date.now() });
        prCount++;
      }
      console.log(`📊 Power Rankings (API directa): ${prCount} equipos`);
    } else {
      console.warn('Power Rankings: sin datos (respuesta no contiene "Power Ranking")');
    }
  } catch (e) { console.warn('Power Rankings API err:', e.message); }

  let browser;
  try {
    browser = await _zbBrowser();
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');
    await _zbSetCookies(page);

    // ── 1. Totals Predictor ────────────────────────────────────────────────────
    try {
      await page.goto('https://zcodesystem.com/totals_predictor', { waitUntil: 'networkidle2', timeout: 40000 });
      await new Promise(r => setTimeout(r, 1200));

      const tpData = await page.evaluate(() => {
        const results = [];
        // Buscar filas con equipos y predicciones de total
        const rows = document.querySelectorAll('tr, .game-row, [class*="Row"], [class*="game"]');
        rows.forEach(row => {
          const text = (row.innerText || '').replace(/\s+/g, ' ').trim();
          if (text.length < 10) return;

          // Buscar patrón "Team1 vs Team2" o "Team1 @ Team2"
          const vsMatch = text.match(/^(.{3,25})\s+(?:vs?\.?|@|–)\s+(.{3,25})/i);
          if (!vsMatch) return;

          const home = vsMatch[1].trim();
          const away = vsMatch[2].replace(/\s+\d.*/, '').trim();

          // Buscar número de total predicho (ej: 2.5, 3.0)
          const totalMatch = text.match(/(?:total[:\s]+)?(\d+\.\d+)(?:\s*goals?)?/i);
          const totalVal = totalMatch ? parseFloat(totalMatch[1]) : null;
          if (!totalVal || totalVal < 0.5 || totalVal > 8) return;

          // Buscar confianza (%)
          const confMatch = text.match(/(\d{2,3})%/);
          const confidence = confMatch ? parseInt(confMatch[1]) : null;

          results.push({ home, away, predictedTotal: totalVal, confidence });
        });

        // Fallback: buscar elementos con data attributes
        document.querySelectorAll('[data-home],[data-away],[data-total]').forEach(el => {
          const home  = el.getAttribute('data-home') || el.getAttribute('data-team1') || '';
          const away  = el.getAttribute('data-away') || el.getAttribute('data-team2') || '';
          const total = parseFloat(el.getAttribute('data-total') || el.getAttribute('data-prediction') || '');
          if (home && away && !isNaN(total)) {
            results.push({ home: home.trim(), away: away.trim(), predictedTotal: total, confidence: null });
          }
        });

        return results;
      });

      let tpCount = 0;
      for (const entry of tpData) {
        if (!entry.home || !entry.away || !entry.predictedTotal) continue;
        if (/basketball|nba|nhl|mlb|nfl/i.test(entry.home + entry.away)) continue;
        const key = _zbNorm(entry.home) + '|||' + _zbNorm(entry.away);
        _tpStore.set(key, {
          home: entry.home, away: entry.away,
          predictedTotal: entry.predictedTotal,
          confidence: entry.confidence,
          _ts: Date.now(),
        });
        tpCount++;
      }
      console.log(`🎯 Totals Predictor: ${tpCount} partidos con predicción de total`);
    } catch (e) { console.warn('Totals Predictor err:', e.message); }

    // ── 2. Power Rankings ─────────────────────────────────────────────────────
    try {
      await page.goto('https://zcodesystem.com/power_rankings.php', { waitUntil: 'networkidle2', timeout: 40000 });
      await new Promise(r => setTimeout(r, 1000));

      const prData = await page.evaluate(() => {
        const results = [];
        // Tabla con rankings: posición, equipo, rating
        const rows = document.querySelectorAll('tr, [class*="rank"], [class*="team-row"]');
        rows.forEach((row, idx) => {
          const cells = row.querySelectorAll('td, th');
          if (cells.length < 2) return;
          const text  = (row.innerText || '').replace(/\s+/g, ' ').trim();

          // Buscar rating numérico (ej: 85.3, 72, etc.)
          const ratingMatch = text.match(/(\d{2,3}(?:\.\d+)?)\s*(?:pts?|points?|rating|power)?/i);
          if (!ratingMatch) return;
          const rating = parseFloat(ratingMatch[1]);
          if (rating < 1 || rating > 200) return;

          // Primer texto sustancial = nombre del equipo
          const teamMatch = text.match(/^(\d+\.?\s+)?([A-Za-zÀ-ÿ\s\-\.]{3,30})/);
          if (!teamMatch) return;
          const team = teamMatch[2].replace(/\d+\.?\s*$/, '').trim();
          if (team.length < 3) return;

          // Tendencia (flecha arriba/abajo o +/-)
          const trend = text.includes('▲') || text.includes('↑') || /\+\d/.test(text) ? 'up' :
                       text.includes('▼') || text.includes('↓') || /-\d/.test(text)  ? 'down' : 'neutral';

          results.push({ team, rating, rank: idx + 1, trend });
        });

        // También buscar con data attributes
        document.querySelectorAll('[data-team],[data-rating],[data-rank]').forEach(el => {
          const team   = el.getAttribute('data-team') || el.getAttribute('data-name') || '';
          const rating = parseFloat(el.getAttribute('data-rating') || el.getAttribute('data-power') || '');
          const rank   = parseInt(el.getAttribute('data-rank') || el.getAttribute('data-position') || '0');
          if (team && !isNaN(rating)) {
            results.push({ team: team.trim(), rating, rank: rank || 999, trend: 'neutral' });
          }
        });

        return results;
      });

      let prCount = 0;
      for (const entry of prData) {
        if (!entry.team || !entry.rating) continue;
        const key = _zbNorm(entry.team);
        if (!key || key.length < 2) continue;
        _prStore.set(key, { team: entry.team, rating: entry.rating, rank: entry.rank, trend: entry.trend, _ts: Date.now() });
        prCount++;
      }
      console.log(`📊 Power Rankings: ${prCount} equipos con rating`);
    } catch (e) { console.warn('Power Rankings err:', e.message); }

    // ── 3. Soccer Oscillator ──────────────────────────────────────────────────
    try {
      await page.goto('https://zcodesystem.com/soccer_oscillator/', { waitUntil: 'networkidle2', timeout: 40000 });
      await new Promise(r => setTimeout(r, 1000));

      const soData = await page.evaluate(() => {
        const results = [];
        const rows = document.querySelectorAll('tr, [class*="osc"], [class*="team-row"], [class*="Row"]');
        rows.forEach(row => {
          const text = (row.innerText || '').replace(/\s+/g, ' ').trim();
          if (text.length < 5) return;

          // Nombre de equipo
          const teamMatch = text.match(/^([A-Za-zÀ-ÿ\s\-\.]{3,30})/);
          if (!teamMatch) return;
          const team = teamMatch[1].trim();

          // Valor del oscilador (puede ser positivo o negativo)
          const oscMatch = text.match(/([+-]?\d+(?:\.\d+)?)\s*(?:osc|pts?|oscillator)?/i);
          const oscillator = oscMatch ? parseFloat(oscMatch[1]) : null;
          if (oscillator === null) return;

          // HOT = positivo / COLD = negativo
          const hot = oscillator > 0 ||
                      /\bhot\b|🔥|⬆️|overperform/i.test(text);
          const cold = oscillator < 0 ||
                       /\bcold\b|❄️|⬇️|underperform/i.test(text);

          results.push({ team, oscillator, hot, cold });
        });

        // También buscar con clases CSS de colores (verde=hot, rojo=cold)
        document.querySelectorAll('[class*="hot"],[class*="fire"],[class*="green"]').forEach(el => {
          const text = (el.innerText || '').replace(/\s+/g, ' ').trim();
          const teamMatch = text.match(/^([A-Za-zÀ-ÿ\s\-\.]{3,30})/);
          if (!teamMatch) return;
          results.push({ team: teamMatch[1].trim(), oscillator: null, hot: true, cold: false });
        });
        document.querySelectorAll('[class*="cold"],[class*="ice"],[class*="red"]').forEach(el => {
          const text = (el.innerText || '').replace(/\s+/g, ' ').trim();
          const teamMatch = text.match(/^([A-Za-zÀ-ÿ\s\-\.]{3,30})/);
          if (!teamMatch) return;
          results.push({ team: teamMatch[1].trim(), oscillator: null, hot: false, cold: true });
        });

        return results;
      });

      let soCount = 0;
      for (const entry of soData) {
        if (!entry.team) continue;
        const key = _zbNorm(entry.team);
        if (!key || key.length < 2) continue;
        _soStore.set(key, { team: entry.team, oscillator: entry.oscillator, hot: entry.hot, cold: entry.cold, _ts: Date.now() });
        soCount++;
      }
      console.log(`🌊 Soccer Oscillator: ${soCount} equipos con señal de momentum`);
    } catch (e) { console.warn('Soccer Oscillator err:', e.message); }

    // ── 4. Contrarian Bets ────────────────────────────────────────────────────
    try {
      await page.goto('https://zcodesystem.com/vipclub/contrarianbets.php', { waitUntil: 'networkidle2', timeout: 40000 });
      await new Promise(r => setTimeout(r, 1000));

      const cbData = await page.evaluate(() => {
        const results = [];
        const rows = document.querySelectorAll('tr, .GamesTableRow, [class*="Row"], [class*="game"]');
        rows.forEach(row => {
          const text = (row.innerText || '').replace(/\s+/g, ' ').trim();
          if (text.length < 10) return;

          // Partido: Team1 vs Team2
          const vsMatch = text.match(/^(.{3,25})\s+(?:vs?\.?|@|–)\s+(.{3,25})/i);
          if (!vsMatch) return;
          const home = vsMatch[1].trim();
          const away = vsMatch[2].replace(/\s+\d.*/, '').trim();

          // % del público en el lado "popular"
          const pctMatches = [...text.matchAll(/(\d{1,3})%/g)].map(m => parseInt(m[1]));
          if (pctMatches.length === 0) return;
          const publicPct = Math.max(...pctMatches); // el lado con más público = lado a fading

          // El lado contrarian = el lado opuesto al que tiene más público
          // Buscar si hay marcas de "bet against" o "contrarian" en el texto
          const contrarianSide = publicPct > 60
            ? (text.indexOf('%') < text.length / 2 ? 'away' : 'home') // heurística
            : null;

          if (publicPct >= 60 && contrarianSide) {
            results.push({ home, away, publicPct, contrarianSide });
          }
        });

        // Data attributes
        document.querySelectorAll('[data-contrarian],[data-fade],[data-public-pct]').forEach(el => {
          const home = el.getAttribute('data-home') || '';
          const away = el.getAttribute('data-away') || '';
          const publicPct = parseInt(el.getAttribute('data-public-pct') || el.getAttribute('data-public') || '0');
          const side = el.getAttribute('data-contrarian') || el.getAttribute('data-fade') || '';
          if (home && away && publicPct >= 60) {
            results.push({ home: home.trim(), away: away.trim(), publicPct, contrarianSide: side || 'unknown' });
          }
        });

        return results;
      });

      let cbCount = 0;
      for (const entry of cbData) {
        if (!entry.home || !entry.away) continue;
        if (/basketball|nba|nhl|mlb|nfl/i.test(entry.home + entry.away)) continue;
        const key = _zbNorm(entry.home) + '|||' + _zbNorm(entry.away);
        _cbStore.set(key, {
          home: entry.home, away: entry.away,
          publicPct: entry.publicPct,
          contrarianSide: entry.contrarianSide,
          _ts: Date.now(),
        });
        cbCount++;
      }
      console.log(`🔀 Contrarian Bets: ${cbCount} partidos con señal contrarian`);
    } catch (e) { console.warn('Contrarian Bets err:', e.message); }

    await page.close();
    _zetLastRun = Date.now();
  } catch (e) {
    console.warn('runZcodeExtraTools err:', e.message);
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
}

// Obtener señales de herramientas extra para un partido
function getZcodeExtraSignals(homeTeam, awayTeam) {
  const hNorm = _zbNorm(homeTeam);
  const aNorm = _zbNorm(awayTeam);
  const signals = {};

  // Totals Predictor
  for (const [key, data] of _tpStore) {
    const [k1, k2] = key.split('|||');
    if ((_zbMatch(hNorm, k1) && _zbMatch(aNorm, k2)) ||
        (_zbMatch(hNorm, k2) && _zbMatch(aNorm, k1))) {
      signals.totalsPredictor = { predictedTotal: data.predictedTotal, confidence: data.confidence };
      break;
    }
  }

  // Power Rankings (buscar por equipo individual)
  for (const [key, data] of _prStore) {
    if (_zbMatch(hNorm, key)) signals.powerHome = data.rating;
    if (_zbMatch(aNorm, key)) signals.powerAway = data.rating;
  }

  // Soccer Oscillator
  for (const [key, data] of _soStore) {
    if (_zbMatch(hNorm, key)) signals.oscHome = { hot: data.hot, cold: data.cold, val: data.oscillator };
    if (_zbMatch(aNorm, key)) signals.oscAway = { hot: data.hot, cold: data.cold, val: data.oscillator };
  }

  // Contrarian Bets
  for (const [key, data] of _cbStore) {
    const [k1, k2] = key.split('|||');
    if ((_zbMatch(hNorm, k1) && _zbMatch(aNorm, k2)) ||
        (_zbMatch(hNorm, k2) && _zbMatch(aNorm, k1))) {
      signals.contrarian = { side: data.contrarianSide, publicPct: data.publicPct };
      break;
    }
  }

  // EV Tool — partido aparece en lista de partidos con alto EV
  for (const [key, data] of _evStore) {
    const [k1, k2] = key.split('|||');
    if ((_zbMatch(hNorm, k1) && _zbMatch(aNorm, k2)) ||
        (_zbMatch(hNorm, k2) && _zbMatch(aNorm, k1))) {
      signals.evTool = { rank: data.rank, evScore: data.evScore };
      break;
    }
  }

  return Object.keys(signals).length > 0 ? signals : null;
}

// Función para obtener señales de mercado para un partido específico
function getZcodeMarketSignals(homeTeam, awayTeam) {
  const signals = {};
  const hNorm = _zbNorm(homeTeam);
  const aNorm = _zbNorm(awayTeam);

  // Buscar en Line Reversals (key = norm1+'|||'+norm2)
  for (const [key, data] of _zlrStore) {
    const [k1, k2] = key.split('|||');
    if ((_zbMatch(hNorm, k1) && _zbMatch(aNorm, k2)) ||
        (_zbMatch(hNorm, k2) && _zbMatch(aNorm, k1))) {
      // Detectar qué lado tiene el dinero sharp (mayor %)
      const localIsSide1 = _zbMatch(hNorm, k1);
      signals.lineReversal = {
        pctLocal:     localIsSide1 ? data.pct1 : data.pct2,
        pctVisitante: localIsSide1 ? data.pct2 : data.pct1,
        sharpSide:    data.sharpSide,
        // Si el sharp está en el local o visitante
        sharpEnLocal: _zbMatch(hNorm, _zbNorm(data.sharpSide || '')),
      };
      break;
    }
  }

  // Buscar en Dropping Odds (key = normTeam+'|||')
  for (const [key, data] of _zdoStore) {
    const teamNorm = key.replace('|||', '');
    if (_zbMatch(hNorm, teamNorm)) signals.droppingOddsLocal = data;
    if (_zbMatch(aNorm, teamNorm)) signals.droppingOddsVisitante = data;
  }

  return Object.keys(signals).length > 0 ? signals : null;
}
// ─────────────────────────────────────────────────────────────────────────────

// Lanzar browser con flags para Railway (sin GPU, sin sandbox)
async function _zbBrowser() {
  return puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--single-process',
      '--no-zygote',
    ],
  });
}

function _zbNorm(name) {
  return (name || '').toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/\b(fc|cf|sc|ac|rc|cd|sd|de|del|la|los|el|us|ss|sv|bv|rj|sp|ad|rd|ca|sa)\b/g, '')
    .replace(/[^a-z0-9]/g, ' ').replace(/\s+/g, ' ').trim();
}

function _zbKey(home, away) {
  return `${_zbNorm(home)}|||${_zbNorm(away)}`;
}

function _zbMatch(a, b) {
  const na = _zbNorm(a), nb = _zbNorm(b);
  if (na === nb) return true;
  if (na.length >= 4 && (nb.includes(na) || na.includes(nb))) return true;
  const sig = s => s.split(' ').filter(w => w.length >= 4);
  return sig(na).some(w => sig(nb).includes(w));
}

// Inyecta las cookies guardadas en ZCODE_COOKIES env var.
// Acepta 4 formatos:
//   1. Cookie-Editor export: {"url":"...","cookies":[{name,value,domain,...}]}
//   2. JSON array Puppeteer: [{"name":"key","value":"val","domain":"..."}]
//   3. JSON object plano:    {"key":"val","key2":"val2"}
//   4. Cookie string:        "key=val; key2=val2"  (del header Cookie: del DevTools)
async function _zbSetCookies(page) {
  const raw = (process.env.ZCODE_COOKIES || '').trim();
  if (!raw) return false;

  // Mapeo sameSite de extensión → Puppeteer
  const sameSiteMap = { lax: 'Lax', strict: 'Strict', no_restriction: 'None', none: 'None' };

  // Convierte un cookie de extensión (Cookie-Editor) a formato Puppeteer
  const extToPuppeteer = c => {
    const out = { name: c.name, value: String(c.value), domain: c.domain || 'zcodesystem.com', path: c.path || '/' };
    if (c.expirationDate) out.expires = Math.floor(c.expirationDate);
    if (c.httpOnly)       out.httpOnly = true;
    if (c.secure)         out.secure   = true;
    const ss = sameSiteMap[(c.sameSite || '').toLowerCase()];
    if (ss) out.sameSite = ss;
    return out;
  };

  let cookieObjects = [];

  try {
    const parsed = JSON.parse(raw);

    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed) && Array.isArray(parsed.cookies)) {
      // Formato 1: Cookie-Editor export {url, cookies:[...]}
      cookieObjects = parsed.cookies.filter(c => c.name && c.value !== undefined).map(extToPuppeteer);

    } else if (Array.isArray(parsed)) {
      if (parsed[0] && parsed[0].domain !== undefined) {
        // Formato 1b: array de Cookie-Editor sin wrapper
        cookieObjects = parsed.filter(c => c.name).map(extToPuppeteer);
      } else {
        // Formato 2: array Puppeteer nativo
        cookieObjects = parsed.filter(c => c.name && c.value);
      }
    } else if (typeof parsed === 'object') {
      // Formato 3: objeto plano {name: value}
      cookieObjects = Object.entries(parsed).map(([name, value]) => ({
        name, value: String(value), domain: 'zcodesystem.com', path: '/',
      }));
    }
  } catch (_) {
    // Formato 4: "key=val; key2=val2" (header Cookie del DevTools)
    const str = raw.replace(/^Cookie:\s*/i, '');
    cookieObjects = str.split(';').map(part => {
      const eq = part.indexOf('=');
      if (eq === -1) return null;
      return { name: part.slice(0, eq).trim(), value: part.slice(eq + 1).trim(), domain: 'zcodesystem.com', path: '/' };
    }).filter(Boolean);
  }

  if (cookieObjects.length === 0) {
    console.warn('ZCODE_COOKIES: no se encontraron cookies válidas en el valor configurado');
    return false;
  }

  await page.setCookie(...cookieObjects);
  console.log(`🍪 ZCode: ${cookieObjects.length} cookies inyectadas (PHPSESSID: ${cookieObjects.find(c => c.name === 'PHPSESSID')?.value?.slice(0, 8)}...)`);
  return true;
}

// ── MODO DESCUBRIMIENTO ──────────────────────────────────────────────────────
// Visita el site, intercepta XHR/fetch, toma screenshot y devuelve toda la info.
// Solo se llama desde /zcode-debug (admin) para que podamos ver cómo está hecho.
async function zbDiscover(chatId) {
  await bot.sendMessage(chatId, '🔍 Iniciando descubrimiento ZCode Soccer Buddy...');
  let browser;
  try {
    browser = await _zbBrowser();
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');

    // Interceptar llamadas de red para encontrar el endpoint de datos
    const apiCalls = [];
    const soccerGameIds = new Map(); // game_id → { home, away, scores, minute }
    page.on('response', async (res) => {
      const url = res.url();
      const ct  = res.headers()['content-type'] || '';
      if ((ct.includes('json') || url.includes('/api/') || url.includes('predict') || url.includes('soccer'))
          && !url.includes('google') && !url.includes('analytics')) {
        const body = await res.text().catch(() => '');
        apiCalls.push({ url, status: res.status(), bodyPreview: body.slice(0, 300) });

        // Detectar game_notification con partidos de fútbol en vivo
        if (body.includes('game_notification') && body.includes('SOCCER')) {
          try {
            const parsed = JSON.parse(body);
            const items = parsed?.data?.items || parsed?.items || [];
            for (const item of items) {
              if (item.sport_name === 'SOCCER' && item.game_id) {
                // Extraer teams del html o del block_title
                const titleMatch = (item.block_title || '').match(/\n(.+?)\s+-vs-\s+(.+?)\n/);
                const home = titleMatch ? titleMatch[1].trim() : '?';
                const away = titleMatch ? titleMatch[2].trim() : '?';
                const minute = item.from_game_start ? Math.floor(item.from_game_start / 60) : null;
                soccerGameIds.set(item.game_id, {
                  home, away, minute,
                  score: `${item.score1}:${item.score2}`,
                  period: item.period_number,
                  status: item.status,
                });
              }
            }
          } catch (_) {}
        }
      }
    });

    const hasCookies = await _zbSetCookies(page);
    await bot.sendMessage(chatId, `🍪 Cookies: ${hasCookies ? 'cargadas' : 'NO configuradas (ZCODE_COOKIES vacío)'}`);

    await page.goto(ZB_URL, { waitUntil: 'networkidle2', timeout: 40000 }).catch(e => {
      console.warn('ZCode navigate warn:', e.message);
    });

    // Esperar carga dinámica
    await new Promise(r => setTimeout(r, 1000));

    // Screenshot
    const screenshotBuf = await page.screenshot({ fullPage: false, type: 'png' });
    await bot.sendPhoto(chatId, screenshotBuf, { caption: '📸 Screenshot del site' });

    // Título + URL actual
    const title   = await page.title();
    const current = page.url();
    await bot.sendMessage(chatId, `📄 Título: ${title}\n🔗 URL: ${current}`);

    // API calls encontradas
    if (apiCalls.length > 0) {
      _zbApiEndpoints = apiCalls.map(c => c.url);
      let msg = `🔌 *${apiCalls.length} llamadas de red detectadas:*\n`;
      for (const c of apiCalls.slice(0, 8)) {
        msg += `\n\`${c.url.slice(0, 80)}\`\nStatus ${c.status} | ${c.bodyPreview.slice(0, 120)}\n`;
      }
      await sendLong(chatId, msg, { parse_mode: 'Markdown' }).catch(() =>
        bot.sendMessage(chatId, `API calls: ${apiCalls.map(c => c.url).join('\n')}`)
      );
    } else {
      await bot.sendMessage(chatId, '⚠️ No se detectaron llamadas JSON/API — puede ser que requiera login.');
    }

    // Partidos de fútbol en vivo detectados via game_notification
    if (soccerGameIds.size > 0) {
      let liveMsg = `⚽ *${soccerGameIds.size} partidos SOCCER en vivo detectados:*\n`;
      for (const [gid, g] of [...soccerGameIds.entries()].slice(0, 10)) {
        liveMsg += `\n🆔 \`${gid}\` | ${g.home} vs ${g.away} | ${g.score} (P${g.period}, min ${g.minute ?? '?'})`;
      }
      await sendLong(chatId, liveMsg, { parse_mode: 'Markdown' }).catch(() =>
        bot.sendMessage(chatId, `Live soccer game IDs: ${[...soccerGameIds.keys()].join(', ')}`)
      );
      // Guardar para análisis posterior (¿hay endpoint /predictions?game_id=X?)
      console.log('ZCode live soccer game IDs:', Object.fromEntries(soccerGameIds));
    } else {
      await bot.sendMessage(chatId, '⚽ No se detectaron game_notification de SOCCER — puede ser que no haya partidos en vivo ahora.');
    }

    // Volcado del HTML de tablas
    const tableText = await page.evaluate(() => {
      const tables = document.querySelectorAll('table');
      return Array.from(tables).map((t, i) =>
        `TABLE ${i}: ${t.id || t.className}\n` + t.innerText.slice(0, 600)
      ).join('\n\n---\n\n');
    });

    if (tableText.trim().length > 10) {
      await sendLong(chatId, '```\n' + tableText.slice(0, 3000) + '\n```', { parse_mode: 'Markdown' })
        .catch(() => bot.sendMessage(chatId, 'Tablas encontradas (ver logs)'));
      console.log('ZCode tables:\n', tableText);
    } else {
      // Sin tablas — mostrar todo el texto visible
      const bodyText = await page.evaluate(() => document.body.innerText.slice(0, 2000));
      await bot.sendMessage(chatId, `📝 Texto visible:\n${bodyText}`);
    }

  } catch (e) {
    await bot.sendMessage(chatId, `❌ Error descubrimiento: ${e.message}`);
    console.error('zbDiscover error:', e);
  } finally {
    if (browser) await browser.close();
  }
}

// ── MODO PRODUCCIÓN ──────────────────────────────────────────────────────────
// Selectores confirmados con el HTML real del site (ver console dump del usuario):
//
//   td.date                    → fecha/hora
//   td.game .league            → "Estonia Esiliiga"
//   td.game .teams             → "Viimsi JK vs Tallinna Kalev"
//   td.game .lines[title]      → "1X0" (resultado predicho más probable)
//   td.probability span        → número entero (77 = 77%)
//   td.tc.score:not(.rscore)   → predicción de score ("2:1")
//   td.tc.score.rscore         → resultado real (cuando ya jugó)
//
// Orden de columnas td.probability (izq → der según screenshots):
//   [0] draw        [1] over15   [2] over25   [3] btts
//   [4] ht_over05   [5] ht_over15  [6] sh_over05  [7] sh_over15
//
// Orden de columnas td.tc.score:not(.rscore):
//   [0] total_score_pred   [1] ht_score_pred
//
// Sección "Value Bets" (tabla separada arriba) — solo fútbol con ht_over05 visible
// ─────────────────────────────────────────────────────────────────────────────
async function zbScrapeOnce() {
  if (!process.env.ZCODE_COOKIES) return;

  let browser;
  let scrapedCount = 0;
  try {
    browser = await _zbBrowser();
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');
    await _zbSetCookies(page);

    await page.goto(ZB_URL, { waitUntil: 'networkidle2', timeout: 40000 }).catch(() => {});
    // Esperar que el JS del site cargue las predicciones (polling interno buddy.js)
    await new Promise(r => setTimeout(r, 1000));

    const raw = await page.evaluate(() => {
      const PROB_ORDER = ['draw','over15','over25','btts','ht_over05','ht_over15','sh_over05','sh_over15','over35','home_win','away_win'];

      // Detecta si una celda está bloqueada (requiere upgrade)
      const isLocked = td =>
        td.querySelector('[class*="lock"],[class*="blur"],[class*="premium"],[class*="upgrade"]') ||
        /unlock|upgrade|subscribe/i.test(td.textContent);

      // Extrae el número de un td.probability
      const probVal = td => {
        if (!td || isLocked(td)) return null;
        const span = td.querySelector('span');
        if (!span) return null;
        const n = parseInt(span.textContent, 10);
        return isNaN(n) ? null : n;
      };

      // Extrae texto de un td.tc.score (predicción o resultado)
      const scoreVal = td => {
        if (!td || isLocked(td)) return null;
        // Limpiar texto: quitar subelemento .goal si hay gol marcado
        const clone = td.cloneNode(true);
        clone.querySelectorAll('.goal,.updated').forEach(el => el.remove());
        const txt = clone.textContent.trim().replace(/\s+/g, '');
        return /^\d+:\d+$/.test(txt) ? txt : null;
      };

      const results = [];

      // Iterar todas las filas de tabla que tengan td.game
      document.querySelectorAll('tr:has(td.game)').forEach(row => {
        const gameCell = row.querySelector('td.game');
        if (!gameCell) return;

        // Solo fútbol — filtrar por sport si hay atributo data-sport o clase
        // Si no hay filtro, tomamos todo y dejamos que el normalizador descarte no-fútbol
        const teamsEl  = gameCell.querySelector('.teams');
        const leagueEl = gameCell.querySelector('.league');
        if (!teamsEl) return;

        const teamsText = teamsEl.textContent.trim();
        const vsIdx = teamsText.indexOf(' vs ');
        if (vsIdx === -1) return;

        const home   = teamsText.slice(0, vsIdx).trim();
        const away   = teamsText.slice(vsIdx + 4).trim();
        const league = leagueEl ? leagueEl.textContent.trim() : '';

        // game_id (si la fila o la tabla tienen data-game-id — útil para cruzar con live scores)
        const gameId = row.getAttribute('data-game-id') ||
                       row.closest('[data-game-id]')?.getAttribute('data-game-id') ||
                       gameCell.getAttribute('data-game-id') || null;

        // Predicción del resultado esperado (title del .lines, ej: "1X0" = local gana)
        const linesEl = gameCell.querySelector('.lines');
        const likelyResult = linesEl ? (linesEl.getAttribute('title') || '').split('\n')[0].trim() : null;

        // Probabilidades — en orden fijo según diseño de la tabla
        const probCells = Array.from(row.querySelectorAll('td.probability'));
        const probs = {};
        PROB_ORDER.forEach((name, i) => {
          const val = probVal(probCells[i]);
          if (val !== null) probs[name] = val;
        });

        // Score predictions (excluir .rscore que son resultados reales)
        const scoreCells = Array.from(row.querySelectorAll('td.tc.score:not(.rscore)'));
        const scorePred   = scoreVal(scoreCells[0]);
        const htScorePred = scoreVal(scoreCells[1]);

        // ¿Está en la sección "Value Bets"?
        const section = row.closest('[class*="valuebets"],[id*="valuebets"],[id*="Value"]');
        const inValueBets = !!section;

        results.push({ home, away, league, likelyResult, probs, scorePred, htScorePred, inValueBets, gameId });
      });

      // También scrapejar la sección Value Bets por separado si existe como tabla aparte
      document.querySelectorAll('[class*="valuebets"] tr, [id*="valuebets"] tr, [id*="Value"] tr').forEach(row => {
        const gameCell = row.querySelector('td.game, td:nth-child(2)');
        if (!gameCell) return;
        const teamsText = gameCell.textContent;
        const vsIdx = teamsText.indexOf(' vs ');
        if (vsIdx === -1) return;
        const home = teamsText.slice(0, vsIdx).trim();
        const away = teamsText.slice(vsIdx + 4).trim();
        // Marcar que aparece en value bets
        const existing = results.find(r => r.home === home && r.away === away);
        if (existing) existing.inValueBets = true;
        else results.push({ home, away, league: '', likelyResult: null, probs: {}, scorePred: null, htScorePred: null, inValueBets: true });
      });

      return results;
    });

    // Procesar y acumular en _zbStore
    const parseNum = v => typeof v === 'number' ? v : null;

    for (const entry of raw) {
      if (!entry.home || !entry.away) continue;
      // Ignorar sports que no sean fútbol (Basketball, etc.) — detectar por nombre de liga
      if (/basketball|nba|nfl|mlb|hockey|nhl|tennis|baseball/i.test(entry.league)) continue;

      const key = _zbKey(entry.home, entry.away);
      const existing = _zbStore.get(key) || {
        home: entry.home, away: entry.away, league: entry.league,
        inValueBets: false, likelyResult: null,
        scorePred: null, htScorePred: null,
        draw: null, over15: null, over25: null, over35: null, btts: null,
        ht_over05: null, ht_over15: null, sh_over05: null, sh_over15: null,
        home_win: null, away_win: null,
        gameId: null, _ts: Date.now(),
      };

      // Acumular — solo sobreescribir si hay nuevo valor (no borrar datos anteriores)
      if (entry.inValueBets) existing.inValueBets = true;
      if (entry.likelyResult) existing.likelyResult = entry.likelyResult;
      if (entry.scorePred)    existing.scorePred    = entry.scorePred;
      if (entry.htScorePred)  existing.htScorePred  = entry.htScorePred;
      if (entry.gameId)       existing.gameId       = entry.gameId;

      for (const [k, v] of Object.entries(entry.probs)) {
        if (v !== null && existing[k] === null) existing[k] = v;
        // Si hay nuevo valor, tomar el promedio ponderado (media entre scrapes)
        else if (v !== null) existing[k] = Math.round((existing[k] + v) / 2);
      }

      existing._ts = Date.now();
      _zbStore.set(key, existing);
      scrapedCount++;
    }

    const football = [..._zbStore.values()].filter(e => !e.league || !/basketball/i.test(e.league));
    console.log(`✅ ZCode Soccer Buddy: ${scrapedCount} entradas, ${football.length} fútbol en store`);
    _zbLastRun = Date.now();

  } catch (e) {
    console.error('zbScrapeOnce error:', e.message);
  } finally {
    if (browser) await browser.close();
  }
  return scrapedCount;
}

// Scraping programado — corre cada ZB_INTERVAL
function zbSchedule() {
  if (!process.env.ZCODE_COOKIES) {
    console.log('ℹ️ ZCODE_COOKIES no configurado — Soccer Buddy desactivado');
    return;
  }
  zbScrapeOnce().catch(e => console.error('ZB scheduled scrape:', e.message));
  setInterval(() => zbScrapeOnce().catch(e => console.error('ZB interval:', e.message)), ZB_INTERVAL);
  // Dropping Odds + Line Reversals — scrape inicial y cada 15 min
  runZcodeMarketScrape().catch(e => console.error('ZDO initial:', e.message));
  setInterval(() => runZcodeMarketScrape().catch(e => console.error('ZDO interval:', e.message)), ZDO_INTERVAL);
  // Extra tools: Totals Predictor, Power Rankings, Soccer Oscillator, Contrarian Bets — cada 20 min
  setTimeout(() => runZcodeExtraTools().catch(e => console.error('ZET initial:', e.message)), 90000); // delay 90s para no saturar al inicio
  setInterval(() => runZcodeExtraTools().catch(e => console.error('ZET interval:', e.message)), ZET_INTERVAL);
  console.log(`📡 Soccer Buddy (${ZB_INTERVAL/60000}min) | Dropping Odds (${ZDO_INTERVAL/60000}min) | Extra Tools (${ZET_INTERVAL/60000}min)`);
}

// Búsqueda de señales para un partido dado
function getZbSignals(homeTeam, awayTeam) {
  // Búsqueda directa
  const directKey = _zbKey(homeTeam, awayTeam);
  if (_zbStore.has(directKey)) return _zbStore.get(directKey);

  // Búsqueda fuzzy si no hay match directo
  for (const [, signals] of _zbStore) {
    if (_zbMatch(homeTeam, signals.home) && _zbMatch(awayTeam, signals.away)) {
      return signals;
    }
  }
  return null;
}

// ─── Chat General ─────────────────────────────────────────────────────────────

async function handleChatGeneral(chatId, pregunta) {
  const CHAT_SYSTEM = `Eres TipsterAI, el analista de fútbol con IA más avanzado disponible en Telegram.
Responde en español. Tono: amigable, directo, seguro de sí mismo. Máximo 4 líneas.
No menciones tecnologías, APIs ni plataformas. No uses ** (doble asterisco) — solo * simple para negrita.

Si el usuario saluda o pregunta qué puedes hacer: responde con energía, menciona 2-3 casos concretos de uso y termina con un CTA directo. Ejemplo de tono: "Cuéntame el partido y te digo exactamente qué apostar."

Si pregunta sobre fútbol o apuestas: responde con conocimiento experto y al final sugiere que pida un análisis completo.

Siempre que sea natural, cierra recordando que puede pedir: *picks de hoy*, *analiza [equipo] vs [equipo]*, *en vivo*, o *ver planes* para acceso completo.`;

  const response = await haiku(CHAT_SYSTEM, pregunta);
  await bot.sendMessage(chatId, normalizeMd(response), { parse_mode: 'Markdown' });
}

async function handleVerPlanes(chatId, telegramId) {
  const tid = telegramId || chatId;
  const linkVip15 = wompiLink(WOMPI_LINKS.vip15, tid, 'vip15');
  const linkVip30 = wompiLink(WOMPI_LINKS.vip30, tid, 'vip30');
  const linkPro30 = wompiLink(WOMPI_LINKS.pro30, tid, 'pro30');

  await bot.sendMessage(chatId,
    `🤖 *TipsterAI — Elige tu plan*\n\n` +
    `Acceso a análisis reales con datos de más de 100 ligas:\n` +
    `✅ Modelo Poisson + estadísticas reales por partido\n` +
    `✅ Lesionados y sancionados confirmados antes del partido\n` +
    `✅ Motivación de equipos (qué se juegan en la jornada)\n` +
    `✅ Historial H2H + forma reciente de los últimos 5 partidos\n` +
    `✅ Picks en vivo con datos en tiempo real\n\n` +
    `━━━━━━━━━━━━━━━━━━━\n\n` +
    `⚡ *VIP 15 días — $59.900 COP*\n` +
    `▸ 10 consultas/día · todo incluido\n` +
    `▸ Solo *$3.993 pesos por día* — menos que un café ☕\n` +
    `💳 [Pagar con Nequi / PSE / Bancolombia](${linkVip15})\n\n` +
    `🔥 *VIP 30 días — $99.900 COP* ← _más popular_\n` +
    `▸ Todo el VIP · doble duración\n` +
    `▸ Solo *$3.330 pesos por día*\n` +
    `💳 [Pagar con Nequi / PSE / Bancolombia](${linkVip30})\n\n` +
    `👑 *PRO 30 días — $179.900 COP*\n` +
    `▸ 50 consultas/día · todo el VIP\n` +
    `▸ + Análisis de imágenes en vivo 📸\n` +
    `▸ Sube la foto del partido → análisis instantáneo\n` +
    `💳 [Pagar con Nequi / PSE / Bancolombia](${linkPro30})\n\n` +
    `━━━━━━━━━━━━━━━━━━━\n` +
    `🌎 ¿Fuera de Colombia? Paga con tarjeta internacional:\n` +
    `🔗 [Suscribirse en Whop (USD)](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)\n\n` +
    `_Pago 100% seguro. Activo en menos de 1 minuto._`,
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
      model: 'claude-sonnet-4-6',
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
        const [h2hRes, shRes, saRes] = await Promise.allSettled([
          getH2H(homeId, awayId),
          getTeamStats(homeId, null),
          getTeamStats(awayId, null),
        ]);
        const h2h = h2hRes.status === 'fulfilled' ? h2hRes.value : [];
        const statsHome = shRes.status === 'fulfilled' ? shRes.value : null;
        const statsAway = saRes.status === 'fulfilled' ? saRes.value : null;
        apiContext = { h2h, bttsEnH2H: h2h.filter(m => m.btts).length, statsLocal: statsHome, statsVisitante: statsAway };
        console.log(`📊 API encontró H2H: ${h2h.length} partidos`);
      }
    } catch (e) { console.log('API lookup falló:', e.message); }

    const contextNote = apiContext
      ? 'Datos históricos disponibles.'
      : 'No se encontraron datos históricos. Analiza SOLO con datos de la imagen. Indica "Análisis basado solo en estadísticas visibles".';

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

// ─── Remarketing ─────────────────────────────────────────────────────────────

async function getUsersByFecha(fecha) {
  try {
    const base = getAirtableBase();
    const records = await base(AIRTABLE_TABLE)
      .select({
        filterByFormula: `AND({fecha_registro} = "${fecha}", {plan} = "free")`,
        fields: ['telegram_id', 'plan', 'fecha_registro'],
      })
      .all();
    return records.map(r => r.fields.telegram_id).filter(Boolean);
  } catch (e) {
    console.error('getUsersByFecha error:', e.message);
    return [];
  }
}

async function enviarRemarketingTelegram(adminChatId, fecha, mensajeCustom) {
  const ids = await getUsersByFecha(fecha);
  if (!ids.length) {
    await bot.sendMessage(adminChatId, `ℹ️ No hay usuarios free registrados el ${fecha}.`);
    return;
  }

  const mensaje = mensajeCustom ||
`⚽ *Los picks de hoy ya están listos*

Análisis con datos reales, lesionados confirmados y motivación de equipos.

Escríbeme *"picks de hoy"* para verlos.

O si tienes un partido específico: *"analiza [equipo] vs [equipo]"* y lo analizo completo ahora.

━━━━━━━━━━━━━━━━━━━
¿Quieres acceso sin límites? Escribe *"ver planes"* — desde *$3.330/día* 💳`;

  await bot.sendMessage(adminChatId, `📤 Enviando a *${ids.length}* usuarios free del ${fecha}...`, { parse_mode: 'Markdown' });

  let enviados = 0;
  let fallidos = 0;

  for (const telegramId of ids) {
    try {
      await bot.sendMessage(telegramId, mensaje, { parse_mode: 'Markdown' });
      enviados++;
      await new Promise(r => setTimeout(r, 500)); // 500ms entre mensajes para evitar flood
    } catch (e) {
      fallidos++;
      console.error(`❌ Remarketing fallido ${telegramId}:`, e.message);
    }
  }

  await bot.sendMessage(adminChatId,
    `✅ *Remarketing completado*\n\n📤 Enviados: ${enviados}\n❌ Fallidos: ${fallidos}\n👥 Total: ${ids.length}`,
    { parse_mode: 'Markdown' }
  );
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
    const linkPro = wompiLink(WOMPI_LINKS.pro30, telegramId, 'pro30');
    await bot.sendMessage(chatId,
      `📸 El análisis de imágenes en vivo está disponible solo en el plan *PRO*.\n\n` +
      `▸ 50 consultas/día · análisis de imágenes 📸 · todas las ligas\n` +
      `💳 [Suscribirse PRO — $179.900 COP](${linkPro})\n` +
      `🌎 [También en Whop (USD)](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)`,
      { parse_mode: 'Markdown' }
    );
    return { allowed: false };
  }

  // Free: verificar período de prueba
  if (plan === 'free' && record.fields.trial_expira) {
    if (today > record.fields.trial_expira) {
      const linkVip15 = wompiLink(WOMPI_LINKS.vip15, telegramId, 'vip15');
      const linkVip30 = wompiLink(WOMPI_LINKS.vip30, telegramId, 'vip30');
      const linkPro30 = wompiLink(WOMPI_LINKS.pro30, telegramId, 'pro30');
      await bot.sendMessage(chatId,
        `⏳ *Tu prueba gratuita terminó*\n\n` +
        `Viste lo que puede hacer TipsterAI: picks con datos reales, motivación de equipos, lesionados confirmados y análisis en vivo.\n\n` +
        `Para seguir accediendo elige tu plan:\n\n` +
        `━━━━━━━━━━━━━━━━━━━\n` +
        `⚡ *VIP 15 días — $59.900 COP*\n` +
        `▸ 10 consultas/día · solo *$3.993/día*\n` +
        `💳 [Activar VIP 15 días](${linkVip15})\n\n` +
        `🔥 *VIP 30 días — $99.900 COP* ← _más popular_\n` +
        `▸ 10 consultas/día · doble duración · *$3.330/día*\n` +
        `💳 [Activar VIP 30 días](${linkVip30})\n\n` +
        `👑 *PRO 30 días — $179.900 COP*\n` +
        `▸ 50 consultas/día + análisis de imágenes en vivo 📸\n` +
        `💳 [Activar PRO](${linkPro30})\n\n` +
        `_Pago con Nequi, PSE, Bancolombia o tarjeta. Activo al instante._\n` +
        `🌎 [También en Whop (USD)](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: '🔥 VIP 30 días — $99.900', url: linkVip30 }],
              [{ text: '⚡ VIP 15 días — $59.900', url: linkVip15 }, { text: '👑 PRO — $179.900', url: linkPro30 }],
            ]
          }
        }
      );
      return { allowed: false };
    }
  }

  // VIP / PRO: verificar que la suscripción no haya expirado
  if ((plan === 'vip' || plan === 'vip15' || plan === 'pro') && record.fields.expires_at) {
    if (today > record.fields.expires_at) {
      // Degradar a free automáticamente
      try {
        const base = getAirtableBase();
        await base(AIRTABLE_TABLE).update(record.id, { plan: 'free' });
      } catch (e) { console.error('downgrade error:', e.message); }
      const linkVip30 = wompiLink(WOMPI_LINKS.vip30, telegramId, 'vip30');
      const linkPro30 = wompiLink(WOMPI_LINKS.pro30, telegramId, 'pro30');
      await bot.sendMessage(chatId,
        `🔄 *Tu suscripción ${plan.toUpperCase()} venció*\n\n` +
        `Renueva y mantén acceso completo sin interrupciones:\n\n` +
        `🔥 *VIP 30 días — $99.900 COP* ← _mejor valor_\n` +
        `▸ 10 consultas/día · solo $3.330/día\n` +
        `💳 [Renovar VIP 30 días](${linkVip30})\n\n` +
        `👑 *PRO 30 días — $179.900 COP*\n` +
        `▸ 50 consultas/día + análisis de imágenes 📸\n` +
        `💳 [Renovar PRO](${linkPro30})\n\n` +
        `_Activo al instante tras el pago._`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: '🔥 Renovar VIP 30 días — $99.900', url: linkVip30 }],
              [{ text: '👑 Renovar PRO — $179.900', url: linkPro30 }],
            ]
          }
        }
      );
      return { allowed: false };
    }
  }

  // Verificar límite diario
  if (consultasHoy >= planConfig.consultas_diarias) {
    let msg;
    if (plan === 'free') {
      await bot.sendMessage(chatId,
        `✅ *Consulta de hoy usada*\n\n` +
        `Vuelve mañana para tu próxima consulta gratis.\n\n` +
        `O accede ahora mismo sin límites:\n\n` +
        `⚡ *VIP — desde $59.900 COP*\n` +
        `▸ 10 consultas/día · picks + análisis + en vivo\n` +
        `▸ Solo $3.993 pesos por día ☕`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [
              [{ text: '🔥 Ver planes y precios', callback_data: 'show_planes' }],
            ]
          }
        }
      );
      console.log('CHECK ACCESS - resultado: bloqueado por límite diario (free)');
      return { allowed: false };
    } else if (plan === 'vip' || plan === 'vip15') {
      const linkPro30 = wompiLink(WOMPI_LINKS.pro30, telegramId, 'pro30');
      msg =
        `⚡ *Llegaste al límite de hoy (10 consultas)*\n\n` +
        `Tus consultas se renuevan a medianoche 🕛\n\n` +
        `¿Necesitas más hoy? Upgrade a PRO:\n\n` +
        `👑 *PRO 30 días — $179.900 COP*\n` +
        `▸ 50 consultas/día + análisis de imágenes 📸\n\n` +
        `💳 [Upgrade a PRO](${linkPro30})`;
    } else {
      msg = `⏰ *50 consultas alcanzadas hoy*\nTus consultas se renuevan a medianoche. ¡Hasta mañana!`;
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

// ─── Command: /remarketing ────────────────────────────────────────────────────
// Uso: /remarketing YYYY-MM-DD [mensaje personalizado]
// Solo el admin puede ejecutarlo

// ─── Command: /zcode-debug ────────────────────────────────────────────────────
// Solo admin. Visita ZCode Soccer Buddy, intercepta red, toma screenshot y
// vuelca el DOM — para descubrir la estructura antes de afinar los selectores.
bot.onText(/\/zcode[-_]?debug/, async (msg) => {
  const telegramId = String(msg.from.id);
  if (!ADMIN_IDS.has(telegramId)) return;
  await zbDiscover(String(msg.chat.id));
});

// ─── Command: /api-debug ─── muestra qué devuelve API-Football para Copa Sud ──
bot.onText(/\/api[-_]?debug/, async (msg) => {
  const telegramId = String(msg.from.id);
  if (!ADMIN_IDS.has(telegramId)) return;
  const chatId = String(msg.chat.id);
  const today = todayDate();
  const nextUtc = (() => {
    const d = new Date(today + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + 1);
    return d.toISOString().split('T')[0];
  })();

  await bot.sendMessage(chatId, `🔍 Debug API-Football\nFecha Bogotá hoy: ${today}\nFecha UTC siguiente: ${nextUtc}\nConsultando...`);

  try {
    // Test 1: por fecha (sin filtro de liga) — como hace picks de hoy
    const r1 = await API.get('/fixtures', { params: { date: today } });
    const all1 = r1.data.response || [];
    const sud1 = all1.filter(f => f.league.id === 9);
    await bot.sendMessage(chatId, `📅 date=${today} → ${all1.length} total, ${sud1.length} Copa Sud (ID 9)\n${sud1.map(f => `▸ ${f.teams.home.name} vs ${f.teams.away.name} | ${f.fixture.status.short} | ${f.fixture.date}`).join('\n') || '(ninguno)'}`);

    // Test 2: fecha siguiente UTC
    const r2 = await API.get('/fixtures', { params: { date: nextUtc } });
    const all2 = r2.data.response || [];
    const sud2 = all2.filter(f => f.league.id === 9);
    await bot.sendMessage(chatId, `📅 date=${nextUtc} → ${all2.length} total, ${sud2.length} Copa Sud (ID 9)\n${sud2.map(f => `▸ ${f.teams.home.name} vs ${f.teams.away.name} | ${f.fixture.status.short} | ${f.fixture.date}`).join('\n') || '(ninguno)'}`);

    // Test 3: buscar equipos de Copa Sud en los 232 fixtures de hoy
    const sudTeams = ['bragantino','river plate','blooming','carabobo','atletico','mineiro','caracas','botafogo','olimpia','racing','vasco','cienciano','juventud','audax','barracas','independiente','san lorenzo','santos','recoleta'];
    const matchesSud = [...all1, ...all2].filter(f => {
      const h = (f.teams.home.name || '').toLowerCase();
      const a = (f.teams.away.name || '').toLowerCase();
      return sudTeams.some(t => h.includes(t) || a.includes(t));
    });
    const uniqueLeagues = [...new Map(matchesSud.map(f => [f.league.id, f.league])).values()];
    await bot.sendMessage(chatId,
      `🔎 Equipos Copa Sud encontrados en fixtures crudos: ${matchesSud.length}\n` +
      matchesSud.slice(0,8).map(f => `▸ [ID ${f.league.id}] ${f.teams.home.name} vs ${f.teams.away.name} | ${f.fixture.date.slice(11,16)} UTC`).join('\n') +
      `\n\n🏷 Ligas únicas: ${uniqueLeagues.map(l => `ID ${l.id} = ${l.name}`).join(', ') || '(ninguna)'}`
    );

    // Test 4: liga 9 sin season (por si season=2026 es incorrecto)
    const r4 = await API.get('/fixtures', { params: { league: 9, date: today } });
    const all4 = r4.data.response || [];
    await bot.sendMessage(chatId, `🏆 league=9 (sin season) date=${today} → ${all4.length} fixtures\n${all4.slice(0,5).map(f => `▸ ${f.teams.home.name} vs ${f.teams.away.name} | ${f.league.season}`).join('\n') || '(ninguno)'}`);

  } catch (e) {
    await bot.sendMessage(chatId, `❌ Error: ${e.message}`);
  }
});

// ─── Command: /debugevaluar — muestra raw de Highlightly para picks pendientes ─
bot.onText(/\/debugevaluar/, async (msg) => {
  const telegramId = String(msg.from.id);
  if (!ADMIN_IDS.has(telegramId)) return;
  const chatId = String(msg.chat.id);

  await bot.sendMessage(chatId, '🔍 Revisando picks pendientes en Airtable...');
  try {
    const picks = await getPicksFromAirtable('total').catch(() => []);
    const pending = picks.filter(p => (!p.resultado || p.resultado === '?') && p.fixtureId);
    const fixtureIds = [...new Set(pending.map(p => p.fixtureId))];

    await bot.sendMessage(chatId,
      `📊 Total Airtable: ${picks.length} | Pendientes con fixtureId: ${pending.length} | Fixtures únicos: ${fixtureIds.length}\n` +
      `Sin fixtureId: ${picks.filter(p => (!p.resultado || p.resultado === '?') && !p.fixtureId).length}`
    );

    // Probar los primeros 5 fixtures
    for (const fid of fixtureIds.slice(0, 5)) {
      try {
        const { data } = await API.get('/matches/' + fid);
        const raw = Array.isArray(data) ? data : Array.isArray(data?.data) ? data.data : [data?.data || data];
        const m = raw[0];
        const info = m
          ? `state="${m.state?.description}" | score="${m.state?.score?.current}" | ${m.homeTeam?.name} vs ${m.awayTeam?.name}`
          : `respuesta vacía`;
        const esTerminado = m && FINISHED_DESCS.has(m.state?.description);
        await bot.sendMessage(chatId, `${esTerminado ? '✅' : '⏳'} Fixture ${fid}:\n${info}`);
      } catch (e) {
        await bot.sendMessage(chatId, `❌ Fixture ${fid}: ${e.message}`);
      }
    }
  } catch (e) {
    await bot.sendMessage(chatId, `❌ Error: ${e.message}`);
  }
});

// ─── Command: /debugstats {fixtureId} — muestra displayNames crudos de Highlightly ─
bot.onText(/\/debugstats(?:\s+(\d+))?/, async (msg, match) => {
  const telegramId = String(msg.from.id);
  if (!ADMIN_IDS.has(telegramId)) return;
  const chatId = String(msg.chat.id);
  const fixtureId = match?.[1];
  if (!fixtureId) {
    await bot.sendMessage(chatId, '❓ Uso: /debugstats {fixtureId}\nEjemplo: /debugstats 1309699145');
    return;
  }
  try {
    const { data } = await API.get('/statistics/' + fixtureId);
    const arr = Array.isArray(data) ? data : (data?.data || []);
    if (!arr.length) {
      await bot.sendMessage(chatId, `⚠️ Fixture ${fixtureId}: sin estadísticas`);
      return;
    }
    for (const teamData of arr) {
      const teamName = teamData.team?.name || teamData.teamName || '(equipo)';
      const stats = teamData.statistics || [];
      const lines = stats.map(s => `${s.displayName}: ${s.value}`).join('\n');
      const msg = `📊 ${teamName}\n\n${lines || '(sin campos)'}`;
      await bot.sendMessage(chatId, msg.slice(0, 4000));
    }
  } catch (e) {
    await bot.sendMessage(chatId, `Error: ${e.message}`);
  }
});

// ─── Command: /zcode-status ───────────────────────────────────────────────────
bot.onText(/\/zcode[-_]?status/, async (msg) => {
  const telegramId = String(msg.from.id);
  if (!ADMIN_IDS.has(telegramId)) return;
  const chatId = String(msg.chat.id);
  const size = _zbStore.size;
  const last = _zbLastRun ? new Date(_zbLastRun).toLocaleString('es-CO', { timeZone: 'America/Bogota' }) : 'nunca';
  const hasCookies = !!process.env.ZCODE_COOKIES;
  let msg2 = `📡 *Estado del motor externo*\n\n`;
  msg2 += `🔑 Sesión: ${hasCookies ? '✅ activa' : '❌ no configurada'}\n`;
  msg2 += `📊 Partidos en store: *${size}*\n`;
  msg2 += `🕐 Último scrape: ${last}\n\n`;
  if (size > 0) {
    msg2 += `*Últimos 5 partidos:*\n`;
    let count = 0;
    for (const [, s] of _zbStore) {
      if (count++ >= 5) break;
      msg2 += `▸ ${s.home} vs ${s.away}`;
      if (s.btts && s.btts !== 'LOCKED') msg2 += ` | BTTS ${s.btts}%`;
      if (s.over25 && s.over25 !== 'LOCKED') msg2 += ` | O2.5 ${s.over25}%`;
      msg2 += `\n`;
    }
  }
  await bot.sendMessage(chatId, msg2, { parse_mode: 'Markdown' });
});

// ─── Command: /zcode-tools — explora Dropping Odds, Line Reversals, Totals Predictor ───
bot.onText(/\/zcode[-_]?tools/, async (msg) => {
  const telegramId = String(msg.from.id);
  if (!ADMIN_IDS.has(telegramId)) return;
  const chatId = String(msg.chat.id);

  const TOOLS = [
    { name: 'Soccer Buddy',      url: 'https://zcodesystem.com/soccerbuddy/',                  wait: 5000 },
    { name: 'EV Tool',           url: 'https://zcodesystem.com/evtool/',                        wait: 3000 },
    { name: 'Line Reversals',    url: 'https://zcodesystem.com/line_reversals',                 wait: 3000 },
    { name: 'Totals Predictor',  url: 'https://zcodesystem.com/totals_predictor',               wait: 4000 },
    { name: 'Power Rankings',    url: 'https://zcodesystem.com/power_rankings.php',             wait: 3000 },
    { name: 'Soccer Oscillator', url: 'https://zcodesystem.com/soccer_oscillator/',             wait: 4000 },
    { name: 'Contrarian Bets',   url: 'https://zcodesystem.com/vipclub/contrarianbets.php',    wait: 3000 },
  ];

  await bot.sendMessage(chatId, `🔍 Explorando ${TOOLS.length} herramientas ZCode con Puppeteer...`);

  let browser;
  try {
    browser = await _zbBrowser();

    for (const tool of TOOLS) {
      await bot.sendMessage(chatId, `\n🔧 *${tool.name}*`, { parse_mode: 'Markdown' });
      const page = await browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36');

      const apiCalls = [];
      page.on('response', async (res) => {
        const url = res.url();
        const ct  = res.headers()['content-type'] || '';
        const isJS  = url.endsWith('.js') || url.includes('.js?') || url.includes('moment') || url.includes('jquery');
        const isCSS = url.endsWith('.css') || url.includes('.css?');
        const isYT  = url.includes('youtube') || url.includes('ytimg') || url.includes('googlevideo');
        const isTrack = url.includes('analytics') || url.includes('facebook') || url.includes('logevent') || url.includes('pixel') || url.includes('gtm');
        if (isJS || isCSS || isYT || isTrack) return;
        const isJson = ct.includes('json') || ct.includes('javascript');
        const isData = url.includes('/api/') || url.includes('ajax') || url.includes('.php')
          || url.includes('predict') || url.includes('odds') || url.includes('reversal')
          || url.includes('ranking') || url.includes('contrarian') || url.includes('oscillator')
          || url.includes('soccer') || url.includes('buddy') || url.includes('total')
          || url.includes('zcode') || url.includes('evtool') || url.includes('gameslist');
        if (isJson || isData) {
          const body = await res.text().catch(() => '');
          if (body.length > 20 && body.length < 500000) {
            apiCalls.push({ url, status: res.status(), body: body.slice(0, 600) });
          }
        }
      });

      const hasCookies = await _zbSetCookies(page);
      try {
        await page.goto(tool.url, { waitUntil: 'networkidle2', timeout: 35000 });
        // Scroll para activar lazy loading y esperar carga dinámica (Ajax/polling)
        await page.evaluate(() => window.scrollBy(0, 600));
        await new Promise(r => setTimeout(r, tool.wait || 3000));
        await page.evaluate(() => window.scrollBy(0, 600));
        await new Promise(r => setTimeout(r, 1500));

        // Screenshot (tolerante a dimensiones inválidas)
        const shot = await page.screenshot({ type: 'png', fullPage: false })
          .catch(() => page.screenshot({ type: 'png', fullPage: false, clip: { x:0, y:0, width:800, height:600 } }))
          .catch(() => null);
        if (shot && shot.length > 500) {
          await bot.sendPhoto(chatId, shot, { caption: `${tool.name} | cookies: ${hasCookies?'✅':'❌'} | URL: ${page.url().slice(0,60)}` })
            .catch(() => bot.sendMessage(chatId, `📸 ${tool.name} | cookies: ${hasCookies?'✅':'❌'} (screenshot omitido)`));
        } else {
          await bot.sendMessage(chatId, `📸 ${tool.name} | cookies: ${hasCookies?'✅':'❌'} | URL: ${page.url().slice(0,60)} (sin screenshot)`);
        }

        // APIs encontradas
        if (apiCalls.length > 0) {
          let msg2 = `🔌 *${apiCalls.length} APIs detectadas:*\n`;
          for (const c of apiCalls.slice(0, 5)) {
            msg2 += `\n\`${c.url.slice(0, 90)}\`\n${c.body.slice(0, 200)}\n`;
          }
          await sendLong(chatId, msg2, { parse_mode: 'Markdown' }).catch(() =>
            bot.sendMessage(chatId, `APIs: ${apiCalls.map(c=>c.url).join('\n')}`)
          );
        }
        // Extraer estructura DOM real: clases usadas y texto visible
        const domInfo = await page.evaluate(() => {
          // Clases únicas de todos los elementos
          const classes = new Set();
          document.querySelectorAll('[class]').forEach(el => {
            String(el.className).split(' ').filter(c=>c.length>2).forEach(c=>classes.add(c));
          });
          // Texto limpio
          document.querySelectorAll('script,style,nav,footer,header').forEach(el=>el.remove());
          const text = document.body.innerText.replace(/\s+/g,' ').trim().slice(0,800);
          // HTML de la primera tabla/div con datos
          const table = document.querySelector('table, .GamesTable, [class*="Table"], [class*="Game"]');
          const tableHtml = table ? table.outerHTML.slice(0,600) : 'no table found';
          return { classes: [...classes].slice(0,40).join(', '), text, tableHtml };
        });
        const info = `📄 *${tool.name}*\nClases DOM: \`${domInfo.classes.slice(0,200)}\`\nTexto: ${domInfo.text.slice(0,300)}\nPrimer table HTML: \`${domInfo.tableHtml.slice(0,300)}\``;
        await sendLong(chatId, info, { parse_mode: 'Markdown' }).catch(
          () => bot.sendMessage(chatId, `📄 ${tool.name}: ${domInfo.text.slice(0,400)}`)
        );
      } catch (e) {
        await bot.sendMessage(chatId, `⚠️ Error en ${tool.name}: ${e.message}`);
      }
      await page.close();
    }

    await bot.sendMessage(chatId, '✅ Exploración completada. Comparte los resultados para integrar los datos.');
  } catch (e) {
    await bot.sendMessage(chatId, `❌ Error general: ${e.message}`);
  } finally {
    if (browser) await browser.close().catch(() => {});
  }
});

bot.onText(/\/remarketing(?:\s+(.+))?/, async (msg, match) => {
  const chatId     = msg.chat.id;
  const telegramId = String(msg.from.id);

  if (!ADMIN_IDS.has(telegramId)) return; // silencioso para no-admins

  const args = (match[1] || '').trim();

  // Extraer fecha (primer token) y mensaje opcional (resto)
  const [fechaArg, ...restoArr] = args.split(' ');
  const mensajeCustom = restoArr.length ? restoArr.join(' ') : null;

  // Si no se pasa fecha, usar ayer en Bogotá
  let fecha = fechaArg;
  if (!fecha || !/^\d{4}-\d{2}-\d{2}$/.test(fecha)) {
    const ayer = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Bogota' }));
    ayer.setDate(ayer.getDate() - 1);
    fecha = ayer.toISOString().slice(0, 10);
  }

  await bot.sendMessage(chatId, `🔍 Buscando usuarios free del *${fecha}*...`, { parse_mode: 'Markdown' });
  await enviarRemarketingTelegram(chatId, fecha, mensajeCustom || null);
});

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

  bot.sendMessage(chatId,
    `🤖 *TipsterAI* — Analista de fútbol con IA\n\n` +
    `Hola${username ? ' *' + username + '*' : ''}. Analizo partidos en segundos con datos reales:\n\n` +
    `✅ Picks con modelo Poisson + Expected Goals\n` +
    `✅ Lesionados y sancionados confirmados\n` +
    `✅ Motivación de equipos (qué se juegan)\n` +
    `✅ H2H, forma reciente y cuotas de mercado\n` +
    `✅ Picks en vivo con estadísticas en tiempo real\n\n` +
    `━━━━━━━━━━━━━━━━━━━\n` +
    `Prueba ahora — *3 días gratis*, 1 consulta al día:\n\n` +
    `▸ Escribe *"picks de hoy"*\n` +
    `▸ O *"analiza [equipo] vs [equipo]"*\n` +
    `▸ O envía 📸 una captura de partido en vivo`,
    {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: [
          [{ text: '🎯 Picks de hoy', callback_data: 'picks_hoy' }, { text: '📊 Ver planes', callback_data: 'show_planes' }],
        ]
      }
    }
  );
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

      // Sistema del Día — solo admin (no aparece en menú público)
      if (ADMIN_IDS.has(telegramId) && /sistema\s*(del?\s*)?(dia|hoy|d[íi]a)/.test(q)) {
        return { intencion: 'sistema_hoy', pregunta_especifica: t };
      }

      // Alerta de gol — debe ir ANTES de en_vivo para capturar "gol en vivo"
      if (/alerta.{0,6}gol|gol.{0,10}(en\s*vivo|vivo|live|ahora|ahora mismo)|probabilidad.{0,6}gol|donde.{0,10}gol|partido.{0,10}gol|next.{0,4}goal/.test(q)) {
        return { intencion: 'alerta_gol', pregunta_especifica: t };
      }

      // Normalizar variantes de "picks": pics, pick, pix → picks
      const qNorm = q.replace(/\bpics?\b/g, 'picks').replace(/\bpix\b/g, 'picks');

      // Picks del día general
      const isPicksHoy =
        /^(picks?|apuestas?)\s*(de\s*)?(hoy|del\s*dia|para\s*hoy)/.test(qNorm) ||
        qNorm === 'picks' ||
        qNorm === 'picks hoy' ||
        // "actualizar picks", "refresh picks", "dame picks", "ver picks", etc.
        /\b(actualizar|refresh|forzar|nuevo|recalcul|regenera|dame|ver|muestra|quiero)\b.*\bpicks?\b/.test(qNorm) ||
        /\bpicks?\b.*\b(actualizar|refresh|forzar|nuevo|recalcul|regenera|globales?|del\s*d[íi]a|de\s*hoy)\b/.test(qNorm) ||
        // "top 3 global", "top picks", "mejores picks"
        /^top\s*\d*\s*(global|picks?|apuestas?)/.test(qNorm) ||
        /^(mejores?|top)\s*(picks?|apuestas?)/.test(qNorm);
      if (isPicksHoy) {
        return { intencion: 'picks_hoy', pregunta_especifica: t };
      }

      // Selecciones nacionales — NUNCA confundir con ligas
      const SELECCIONES = [
        'francia','brazil','brasil','alemania','espana','italia','inglaterra',
        'portugal','holanda','paises bajos','belgica','croacia','colombia',
        'argentina','uruguay','chile','peru','ecuador','venezuela','mexico',
        'estados unidos','usa','eeuu','japon','corea','corea del sur',
        'marruecos','senegal','nigeria','ghana','camerun','suiza','austria',
        'turquia','dinamarca','suecia','noruega','polonia','ucrania','serbia',
        'escocia','gales','irlanda','australia','canada','costa rica','panama',
        'paraguay','bolivia','arabia saudita','iran','qatar','china','egipto',
        'sudafrica','france','germany','spain','england','netherlands',
        'belgium','croatia','sweden','denmark','switzerland',
      ];
      // Si el mensaje es solo o empieza por un nombre de selección
      const qClean = q.replace(/^(analiza?|picks?|apuesta[s]?\s+en?\s+|dame\s+|ver\s+|como\s+viene\s+|amistoso\s+)/,'').trim();
      if (SELECCIONES.includes(qClean)) {
        return { intencion: 'partido_especifico', equipo: t.replace(/^(analiza?|picks?|apuesta[s]?\s+en?\s+|dame\s+|ver\s+|como\s+viene\s+|amistoso\s+)/i,'').trim(), pregunta_especifica: t, liga: null, mercado: null, tiempo: null, contexto: 'proximo_partido', period: null, venue: 'all' };
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
      return handleVerPlanes(chatId, telegramId);
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
      case 'sistema_hoy':
        await handleSistemaHoy(chatId);
        break;
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

// Continúa handlePartido con un equipo ya seleccionado (post-botón)
// Redirige al handlePartido completo pasando el teamData directamente — mismo análisis, mismos datos
async function handlePartidoConTeam(chatId, teamData, intent = {}) {
  await handlePartido(chatId, '', '', teamData);
}

// Continúa handleRachas con un equipo ya seleccionado (post-botón)
async function handleRachasConTeam(chatId, teamData, intent = {}) {
  const syntheticIntent = { ...intent, equipo: teamData.team.name, _teamData: teamData };
  await handleRachas(chatId, syntheticIntent);
}

// ─── Callback query handler (botones inline) ──────────────────────────────────

bot.on('callback_query', async (query) => {
  // Dismissar el spinner de Telegram INMEDIATAMENTE antes de cualquier procesamiento
  await bot.answerCallbackQuery(query.id).catch(() => {});

  if (!query.message) return;

  const chatId = query.message.chat.id;
  const data   = query.data || '';

  console.log(`[callback_query] chatId=${chatId} data="${data}"`);

  try {
    // Picks del día (botón del /start)
    if (data === 'picks_hoy') {
      const telegramId = String(query.from?.id || chatId);
      await bot.editMessageText(
        `🎯 Buscando los mejores picks de hoy...`,
        { chat_id: chatId, message_id: query.message.message_id }
      ).catch(() => {});
      const access = await checkAccess(chatId, telegramId, false);
      if (!access.allowed) return;
      await handlePicksHoy(chatId, false);
      return;
    }

    // Ver planes (botón desde límite diario free o /start)
    if (data === 'show_planes') {
      const telegramId = query.from?.id || chatId;
      await bot.editMessageText(
        `📊 Planes disponibles:`,
        { chat_id: chatId, message_id: query.message.message_id }
      ).catch(() => {});
      await handleVerPlanes(chatId, telegramId);
      return;
    }

    // Cancelar
    if (data === 'tm_cancel') {
      await bot.editMessageText('❌ Selección cancelada.',
        { chat_id: chatId, message_id: query.message.message_id }
      ).catch(() => {});
      return;
    }

    // Selección: tm_{teamId}_{intentCode}
    if (data.startsWith('tm_')) {
      const parts      = data.split('_');      // ['tm','12345','p']
      const teamId     = parseInt(parts[1]);
      const intentCode = parts[2] || 'p';

      console.log(`[callback_query] teamId=${teamId} intentCode=${intentCode}`);

      // Editar el mensaje de botones para mostrar que estamos procesando
      await bot.editMessageText('⏳ Buscando información del equipo...',
        { chat_id: chatId, message_id: query.message.message_id }
      ).catch(() => {});

      // Obtener datos del equipo directo de la API (sin Map en memoria)
      const { data: apiRes } = await API.get('/teams', { params: { id: teamId } });
      const teamData = (apiRes.response || [])[0];

      if (!teamData) {
        await bot.sendMessage(chatId, '❌ No pude cargar el equipo. Escribe la pregunta de nuevo.');
        return;
      }

      console.log(`[callback_query] team=${teamData.team.name} intencion=${intentCode}`);

      if (intentCode === 'r') {
        await handleRachasConTeam(chatId, teamData, {});
      } else {
        await handlePartidoConTeam(chatId, teamData, {});
      }
      return;
    }

  } catch (err) {
    console.error('callback_query error:', err.message);
    await bot.sendMessage(chatId, '❌ Error al procesar. Intenta de nuevo.').catch(() => {});
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
function exitBot(reason, delayMs = 0) {
  console.error(`💥 Reiniciando proceso: ${reason}`);
  if (delayMs > 0) {
    console.log(`⏳ Esperando ${delayMs / 1000}s antes de reiniciar...`);
    setTimeout(() => process.exit(1), delayMs);
  } else {
    process.exit(1);
  }
}

// Polling errors: 429 espera, otros errores transitorios se ignoran
let consecutivePollingErrors = 0;
bot.on('polling_error', err => {
  const msg = err.message || '';
  const code = err.code || '';

  // 409 Conflict — otra instancia corriendo, reiniciar después de esperar
  if (msg.includes('409') || code === 'ETELEGRAM') {
    console.warn(`⚠️  Telegram 409 Conflict — esperando 15s`);
    exitBot(`polling_error 409: ${msg}`, 15000);
    return;
  }

  // 401 Unauthorized — token inválido, no tiene sentido reintentar
  if (msg.includes('401')) {
    exitBot(`polling_error 401 — token inválido: ${msg}`);
    return;
  }

  // 429 Rate limit — esperar retry_after
  const retryMatch = msg.match(/retry after (\d+)/i);
  if (retryMatch) {
    const retrySecs = Math.max(parseInt(retryMatch[1], 10), 10);
    console.warn(`⚠️  Telegram 429 — esperando ${retrySecs}s`);
    consecutivePollingErrors = 0;
    exitBot(`polling_error 429: ${msg}`, retrySecs * 1000);
    return;
  }

  // Errores de red transitorios — tolerar hasta 5 consecutivos
  consecutivePollingErrors++;
  console.warn(`⚠️  polling_error (${consecutivePollingErrors}/5): ${msg}`);
  if (consecutivePollingErrors >= 5) {
    exitBot(`polling_error persistente: ${msg}`);
  }
});
bot.on('message',        () => { consecutivePollingErrors = 0; });
bot.on('callback_query', () => { consecutivePollingErrors = 0; });

// Errores no capturados — loguear siempre, solo reiniciar en uncaughtException
process.on('uncaughtException', err => {
  console.error(`💥 uncaughtException: ${err.message}\n${err.stack}`);
  exitBot(`uncaughtException: ${err.message}`);
});
process.on('unhandledRejection', (err) => {
  const msg = err instanceof Error ? err.message : String(err);
  console.error(`⚠️  unhandledRejection (no fatal): ${msg}`);
  // No crashear — solo loguear. Las promesas rechazadas no deben bajar el bot.
});

// Watchdog: cada 90 segundos verifica que el polling siga vivo
let lastUpdateReceived = Date.now();
let watchdogFailures = 0;

setInterval(async () => {
  try {
    await bot.getMe();
    const minutesSilent = (Date.now() - lastUpdateReceived) / 60000;
    // Si llevamos más de 20 min sin ningún update Y getMe empieza a tener
    // fallos intermitentes, algo está mal. Pero getMe OK = polling OK aquí
    // solo logueamos para diagnóstico.
    watchdogFailures = 0;
    console.log(`💓 OK ${new Date().toISOString()} | silencio: ${minutesSilent.toFixed(1)}min`);
  } catch (err) {
    watchdogFailures++;
    console.warn(`⚠️  keepalive getMe falló (${watchdogFailures}/3): ${err.message}`);
    if (watchdogFailures >= 3) {
      exitBot(`keepalive getMe falló 3 veces seguidas: ${err.message}`);
    }
  }
}, 90 * 1000);


// Arrancar Soccer Buddy scraper si hay cookies configuradas (no bloquea el arranque)
zbSchedule();

// ─── Whop Webhook Server ───────────────────────────────────────────────────────

const AIRTABLE_TABLE = process.env.AIRTABLE_TABLE || 'Users';
const WHOP_SECRET   = process.env.WHOP_WEBHOOK_SECRET;
const WEBHOOK_PORT  = process.env.PORT || 3000;

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

function expiresAt15Days() {
  const d = new Date();
  d.setDate(d.getDate() + 15);
  return d.toISOString().split('T')[0];
}

// Wompi: verifica checksum del evento usando WOMPI_EVENTS_SECRET
// Spec: SHA256( signature.properties values concat + eventsKey )
function verifyWompiSignature(payload, eventsKey) {
  try {
    const { signature, data } = payload;
    if (!eventsKey) return true;   // sin clave configurada: permisivo (solo desarrollo)
    if (!signature?.checksum || !signature?.properties) return false;
    const getValue = (obj, dotPath) =>
      dotPath.split('.').reduce((acc, k) => (acc != null ? acc[k] : undefined), obj);
    const concat = signature.properties.map(p => String(getValue(data, p) ?? '')).join('') + eventsKey;
    const expected = crypto.createHash('sha256').update(concat).digest('hex');
    return crypto.timingSafeEqual(Buffer.from(signature.checksum), Buffer.from(expected));
  } catch (e) {
    console.error('verifyWompiSignature error:', e.message);
    return false;
  }
}

const app = express();

// Parse raw body for signature verification before JSON parsing
app.use('/webhook/whop',  express.raw({ type: 'application/json' }));
app.use('/webhook/wompi', express.raw({ type: 'application/json' }));
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

// ─── Wompi Webhook ────────────────────────────────────────────────────────────

const WOMPI_EVENTS_SECRET = process.env.WOMPI_EVENTS_SECRET;

// Mapeo: referencia sufijo → plan interno + días
const WOMPI_PLAN_MAP = {
  vip15: { plan: 'vip15', expires: expiresAt15Days },
  vip30: { plan: 'vip',   expires: expiresAt30Days },
  pro30: { plan: 'pro',   expires: expiresAt30Days },
};

app.post('/webhook/wompi', async (req, res) => {
  const rawBody = req.body.toString('utf8');

  let payload;
  try { payload = JSON.parse(rawBody); }
  catch { return res.status(400).json({ error: 'Invalid JSON' }); }

  if (!verifyWompiSignature(payload, WOMPI_EVENTS_SECRET)) {
    console.warn('⚠️  Wompi webhook: firma inválida');
    return res.status(401).json({ error: 'Invalid signature' });
  }

  const event = payload.event;
  const tx    = payload?.data?.transaction;

  console.log(`📨 Wompi event: ${event} | ref=${tx?.reference} | status=${tx?.status}`);

  // Solo procesar transacciones aprobadas
  if (event !== 'transaction.updated' || tx?.status !== 'APPROVED') {
    return res.status(200).json({ received: true, skipped: true });
  }

  try {
    // reference = "TELEGRAMID_plan" (ej: 123456789_vip15)
    const reference = tx.reference || '';
    const underscoreIdx = reference.indexOf('_');
    if (underscoreIdx === -1) {
      console.warn('⚠️  Wompi: referencia sin guión bajo:', reference);
      return res.status(200).json({ received: true, error: 'Bad reference format' });
    }

    const telegramId  = reference.substring(0, underscoreIdx);
    const planSuffix  = reference.substring(underscoreIdx + 1); // vip15 | vip30 | pro30
    const planConfig  = WOMPI_PLAN_MAP[planSuffix];

    if (!planConfig) {
      console.warn('⚠️  Wompi: plan desconocido en referencia:', planSuffix);
      return res.status(200).json({ received: true, error: 'Unknown plan' });
    }

    const internalPlan = planConfig.plan;
    const expires      = planConfig.expires();

    await upsertAirtableUser(telegramId, { plan: internalPlan, expires_at: expires });
    console.log(`✅ Wompi: activado plan=${internalPlan} para telegram_id=${telegramId} hasta ${expires}`);

    // Mensaje de bienvenida según plan
    const planNombre = PLANES[internalPlan]?.nombre || internalPlan.toUpperCase();
    const consultasDia = PLANES[internalPlan]?.consultas_diarias || 10;
    const puedeImagen  = PLANES[internalPlan]?.puede_imagen || false;

    await bot.sendMessage(telegramId,
      `🎉 *¡Pago confirmado! Ya eres ${planNombre}*\n\n` +
      `Acceso activo hasta el *${expires}* · *${consultasDia} consultas/día*\n\n` +
      `Esto es lo que puedes hacer ahora:\n\n` +
      `🎯 *"picks de hoy"* → los mejores picks del día con datos reales\n` +
      `⚽ *"analiza [equipo] vs [equipo]"* → análisis completo con H2H, forma y cuotas\n` +
      `📡 *"en vivo"* → picks para partidos que están jugando ahora\n` +
      (puedeImagen ? `📸 *Envía una captura* de cualquier partido → análisis instantáneo\n\n` : `\n`) +
      `━━━━━━━━━━━━━━━━━━━\n` +
      `¿Empezamos? Escribe *"picks de hoy"* ahora mismo 🚀`,
      {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '🎯 Picks de hoy', callback_data: 'picks_hoy' }],
          ]
        }
      }
    );

    res.status(200).json({ received: true });
  } catch (err) {
    console.error('Wompi webhook handler error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (_req, res) => res.json({ status: 'ok', ts: new Date().toISOString() }));

app.listen(WEBHOOK_PORT, '0.0.0.0', () => {
  console.log(`🌐 Webhook server escuchando en puerto ${WEBHOOK_PORT}`);
  console.log(`   POST http://localhost:${WEBHOOK_PORT}/webhook/whop`);
  console.log(`   POST http://localhost:${WEBHOOK_PORT}/webhook/wompi`);
});

// Auto-fetch árbitros de StatsHub al arrancar y cada 6 horas
fetchStatsHubReferees();
setInterval(fetchStatsHubReferees, 6 * 60 * 60 * 1000);
if (process.env.AIRTABLE_API_KEY) ensurePicksTable().catch(e => console.error('ensurePicksTable:', e.message));
