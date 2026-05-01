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
  // Europa — Top 5
  39:2025, 140:2025, 135:2025, 78:2025, 61:2025,
  // Europa — Segundas divisiones top 5
  40:2025, 141:2025, 136:2025, 79:2025, 62:2025,
  // Europa — Copas y ligas medianas
  2:2025,  3:2025,   848:2025, 88:2025, 89:2025,
  94:2025, 95:2025,  // Portugal 1a y 2a
  179:2025,180:2025, // Scotland 1a y 2a
  144:2025,169:2025, // Belgium
  197:2025,          // Greece Super League 1
  203:2025,          // Turkey Süper Lig
  207:2025,          // Switzerland Super League
  210:2025,          // Croatia HNL
  218:2025,          // Austria Bundesliga
  235:2025,          // Russia Premier Liga
  333:2025,          // Ukraine Premier League
  98:2025,           // Japan J League
  // Europa — Más ligas
  119:2025,          // Denmark Superliga
  106:2025,          // Poland Ekstraklasa
  283:2025,          // Romania Liga I
  345:2025,          // Czech Fortuna Liga
  286:2025,          // Serbia Super Liga
  172:2025,          // Bulgaria First League
  113:2026,          // Sweden Allsvenskan
  // Medio Oriente / Asia / Africa
  307:2025,          // Saudi Pro League
  233:2025,          // Egypt Premier League
  318:2025,          // Cyprus First Division
  292:2026,          // South Korea K League 1
  // Sudamérica
  11:2026, 9:2026, 71:2026, 128:2026, 239:2026,
  262:2026,253:2026, 72:2026, 66:2026, 129:2026,
  263:2026,240:2026, 65:2026,
  // Colombia
  239:2026,          // Liga BetPlay Colombia
  // Resto
  671:2025,          // Azerbaijan Premier League
  103:2026,          // Norway Eliteserien
  // Competiciones internacionales
  4:2025,  5:2025,   480:2025,
  10:2026, 1:2026,   6:2026,  7:2026,  8:2026,
  29:2026, 32:2026,
};

const LEAGUE_IDS = new Set([
  // Top 5 Europa + segundas
  39,40,140,141,135,136,78,79,61,62,
  // Otras ligas europeas
  2,3,848,88,89,94,95,179,180,144,169,197,
  203,207,210,218,235,333, // Turkey,Switzerland,Croatia,Austria,Russia,Ukraine
  98,119,106,283,345,286,172,113, // Japan,Denmark,Poland,Romania,Czech,Serbia,Bulgaria,Sweden
  // Oriente Medio / Asia / Africa
  307,233,318,292,
  // Sudamérica
  11,9,71,128,239,262,253,72,66,129,263,240,65,
  // Otros
  671,103,
  // Competiciones internacionales
  1,4,5,6,7,8,10,29,32,480,
]);

const LEAGUE_MAP = {
  // Top 5 Europa
  39: { name:'Premier League',     country:'England'     },
  140:{ name:'LaLiga',             country:'Spain'       },
  135:{ name:'Serie A',            country:'Italy'       },
  78: { name:'Bundesliga',         country:'Germany'     },
  61: { name:'Ligue 1',            country:'France'      },
  // Top 5 segundas divisiones
  40: { name:'Championship',       country:'England'     },
  141:{ name:'LaLiga2',            country:'Spain'       },
  136:{ name:'Serie B',            country:'Italy'       },
  79: { name:'2.Bundesliga',       country:'Germany'     },
  62: { name:'Ligue 2',            country:'France'      },
  // Copas europeas
  2:  { name:'Champions League',   country:'Europe'      },
  3:  { name:'Europa League',      country:'Europe'      },
  848:{ name:'Conference League',  country:'Europe'      },
  // Ligas europeas medianas (IDs verificados con API-Football)
  88: { name:'Eredivisie',         country:'Netherlands' },
  89: { name:'Eerste Divisie',     country:'Netherlands' },
  94: { name:'Primeira Liga',      country:'Portugal'    },
  95: { name:'Segunda Liga',       country:'Portugal'    },
  179:{ name:'Scottish Premier',   country:'Scotland'    },
  180:{ name:'Scottish Championship', country:'Scotland' },
  144:{ name:'Jupiler Pro League', country:'Belgium'     },
  169:{ name:'Jupiler Pro',        country:'Belgium'     },
  197:{ name:'Super League 1',     country:'Greece'      },
  203:{ name:'Süper Lig',          country:'Turkey'      },  // ← ID correcto Turkey
  207:{ name:'Super League',       country:'Switzerland' }, // ← ID correcto Suiza
  210:{ name:'HNL',                country:'Croatia'     }, // ← ID correcto Croacia
  218:{ name:'Bundesliga',         country:'Austria'     }, // ← ID correcto Austria
  235:{ name:'Premier Liga',       country:'Russia'      },
  333:{ name:'Premier League',     country:'Ukraine'     }, // ← ID correcto Ucrania
  // Ligas europeas adicionales (verificadas)
  119:{ name:'Superliga',          country:'Denmark'     },
  106:{ name:'Ekstraklasa',        country:'Poland'      },
  283:{ name:'Liga I',             country:'Romania'     },
  345:{ name:'Fortuna Liga',       country:'Czech Rep.'  },
  286:{ name:'Super Liga',         country:'Serbia'      },
  172:{ name:'First League',       country:'Bulgaria'    },
  113:{ name:'Allsvenskan',        country:'Sweden'      },
  103:{ name:'Eliteserien',        country:'Norway'      },
  98: { name:'J League',           country:'Japan'       },
  // Oriente Medio / Asia / Africa (IDs verificados)
  307:{ name:'Pro League',         country:'Saudi Arabia'},// ← ID correcto Saudi
  233:{ name:'Premier League',     country:'Egypt'       }, // ← ID correcto Egypt
  318:{ name:'First Division',     country:'Cyprus'      }, // ← ID correcto Cyprus
  292:{ name:'K League 1',         country:'South Korea' }, // ← ID correcto Korea
  // Sudamérica
  11: { name:'Libertadores',       country:'South Am.'   },
  9:  { name:'Sudamericana',       country:'South Am.'   },
  71: { name:'Brasileirao',        country:'Brazil'      },
  72: { name:'Brasileirao B',      country:'Brazil'      },
  128:{ name:'Liga Argentina',     country:'Argentina'   },
  129:{ name:'Primera B Argentina',country:'Argentina'   },
  239:{ name:'Liga BetPlay',       country:'Colombia'    },
  240:{ name:'Torneo Águila',      country:'Colombia'    },
  66: { name:'Liga Colombia B',    country:'Colombia'    },
  262:{ name:'Liga MX',            country:'Mexico'      },
  263:{ name:'Ascenso MX',         country:'Mexico'      },
  253:{ name:'MLS',                country:'USA'         },
  // Competiciones internacionales
  1:  { name:'World Cup',          country:'Mundial'     },
  4:  { name:'Euro Championship',  country:'Europe'      },
  5:  { name:'Nations League',     country:'Europe'      },
  6:  { name:'WC Qualifiers',      country:'Mundial'     },
  7:  { name:'AFC Asian Cup',      country:'Asia'        },
  8:  { name:'Copa Africa',        country:'Africa'      },
  10: { name:'Amistosos Int.',     country:'Mundial'     },
  29: { name:'Nations League Play',country:'Europe'      },
  32: { name:'Eliminatorias CONMEBOL', country:'South Am.'},
  480:{ name:'Copa America',       country:'South Am.'   },
  // Otros
  671:{ name:'Premyer Liqa',       country:'Azerbaijan'  },
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
  // Turkey — ID correcto 203
  'super lig':203, 'superlig':203, 'turquia':203, 'turkey':203, 'liga turca':203, 'tff':203, 'turkiye':203,
  // Saudi Arabia — ID correcto 307
  'saudi pro league':307, 'saudi league':307, 'arabia saudita':307, 'saudi':307,
  // Suiza — ID correcto 207
  'switzerland':207, 'suiza':207, 'super league suiza':207, 'swiss super league':207,
  // Croacia — ID correcto 210
  'croacia':210, 'croatia':210, 'hnl':210, 'liga croacia':210,
  // Austria — ID correcto 218
  'austria':218, 'austria bundesliga':218, 'liga austria':218,
  // Ucrania — ID correcto 333
  'ucrania':333, 'ukraine':333, 'liga ucrania':333, 'premier league ucrania':333,
  // Egipto — ID correcto 233
  'liga egipto':233, 'egypt':233, 'egipto':233,
  // Chipre — ID correcto 318
  'chipre':318, 'primera division chipre':318, 'primera división chipre':318, 'cyprus':318,
  // Corea — ID correcto 292
  'k league':292, 'k-league':292, 'k league 1':292, 'corea':292, 'south korea':292,
  'j league':98, 'j1 league':98,
  'scottish premier':179, 'scottish premiership':179, 'scotland':179,
  'scottish championship':180,
  'championship':40,
  // Nuevas ligas europeas
  'dinamarca':119, 'denmark':119, 'superliga dinamarca':119, 'danish superliga':119,
  'polonia':106, 'poland':106, 'ekstraklasa':106,
  'rumania':283, 'romania':283, 'liga i':283, 'liga rumania':283,
  'republica checa':345, 'czech':345, 'fortuna liga':345, 'czech liga':345,
  'serbia':286, 'super liga serbia':286,
  'bulgaria':172, 'liga bulgaria':172, 'first league bulgaria':172, 'primera bulgaria':172,
  'suecia':113, 'sweden':113, 'allsvenskan':113,
  'segunda liga portugal':95, 'liga portugal 2':95,
  'segunda liga':95,
  'noruega':103, 'eliteserien':103, 'norway':103,
  'azerbaiyan':671, 'azerbaiyán':671, 'azerbaijan':671, 'liga azerbaiyan':671, 'liga azerbaiyán':671,
  'amistosos':10, 'amistoso':10, 'amistosos internacionales':10, 'friendly':10, 'friendlies':10, 'internacional':10,
  'world cup':1, 'mundial':1, 'copa mundial':1,
  'eliminatorias':6, 'qualifiers':6, 'wc qualifiers':6, 'clasificatorias':6,
  'eliminatorias conmebol':32, 'conmebol':32, 'eliminatorias sudamericanas':32,
  'copa africa':8, 'afcon':8,
  'asian cup':7, 'copa asia':7,
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
  1:105,                       // World Cup — máxima prioridad
  6:98, 32:97,                 // Eliminatorias mundialistas
  2:100,3:95,848:90,
  10:89,                       // Amistosos internacionales — entre UCL y ligas top
  39:88,140:87,135:86,78:85,61:84,
  11:80,9:78,
  88:70,94:69,207:68,144:67,169:66,
  71:65,262:64,128:63,239:62,253:61,
  40:55,141:54,136:53,79:52,62:51,
  240:45,72:44,66:43,129:42,263:41,89:40,
  // Ligas europeas medianas
  207:42, // Switzerland
  210:41, // Croatia
  218:40, // Austria
  119:39, // Denmark
  235:38, // Russia
  333:37, // Ukraine
  103:36, // Norway
  106:35, // Poland
  113:34, // Sweden
  283:33, // Romania
  286:32, // Serbia
  345:31, // Czech
  172:30, // Bulgaria
  // Otros
  671:25, // Azerbaijan
  // Asia / Oriente Medio / Africa
  307:28, // Saudi
  292:27, // K League
  233:26, // Egypt
  318:24, // Cyprus
};

// Ligas excluidas de picks automáticos (picks de hoy, picks en vivo)
// El bot sigue respondiendo si el usuario pregunta por estas ligas específicamente
const PICKS_EXCLUDE_LEAGUES = new Set([78, 94, 128]); // Bundesliga, Primeira Liga, Liga Argentina

// Buffer de cuota que se suma a la cuota real al mostrarla al usuario.
// Representa el margen de seguridad: si la cuota cayó por debajo de la real
// cuando el usuario intenta apostar, el pick ya no tiene valor → no entra.
// Internamente siempre usamos la cuota real para cálculos de EV.
const ODDS_DISPLAY_BUFFER = 0.15;

// Tasas históricas base de Over 2.5 y BTTS por liga
// Fuente: estadísticas 2023-2025, usadas para calibrar probabilidades
const LEAGUE_BASE_RATES = {
  39:  { over25: 56, btts: 61, name: 'Premier League' },    // alta
  140: { over25: 52, btts: 55, name: 'LaLiga' },            // media
  135: { over25: 54, btts: 57, name: 'Serie A' },           // media
  78:  { over25: 62, btts: 60, name: 'Bundesliga' },        // alta (pero 0% WR)
  61:  { over25: 53, btts: 54, name: 'Ligue 1' },           // media-baja
  2:   { over25: 61, btts: 63, name: 'Champions League' },  // alta
  3:   { over25: 58, btts: 59, name: 'Europa League' },     // media-alta
  71:  { over25: 55, btts: 58, name: 'Brasileirao' },       // media
  94:  { over25: 50, btts: 53, name: 'Primeira Liga' },     // baja (0% WR)
  128: { over25: 48, btts: 50, name: 'Liga Argentina' },    // baja (0% WR)
  262: { over25: 54, btts: 56, name: 'Liga MX' },
  239: { over25: 53, btts: 55, name: 'Liga BetPlay' },
  207: { over25: 59, btts: 60, name: 'Super Lig' },
  88:  { over25: 57, btts: 61, name: 'Eredivisie' },
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
  // Invalidar si el caché tiene más de 3 horas — los partidos pueden haber empezado o terminado
  const ageMs = Date.now() - new Date(entry.generadoAt).getTime();
  if (ageMs > 3 * 60 * 60 * 1000) return null;
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
  const cached = dateCache.get(date);
  // Caché válido por 30 minutos para reflejar cambios de status durante el día
  if (cached && (Date.now() - cached.ts) < 30 * 60 * 1000) return cached.data;
  const { data } = await API.get('/fixtures', { params: { date } });
  const result = data.response || [];
  dateCache.set(date, { data: result, ts: Date.now() });
  return result;
}

const FINISHED_STATUSES = new Set(['FT', 'AET', 'PEN', 'AWD', 'WO']);

async function getFixturesByDate(date) {
  const all = await fetchFixturesByDate(date);
  return all
    .filter(f => {
      if (!LEAGUE_IDS.has(f.league.id)) return false;
      if (FINISHED_STATUSES.has(f.fixture.status.short)) return false;
      // Verificar que la fecha real del partido (en hora Bogotá) sea hoy
      const fixtureDate = new Date(f.fixture.date).toLocaleDateString('en-CA', { timeZone: 'America/Bogota' });
      return fixtureDate === date;
    })
    .map(parseFixture);
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
  // La API siempre devuelve [local, visitante] en ese orden — marcamos explícitamente
  data.response.forEach((teamStats, idx) => {
    const key = teamStats.team.name;
    stats[key] = {};
    teamStats.statistics.forEach(s => { stats[key][s.type] = s.value; });
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

// IDs directos para equipos que la API no encuentra bien por búsqueda de texto
// Formato: 'alias normalizado' → teamId (número)
const TEAM_ID_OVERRIDES = {
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
  'corea':           'Korea Republic',
  'corea del sur':   'Korea Republic',
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

function scoreTeamResult(t, q, country = '', isNationalSearch = false) {
  const tname    = normalizeTeamName(t.team.name);
  const tcountry = (t.team.country || '').toLowerCase();
  const RESERVE  = /\b(ii|b|reserve|reserva|sub|youth|juvenil|u\d{2}|amateur|filial)\b/i;
  const WOMEN    = /\b(women|femenin[ao]|ladies|femmes|damen|vrouwen|mujer|femenino|fem\.?)\b/i;
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
  const aliasKey = name.toLowerCase().trim().normalize('NFD').replace(/[\u0300-\u036f]/g, '');

  // Lookup directo por ID (evita fallos de búsqueda de texto para equipos conocidos)
  if (TEAM_ID_OVERRIDES[aliasKey]) {
    const { data } = await API.get('/teams', { params: { id: TEAM_ID_OVERRIDES[aliasKey] } });
    if (data.response?.[0]) return data.response[0];
  }

  const stripped = name.trim().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const resolvedName = TEAM_ALIASES[aliasKey] || stripped;

  const { data } = await API.get('/teams', { params: { search: resolvedName } });
  const results = data.response || [];
  if (results.length === 0) return null;
  if (results.length === 1) return results[0];

  const q = normalizeTeamName(resolvedName);
  const country = countryHint.trim().toLowerCase();

  return results.sort((a, b) => scoreTeamResult(b, q, country, false) - scoreTeamResult(a, q, country, false))[0];
}

// Verifica si un equipo está jugando ahora, hoy o en los próximos 2 días
async function getTeamPlayingPriority(teamId) {
  try {
    // Live now
    const live = liveCache.raw || [];
    if (live.some(f => f.teams.home.id === teamId || f.teams.away.id === teamId)) return { priority: 3, label: '🔴 En vivo ahora' };
    // Today
    const today = todayDate();
    const todayData = dateCache.get(today);
    if (todayData) {
      const todayFixtures = todayData.data || [];
      if (todayFixtures.some(f => f.teams.home.id === teamId || f.teams.away.id === teamId)) return { priority: 2, label: '📅 Juega hoy' };
    }
    // Next 2 days
    for (let d = 1; d <= 2; d++) {
      const dt = new Date(); dt.setDate(dt.getDate() + d);
      const ds = dt.toISOString().split('T')[0];
      const cached = dateCache.get(ds);
      if (cached && (cached.data || []).some(f => f.teams.home.id === teamId || f.teams.away.id === teamId)) {
        return { priority: 1, label: `📆 Juega en ${d} día${d>1?'s':''}` };
      }
    }
  } catch {}
  return { priority: 0, label: '' };
}

async function findTeamWithButtons(chatId, name, countryHint = '', intent = null) {
  const aliasKey = name.toLowerCase().trim().normalize('NFD').replace(/[\u0300-\u036f]/g, '');

  // Lookup directo por ID (evita fallos de búsqueda de texto para equipos conocidos)
  if (TEAM_ID_OVERRIDES[aliasKey]) {
    const { data } = await API.get('/teams', { params: { id: TEAM_ID_OVERRIDES[aliasKey] } });
    if (data.response?.[0]) return data.response[0];
  }

  const stripped = name.trim().normalize('NFD').replace(/[\u0300-\u036f]/g, ''); // sin tildes
  const resolvedName = TEAM_ALIASES[aliasKey] || stripped;

  const { data } = await API.get('/teams', { params: { search: resolvedName } });
  const results = data.response || [];
  if (results.length === 0) return null;

  const q       = normalizeTeamName(resolvedName);
  const country = countryHint.trim().toLowerCase();

  // Filtrar equipos no profesionales (sub20, sub21, reservas, filiales, tier3+)
  const YOUTH_RE = /\b(u\d{2}|sub[\s-]?\d{2}|under[\s-]?\d{2}|ii|iii|iv|vi?|reserve|reserves|youth|juvenil|cadete|filial|amador|amateur)\b/i;
  const filtered = results.filter(t => !YOUTH_RE.test(t.team.name));
  const pool = filtered.length > 0 ? filtered : results; // fallback si todo es filtrado

  // Puntuar todos los resultados
  const scored = pool
    .map(t => ({ ...t, _score: scoreTeamResult(t, q, country, false) }))
    .filter(t => t._score > 0)
    .sort((a, b) => b._score - a._score)
    .slice(0, 5);

  if (scored.length === 0) return null;

  // Si el mejor resultado gana por más de 30 puntos → elegir automático
  const gap = scored.length > 1 ? scored[0]._score - scored[1]._score : 999;
  if (gap > 30 || scored.length === 1) return scored[0];

  // Hay ambigüedad → enriquecer candidatos con si juegan pronto
  const enriched = await Promise.all(scored.slice(0, 4).map(async t => {
    const playingInfo = await getTeamPlayingPriority(t.team.id);
    return { ...t, _priority: playingInfo.priority, _priorityLabel: playingInfo.label };
  }));

  // Ordenar: primero los que juegan pronto, luego por score
  enriched.sort((a, b) => b._priority - a._priority || b._score - a._score);

  // Si tras el ordenamiento hay uno claramente en vivo/hoy y los demás no → elegir automático
  if (enriched[0]._priority >= 2 && (enriched[1]?._priority || 0) === 0) return enriched[0];

  // Codificar intencion en callback_data (sin estado en memoria)
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

async function getLineups(fixtureId) {
  try {
    const { data } = await API.get('/fixtures/lineups', { params: { fixture: fixtureId } });
    const res = data.response || [];
    return res.map(t => ({
      team: t.team.name,
      formation: t.formation,
      startXI: (t.startXI || []).map(p => ({
        name: p.player.name,
        number: p.player.number,
        pos: p.player.pos,
      })),
      coach: t.coach?.name,
    }));
  } catch { return null; }
}

async function getInjuries(teamId, leagueId) {
  try {
    const season = LEAGUE_SEASONS[leagueId] || 2025;
    const { data } = await API.get('/injuries', { params: { team: teamId, league: leagueId, season } });
    const res = data.response || [];
    return res.slice(0, 5).map(i => ({
      player: i.player.name,
      type: i.player.type,
      reason: i.player.reason,
    }));
  } catch { return []; }
}

async function getRealOdds(fixtureId) {
  try {
    // Intentar primero sin filtro de bookmaker para obtener lo que esté disponible
    const { data } = await API.get('/odds', { params: { fixture: fixtureId } });
    // Tomar el primer bookmaker disponible (prioriza Bet365 id=6, sino cualquiera)
    const bookmakers = data.response?.[0]?.bookmakers || [];
    const bm = bookmakers.find(b => b.id === 6) || bookmakers[0];
    const bets = bm?.bets || [];
    const odds = {};
    for (const bet of bets) {
      if (bet.name === 'Match Winner') {
        odds.homeWin = parseFloat(bet.values.find(v => v.value === 'Home')?.odd) || null;
        odds.draw    = parseFloat(bet.values.find(v => v.value === 'Draw')?.odd) || null;
        odds.awayWin = parseFloat(bet.values.find(v => v.value === 'Away')?.odd) || null;
      }
      if (bet.name === 'Both Teams Score') {
        odds.bttsYes = parseFloat(bet.values.find(v => v.value === 'Yes')?.odd) || null;
        odds.bttsNo  = parseFloat(bet.values.find(v => v.value === 'No')?.odd) || null;
      }
      if (bet.name === 'Goals Over/Under') {
        odds.over05  = parseFloat(bet.values.find(v => v.value === 'Over 0.5')?.odd)  || null;
        odds.over15  = parseFloat(bet.values.find(v => v.value === 'Over 1.5')?.odd)  || null;
        odds.over25  = parseFloat(bet.values.find(v => v.value === 'Over 2.5')?.odd)  || null;
        odds.under25 = parseFloat(bet.values.find(v => v.value === 'Under 2.5')?.odd) || null;
        odds.over35  = parseFloat(bet.values.find(v => v.value === 'Over 3.5')?.odd)  || null;
        odds.under35 = parseFloat(bet.values.find(v => v.value === 'Under 3.5')?.odd) || null;
      }
      if (bet.name === 'Goals Over/Under First Half') {
        odds.over05_1T = parseFloat(bet.values.find(v => v.value === 'Over 0.5')?.odd) || null;
        odds.over15_1T = parseFloat(bet.values.find(v => v.value === 'Over 1.5')?.odd) || null;
      }
      if (bet.name === 'First Half Winner') {
        odds.homeWin_1T = parseFloat(bet.values.find(v => v.value === 'Home')?.odd) || null;
        odds.draw_1T    = parseFloat(bet.values.find(v => v.value === 'Draw')?.odd) || null;
        odds.awayWin_1T = parseFloat(bet.values.find(v => v.value === 'Away')?.odd) || null;
      }
      if (bet.name === 'Double Chance') {
        for (const v of bet.values) {
          if (v.value === '1X') odds.dc_1X = parseFloat(v.odd) || null;
          if (v.value === 'X2') odds.dc_X2 = parseFloat(v.odd) || null;
          if (v.value === '12') odds.dc_12 = parseFloat(v.odd) || null;
        }
      }
      if (bet.name === 'Asian Handicap') {
        for (const v of bet.values) {
          if (v.value === 'Home -0.5') odds.ah_home_m05 = parseFloat(v.odd) || null;
          if (v.value === 'Away -0.5') odds.ah_away_m05 = parseFloat(v.odd) || null;
          if (v.value === 'Home +0.5') odds.ah_home_p05 = parseFloat(v.odd) || null;
          if (v.value === 'Away +0.5') odds.ah_away_p05 = parseFloat(v.odd) || null;
        }
      }
      if (bet.name === 'Corners Over/Under') {
        for (const v of bet.values) {
          if (v.value === 'Over 7.5')   odds.cornersOver75  = parseFloat(v.odd) || null;
          if (v.value === 'Over 8.5')   odds.cornersOver85  = parseFloat(v.odd) || null;
          if (v.value === 'Over 9.5')   odds.cornersOver95  = parseFloat(v.odd) || null;
          if (v.value === 'Over 10.5')  odds.cornersOver105 = parseFloat(v.odd) || null;
          if (v.value === 'Over 11.5')  odds.cornersOver115 = parseFloat(v.odd) || null;
          if (v.value === 'Under 7.5')  odds.cornersUnder75 = parseFloat(v.odd) || null;
          if (v.value === 'Under 8.5')  odds.cornersUnder85 = parseFloat(v.odd) || null;
          if (v.value === 'Under 9.5')  odds.cornersUnder95 = parseFloat(v.odd) || null;
        }
      }
      if (bet.name === 'Cards Over/Under') {
        for (const v of bet.values) {
          if (v.value === 'Over 2.5')  odds.cardsOver25  = parseFloat(v.odd) || null;
          if (v.value === 'Over 3.5')  odds.cardsOver35  = parseFloat(v.odd) || null;
          if (v.value === 'Over 4.5')  odds.cardsOver45  = parseFloat(v.odd) || null;
          if (v.value === 'Over 5.5')  odds.cardsOver55  = parseFloat(v.odd) || null;
          if (v.value === 'Under 3.5') odds.cardsUnder35 = parseFloat(v.odd) || null;
          if (v.value === 'Under 4.5') odds.cardsUnder45 = parseFloat(v.odd) || null;
        }
      }
      if (bet.name === 'Goals Both Halves') {
        odds.goalsBothHalves = parseFloat(bet.values.find(v => v.value === 'Yes')?.odd) || null;
      }
    }
    return Object.keys(odds).length > 0 ? odds : null;
  } catch { return null; }
}

async function getLiveOdds(fixtureId) {
  try {
    const { data } = await API.get('/odds/live', { params: { fixture: fixtureId } });
    const bets = data.response?.[0]?.odds || [];
    const odds = {};
    for (const bet of bets) {
      if (bet.name === 'Match Winner') {
        odds.homeWin = parseFloat(bet.values.find(v => v.value === 'Home')?.odd) || null;
        odds.draw    = parseFloat(bet.values.find(v => v.value === 'Draw')?.odd) || null;
        odds.awayWin = parseFloat(bet.values.find(v => v.value === 'Away')?.odd) || null;
      }
      if (bet.name === 'Both Teams Score') {
        odds.bttsYes = parseFloat(bet.values.find(v => v.value === 'Yes')?.odd) || null;
        odds.bttsNo  = parseFloat(bet.values.find(v => v.value === 'No')?.odd) || null;
      }
      if (bet.name === 'Goals Over/Under') {
        odds.over05  = parseFloat(bet.values.find(v => v.value === 'Over 0.5')?.odd)  || null;
        odds.over15  = parseFloat(bet.values.find(v => v.value === 'Over 1.5')?.odd)  || null;
        odds.over25  = parseFloat(bet.values.find(v => v.value === 'Over 2.5')?.odd)  || null;
        odds.over35  = parseFloat(bet.values.find(v => v.value === 'Over 3.5')?.odd)  || null;
      }
      if (bet.name === 'Cards Over/Under') {
        odds.cardsOver35 = parseFloat(bet.values.find(v => v.value === 'Over 3.5')?.odd) || null;
        odds.cardsOver45 = parseFloat(bet.values.find(v => v.value === 'Over 4.5')?.odd) || null;
        odds.cardsOver55 = parseFloat(bet.values.find(v => v.value === 'Over 5.5')?.odd) || null;
      }
      if (bet.name === 'Corners Over/Under') {
        odds.cornersOver85  = parseFloat(bet.values.find(v => v.value === 'Over 8.5')?.odd)  || null;
        odds.cornersOver95  = parseFloat(bet.values.find(v => v.value === 'Over 9.5')?.odd)  || null;
        odds.cornersOver105 = parseFloat(bet.values.find(v => v.value === 'Over 10.5')?.odd) || null;
        odds.cornersOver115 = parseFloat(bet.values.find(v => v.value === 'Over 11.5')?.odd) || null;
        odds.cornersOver125 = parseFloat(bet.values.find(v => v.value === 'Over 12.5')?.odd) || null;
      }
    }
    return Object.keys(odds).length > 0 ? { source: 'live', ...odds } : null;
  } catch { return null; }
}

async function getApiPrediction(fixtureId) {
  try {
    const { data } = await API.get('/predictions', { params: { fixture: fixtureId } });
    const pred = data.response?.[0];
    if (!pred) return null;
    return {
      winner: pred.predictions?.winner?.name,
      winnerComment: pred.predictions?.winner?.comment,
      under_over: pred.predictions?.under_over,
      goals_home: pred.predictions?.goals?.home,
      goals_away: pred.predictions?.goals?.away,
      advice: pred.predictions?.advice,
      percent: pred.predictions?.percent,
    };
  } catch { return null; }
}

async function getTeamStats(teamId, leagueId) {
  const season = LEAGUE_SEASONS[leagueId] || 2026;

  function parseStats(r) {
    if (!r) return null;
    const played = r.fixtures?.played?.total || 0;
    const hasData = played > 0 || r.goals?.for?.total?.total > 0;
    if (!hasData) return null;
    // Marcar muestras pequeñas para que Claude no las use como argumento sólido
    const muestraReducida = played > 0 && played < 5;
    return {
      equipo:             r.team?.name,
      liga:               r.league?.name,
      temporada:          r.league?.season,
      partidosJugados:    played,
      ...(muestraReducida && { advertencia: `Muestra reducida (${played} partido${played>1?'s':''}) — promedios poco confiables` }),
      forma:              r.form?.replace(/W/g,'G').replace(/L/g,'P').replace(/D/g,'E').slice(-6).split('').join('-'),
      forma5:             (() => {
        const f5 = r.form?.replace(/W/g,'G').replace(/L/g,'P').replace(/D/g,'E').slice(-5) || '';
        const wins = (f5.match(/G/g) || []).length;
        const losses = (f5.match(/P/g) || []).length;
        const draws = (f5.match(/E/g) || []).length;
        const pts = wins * 3 + draws;
        return { forma: f5.split('').join('-'), victorias: wins, empates: draws, derrotas: losses, puntos: pts, nota: pts >= 12 ? 'Forma excelente' : pts >= 9 ? 'Forma buena' : pts >= 6 ? 'Forma regular' : 'Forma mala' };
      })(),
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

  // 1. Intentar con la liga y temporada del partido
  try {
    const { data } = await API.get('/teams/statistics', { params: { team: teamId, league: leagueId, season } });
    const stats = parseStats(data.response);
    if (stats) return stats;
  } catch {}

  // 2. Fallback ligero: máximo 2 intentos adicionales para no agotar la API
  // Solo aplica cuando la liga actual no tiene datos (ej: amistosos sin historial)
  const FALLBACK_LEAGUES = [
    { league: 10, season: 2025 }, // Amistosos Int. temporada anterior
    { league: 5,  season: 2024 }, // Nations League 2024
  ];

  for (const fb of FALLBACK_LEAGUES) {
    if (fb.league === leagueId && fb.season === season) continue;
    try {
      const { data } = await API.get('/teams/statistics', { params: { team: teamId, league: fb.league, season: fb.season } });
      const stats = parseStats(data.response);
      if (stats) return { ...stats, nota: `Referencia: ${stats.liga} ${stats.temporada}` };
    } catch {}
  }

  return null;
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
 * Extiende calcPoissonProbs con probabilidades de 1er tiempo y corners.
 * Necesario para pick selection multi-mercado sin depender del LLM.
 */
function calcExtendedProbs(homeFor, homeAgainst, awayFor, awayAgainst) {
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
  // Empíricamente: ~10 corners/partido cuando xG total ≈ 2.5
  // Fórmula calibrada: cornersLambda = 8 + (xGTotal - 2.0) * 3.0
  const xGTotal       = base.homeLambda + base.awayLambda;
  const cornersLambda = Math.max(5, Math.min(14, 8 + (xGTotal - 2.0) * 3.0));

  return {
    ...base,
    htHomeWin:      +(htHomeWin * 100).toFixed(1),
    htDraw:         +(htDraw    * 100).toFixed(1),
    htAwayWin:      +(htAwayWin * 100).toFixed(1),
    htOver05:       +(htOver05  * 100).toFixed(1),
    htOver15:       +(htOver15  * 100).toFixed(1),
    cornersLambda:  +cornersLambda.toFixed(1),
    cornersOver75:  +(poissonCDF_above(cornersLambda,  8) * 100).toFixed(1),
    cornersOver85:  +(poissonCDF_above(cornersLambda,  9) * 100).toFixed(1),
    cornersOver95:  +(poissonCDF_above(cornersLambda, 10) * 100).toFixed(1),
    cornersOver105: +(poissonCDF_above(cornersLambda, 11) * 100).toFixed(1),
    cornersOver115: +(poissonCDF_above(cornersLambda, 12) * 100).toFixed(1),
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
function calcLiveProjection(current, elapsed, total = 90) {
  if (!elapsed || elapsed <= 0) return null;
  const pace = current / elapsed;
  const projected = pace * total;
  const remaining = (total - elapsed) * pace;
  const confidence = elapsed >= 30 ? 'alta' : elapsed >= 15 ? 'media' : 'baja';
  return {
    current:    current,                  // corners/tarjetas YA ocurridos
    projected:  +projected.toFixed(1),   // total proyectado a 90 min
    remaining:  +remaining.toFixed(1),   // cuántos más se esperan
    pace:       +(pace * 90).toFixed(1),
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
function buildProbBlock(homeStats, awayStats, h2h = [], leagueId = null) {
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
    const probs = f._extendedProbs;
    const odds  = f.cuotasReales || {};
    if (!probs) continue;

    // ── Definición de mercados a evaluar ──────────────────────────────────────
    // minOdds son umbrales INTERNOS (cuota real de mercado).
    // Al usuario se muestra cuota real + ODDS_DISPLAY_BUFFER.
    const markets = [
      // ── Resultado FT
      { key: 'homeWin',     label: 'Victoria Local',       prob: probs.homeWin  / 100, oddsVal: odds.homeWin,  cat: 'result',    minOdds: 1.50, minProb: 0.52 },
      { key: 'awayWin',     label: 'Victoria Visitante',   prob: probs.awayWin  / 100, oddsVal: odds.awayWin,  cat: 'result',    minOdds: 1.65, minProb: 0.50 },
      // ── Goles FT
      { key: 'over25',      label: 'Más de 2.5 Goles',     prob: probs.over25   / 100, oddsVal: odds.over25,   cat: 'goals',     minOdds: 1.45, minProb: 0.52 },
      { key: 'over35',      label: 'Más de 3.5 Goles',     prob: probs.over35   / 100, oddsVal: odds.over35,   cat: 'goals',     minOdds: 1.55, minProb: 0.45 },
      { key: 'under25',     label: 'Menos de 2.5 Goles',   prob: probs.under25  / 100, oddsVal: odds.under25,  cat: 'goals',     minOdds: 1.45, minProb: 0.50 },
      // ── BTTS
      { key: 'btts',        label: 'Ambos Marcan (Sí)',     prob: probs.btts     / 100, oddsVal: odds.bttsYes,  cat: 'btts',      minOdds: 1.45, minProb: 0.55 },
      // ── 1er tiempo — goles
      { key: 'ht_over05',   label: 'Gol en el 1er Tiempo', prob: probs.htOver05 / 100, oddsVal: odds.over05_1T, cat: 'ht_goals', minOdds: 1.40, minProb: 0.55 },
      { key: 'ht_over15',   label: 'Más de 1.5 Goles 1T',  prob: probs.htOver15 / 100, oddsVal: odds.over15_1T, cat: 'ht_goals', minOdds: 1.55, minProb: 0.45 },
      // ── 1er tiempo — resultado
      { key: 'homeWin_1T',  label: 'Local Gana el 1er Tiempo',      prob: probs.htHomeWin / 100, oddsVal: odds.homeWin_1T, cat: 'ht_result', minOdds: 1.50, minProb: 0.45 },
      { key: 'awayWin_1T',  label: 'Visitante Gana el 1er Tiempo',  prob: probs.htAwayWin / 100, oddsVal: odds.awayWin_1T, cat: 'ht_result', minOdds: 1.65, minProb: 0.42 },
      // ── Corners FT
      { key: 'cornersOver75',  label: 'Corners Over 7.5',   prob: probs.cornersOver75  / 100, oddsVal: odds.cornersOver75,  cat: 'corners', minOdds: 1.45, minProb: 0.58 },
      { key: 'cornersOver85',  label: 'Corners Over 8.5',   prob: probs.cornersOver85  / 100, oddsVal: odds.cornersOver85,  cat: 'corners', minOdds: 1.45, minProb: 0.52 },
      { key: 'cornersOver95',  label: 'Corners Over 9.5',   prob: probs.cornersOver95  / 100, oddsVal: odds.cornersOver95,  cat: 'corners', minOdds: 1.50, minProb: 0.45 },
      { key: 'cornersOver105', label: 'Corners Over 10.5',  prob: probs.cornersOver105 / 100, oddsVal: odds.cornersOver105, cat: 'corners', minOdds: 1.55, minProb: 0.40 },
      { key: 'cornersUnder85', label: 'Corners Under 8.5',  prob: 1 - probs.cornersOver75 / 100, oddsVal: odds.cornersUnder85, cat: 'corners', minOdds: 1.45, minProb: 0.50 },
      { key: 'cornersUnder95', label: 'Corners Under 9.5',  prob: 1 - probs.cornersOver85 / 100, oddsVal: odds.cornersUnder95, cat: 'corners', minOdds: 1.45, minProb: 0.50 },
      // ── Tarjetas FT
      { key: 'cardsOver25',  label: 'Tarjetas Over 2.5',   prob: 0.72, oddsVal: odds.cardsOver25,  cat: 'cards', minOdds: 1.45, minProb: 0.60 },
      { key: 'cardsOver35',  label: 'Tarjetas Over 3.5',   prob: 0.55, oddsVal: odds.cardsOver35,  cat: 'cards', minOdds: 1.45, minProb: 0.48 },
      { key: 'cardsOver45',  label: 'Tarjetas Over 4.5',   prob: 0.38, oddsVal: odds.cardsOver45,  cat: 'cards', minOdds: 1.65, minProb: 0.42 },
      // ── Doble oportunidad (DC)
      { key: 'dc_1X', label: 'Doble Oportunidad 1X (Local o Empate)',    prob: (probs.homeWin + probs.draw) / 100, oddsVal: odds.dc_1X, cat: 'dc', minOdds: 1.45, minProb: 0.65 },
      { key: 'dc_X2', label: 'Doble Oportunidad X2 (Empate o Visitante)', prob: (probs.draw + probs.awayWin) / 100, oddsVal: odds.dc_X2, cat: 'dc', minOdds: 1.45, minProb: 0.65 },
      // ── Hándicap asiático
      { key: 'ah_home_m05', label: 'Hándicap Asiático Local -0.5',      prob: probs.homeWin / 100, oddsVal: odds.ah_home_m05, cat: 'ah', minOdds: 1.50, minProb: 0.60 },
      { key: 'ah_away_m05', label: 'Hándicap Asiático Visitante -0.5',  prob: probs.awayWin / 100, oddsVal: odds.ah_away_m05, cat: 'ah', minOdds: 1.50, minProb: 0.58 },
      // ── Goals Both Halves
      { key: 'goalsBothHalves', label: 'Goles en Ambas Mitades', prob: probs.btts * 0.75 / 100, oddsVal: odds.goalsBothHalves, cat: 'both_halves', minOdds: 1.55, minProb: 0.45 },
    ];

    for (const m of markets) {
      const o = m.oddsVal;
      if (!o || o <= 1) continue;                          // sin cuota
      if (o < (m.minOdds || 1.65)) continue;              // cuota muy baja
      if (!m.prob || m.prob < (m.minProb || 0.50)) continue; // prob insuficiente

      // Tarjetas: no hay modelo propio → solo evaluar si la cuota tiene valor implícito
      // usamos prob heurística directamente (ya definida en cada tarjeta arriba)

      const ev = calcEV(m.prob, o);
      // Permitimos hasta -3% de EV (el margen de las casas es ~5-8%, estar en -3% sigue siendo valor)
      if (ev === null || ev < -3) continue;

      // DNB absolutamente prohibido
      if (m.key.includes('dnb')) continue;

      // Stake basado en prob + EV
      let stake;
      if      (m.prob >= 0.80 && o >= 1.90) stake = 10;
      else if (m.prob >= 0.75 && o >= 1.80) stake = 9;
      else if (m.prob >= 0.70 && o >= 1.70) stake = 8;
      else if (m.prob >= 0.68 && ev > 5)    stake = 7;
      else if (m.prob >= 0.63)               stake = 6;
      else                                   stake = 5;

      // La cuota mostrada al usuario lleva el buffer (+0.15).
      // La cuota real (o) se usó para el cálculo de EV — es correcta internamente.
      const oddsDisplayed = +Math.round((o + ODDS_DISPLAY_BUFFER) * 20) / 20; // redondea a 0.05

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
        odds:         oddsDisplayed,   // cuota mostrada al usuario (real + buffer)
        ev:           ev,
        stake,
        xGLocal:      probs.homeLambda,
        xGVisitante:  probs.awayLambda,
        cornersLambda: probs.cornersLambda,
      });
    }
  }

  // Ordenar de mayor EV a menor
  return candidates.sort((a, b) => b.ev - a.ev);
}

/**
 * Selecciona N picks con diversidad de mercado y fixture.
 * Reglas: máx 1 pick por fixture, máx 1 pick por categoría (se relaja si faltan picks).
 */
function selectDiversePicks(candidates, count = 3) {
  const usedFixtures = new Set();
  const catCount     = {};
  const selected     = [];

  // Paso 1: estricto — max 1 por fixture, max 1 por categoría
  for (const c of candidates) {
    if (selected.length >= count) break;
    if (usedFixtures.has(c.fixtureId)) continue;
    if ((catCount[c.category] || 0) >= 1) continue;
    usedFixtures.add(c.fixtureId);
    catCount[c.category] = (catCount[c.category] || 0) + 1;
    selected.push(c);
  }

  // Paso 2: si faltan, relaja a max 2 por categoría (pero sigue max 1 por fixture)
  if (selected.length < count) {
    for (const c of candidates) {
      if (selected.length >= count) break;
      if (usedFixtures.has(c.fixtureId)) continue;
      if ((catCount[c.category] || 0) >= 2) continue;
      usedFixtures.add(c.fixtureId);
      catCount[c.category] = (catCount[c.category] || 0) + 1;
      selected.push(c);
    }
  }

  // Paso 3: si aún faltan, toma cualquier fixture distinto sin límite de categoría
  if (selected.length < count) {
    for (const c of candidates) {
      if (selected.length >= count) break;
      if (usedFixtures.has(c.fixtureId)) continue;
      usedFixtures.add(c.fixtureId);
      selected.push(c);
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

  if (impliedOdds < 1.45) return null;

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

FORMATO OBLIGATORIO:
⚡ *ALERTA DE GOL #[N]*
⚽ [local] [marcador] [visitante] | 🕐 Min [minuto] ([period])
🏆 [liga] — [country]
━━━━━━━━━━━━━━━━━━━
🎯 Pick: *[market]*
📊 Probabilidad: *[pGoal]%*
💰 Cuota estimada: *~[impliedOdds]*
⏱️ Apostar antes del min: *[minuto límite concreto]*
📈 Contexto: [razon — usa tiros a puerta, marcador, minuto para explicar por qué AHORA]
🏆 Stake: *[X]/10*
━━━━━━━━━━━━━━━━━━━

STAKE:
- Stake 8: prob > 72%
- Stake 7: prob 62-72%
- Stake 6: prob 55-62%
- Omite alertas con prob < 55% o cuota < 1.45

MINUTO LÍMITE: siempre concreto. En 1T apuesta antes del min 30. En HT decide antes de que empiece el 2T. En 2T nunca más allá del min 75.

IMPORTANTE:
- Si una alerta no cumple criterios (prob < 55% o cuota < 1.45), simplemente NO la incluyas. Nada de "omitida", nada de notas explicativas. Solo las alertas válidas.
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
- Asian Handicap (AH) y Draw No Bet local (DNB_HOME) en picks automáticos del día — mercados con historial de 10-22% win rate. Solo usar DNB_AWAY si la probabilidad supera 75%
- Asian Handicap en Bundesliga — historial negativo. En Bundesliga usar EXCLUSIVAMENTE Over 2.5 o BTTS (65% y 58% base esta temporada)
- Cualquier pick en Primeira Liga o Liga Argentina para picks automáticos del día — historial negativo en este sistema
- BTTS en La Liga cuando no son equipos ofensivos — la liga tiene solo 40% BTTS base, muy por debajo del umbral

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
10. Over goles 2T cuando el equipo tiene patrón de arrancar lento (ver LEAGUE_STATS_CONTEXT)

${LEAGUE_STATS_CONTEXT}

PROCESO DE ANÁLISIS OBLIGATORIO:
Para BTTS: % local marcó en casa + % visitante marcó fuera + % BTTS en H2H. Los 3 deben superar 68% (umbral estricto). Usa probBTTS_Combinada: debe superar 65%. Si uno no llega, NO es pick.
Para Corners: promedio local en casa + visitante fuera. Recomienda Over si total supera línea en +1.5.
Para HT: % local gana 1T en casa. Solo si supera 60%.
Para Tarjetas: suma promedios. Solo si supera línea en +1.
Para Over/Under goles: usa probOver25 y probOver35 del modelo. Si probOver25 > 65% con EV positivo, considera pick.
Para DNB: usa probDNB_Local o probDNB_Visitante. Solo si supera 72% para stake 7+.
  REGLA DNB OBLIGATORIA: DNB solo tiene valor cuando la cuota de victoria directa (1X2) del equipo es ≥ 2.00. Si el equipo gana a 1.30-1.70, el DNB queda en ~1.10-1.45 — SIN NINGÚN VALOR. En esos casos BUSCA OTRO MERCADO (goles, corners, BTTS, AH). Si la cuota de victoria directa es ≥ 2.00 y con DNB queda en ~1.65-1.85, ahí sí tiene sentido.
Para AH: solo -0.5. Solo si prob de victoria supera 70%. Nunca -1 ni -1.5.

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

INSTRUCCIONES PARA MOMENTUM EN VIVO:
Si el JSON incluye "momentumEnVivo", úsalo para detectar oportunidades en tiempo real:
- Si domina un equipo (score > 15) pero el marcador no lo refleja aún, considera apuesta al próximo gol de ese equipo.
- Si está equilibrado, prioriza mercados de corners o tarjetas sobre resultado.
- CORNERS EN VIVO: usa proyeccionCorners.current (ya ocurridos) y .remaining (esperados restantes).
  La línea válida debe ser: current + (remaining * 0.6) como mínimo. Si current ya supera o está cerca de la línea estándar (9.5/10.5), esa línea NO tiene valor — busca una línea mayor o descarta el mercado.
  Ejemplo correcto: 8 corners al min 55, remaining=5.1 → proyecta 13. Recomienda Over 11.5 o Over 12.5, NO Over 9.5.
- TARJETAS EN VIVO: misma lógica. Si ya van 3 tarjetas y projected=5, Over 3.5 no tiene valor. Busca Over 4.5.

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

REGLA DNB GLOBAL — APLICA PRE-PARTIDO Y EN VIVO:
- DNB solo cuando cuota victoria directa del equipo ≥ 2.00 (así el DNB queda ~1.65-1.85 y tiene valor real).
- Si el equipo favorito gana a 1.30-1.75, el DNB no tiene valor — busca BTTS, Over goles, corners u otro mercado.
- NUNCA recomendar DNB de un equipo que ya va ganando en el marcador (la cuota sería 1.05-1.20, sin ningún valor).
- NUNCA recomendar Match Winner (1X2) de un favorito que ya va ganando en el entretiempo — la cuota no tiene valor.

CRITERIO DE STAKE:
10/10: +80% probabilidad, cuota entre 1.90–2.20
9/10:  +75% probabilidad, cuota entre 1.80–2.20
8/10:  +70% probabilidad, cuota entre 1.70–2.20
7/10:  +68% probabilidad + EV positivo, cuota entre 1.65–2.20
1-6:   NUNCA publicar

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

REGLA DE PUBLICACIÓN (solo aplica a picks automáticos del día):
- Stake mínimo publicable: 7/10
- Si no hay partidos que cumplan stake 7+, emite solo los que sí cumplen (puede ser 1 o 2)
- Si ningún partido cumple los criterios: "⛔ Sin picks de valor hoy"

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

REGLAS DE FORMATO — OBLIGATORIAS SIN EXCEPCIÓN:
- Usa *texto* para negritas (Telegram Markdown)
- ⛔ NUNCA uses # ni ## ni ### — son headers de escritorio, se ven como texto plano en Telegram
- ⛔ NUNCA uses | columnas | ni tablas HTML
- ⛔ NUNCA menciones fuentes de datos, APIs, plataformas ni herramientas
- ⛔ NUNCA escribas disclaimers ni advertencias de responsabilidad al final
- ⛔ NUNCA muestres valores técnicos: xG, lambdaRem, EV%, score de momentum, pases — son internos
- ⛔ NUNCA muestres conteos de pases en los primeros minutos del partido
- La forma reciente SIEMPRE con guiones: *G-G-P-E-G* (máximo 6 resultados, nunca más)
- Si la muestra de partidos es menor a 5, NO uses ese promedio como argumento principal — menciónalo como "datos limitados (N partidos)"
- Si no hay picks válidos: escribe solo "⛔ Sin picks de valor. Mejor no apostar."

REGLAS DE PICKS — OBLIGATORIAS:
- MÁXIMO 2 picks por partido. Si ya diste 2 picks de un mismo partido, NO agregues más de ese partido
- BTTS Solo si: % local marcó en casa ≥65% Y % visitante marcó fuera ≥65% Y H2H BTTS ≥65%. Si alguno no llega, NO dar BTTS
- Asian Handicap MÁXIMO -0.5. NUNCA recomendar -1, -1.5 ni más — el riesgo no justifica el stake
- Stake 7: requiere probabilidad ≥68% + EV >+5%. Si no cumple ambas, bajar a stake 6 o no dar
- PROHIBIDO analizar partidos que ya empezaron hace más de 10 minutos (evitar picks sobre partidos en curso sin datos en vivo)

Responde en español. NUNCA inventes estadísticas. Usa SOLO los datos que recibes.`;

const PICKS_HOY_SYSTEM = `${TIPSTER_SYSTEM}

PROHIBICIONES ABSOLUTAS PARA PICKS DEL DÍA — NINGUNA EXCEPCIÓN:
- PROHIBIDO escribir análisis previo, lista de partidos descartados, razonamiento de por qué se descartó algo, o cualquier texto antes del primer pick. Ve DIRECTO al formato de pick.
- PROHIBIDO escribir frases como "Voy a analizar...", "ANÁLISIS PREVIO:", "descartado", "datos nulos", "EV positivo/negativo" o cualquier meta-comentario sobre el proceso de selección.
- PROHIBIDO mostrar EV%, xG, ni ningún valor técnico interno — esos son datos de calibración, no de salida.
- PROHIBIDO recomendar mercados AH_HOME, AH_AWAY o DNB_HOME en picks del día.
- PROHIBIDO incluir partidos de Bundesliga, Primeira Liga o Liga Argentina en picks del día.
- CUOTA MÍNIMA ABSOLUTA: 1.65. Cualquier cuota menor se descarta.
- CUOTA MÁXIMA ABSOLUTA: 2.30. Cualquier pick que solo exista a cuota mayor se descarta — no importa el EV teórico, con muestra reducida las probabilidades son poco confiables.
- STAKE MÍNIMO PUBLICABLE: 7/10. No emitas picks con stake 6 o menor.
- PARTIDOS CON MUESTRA REDUCIDA EN LA COMPETICIÓN ACTUAL: Si un equipo tiene menos de 5 partidos en esa copa/torneo específico, usa su liga doméstica como fuente principal de estadísticas. Si no hay NINGÚN dato adicional (ni liga doméstica ni H2H), descarta ese partido de los picks automáticos del día — el usuario puede preguntar por él directamente y el bot lo analiza con todos los factores disponibles.
- Calidad sobre cantidad: es mejor dar 1 pick sólido que 3 mediocres.

INSTRUCCIÓN ESPECIAL PARA PICKS DEL DÍA:
Emite entre 1 y 3 picks individuales (partidos diferentes) que cumplan stake 7+ + 1 APUESTA COMBINADA al final solo si hay 2+ picks válidos. Calidad sobre cantidad — es preferible dar 1 pick sólido que 3 débiles.

CONTEXTO PARA COMPETICIONES INTERNACIONALES (Libertadores, Sudamericana, Champions, Europa League, Copa del Mundo):
Cuando el partido es de copa con pocos juegos en esa competición, el razonamiento DEBE basarse en:
1. Forma de los últimos 10 partidos TOTALES (liga doméstica + copa combinados)
2. Rendimiento en liga local esta temporada — es la base estadística principal
3. H2H histórico entre ambos equipos (sin importar la competición)
4. Para Copa del Mundo o torneos sin historial reciente: usa ranking FIFA, nivel de la liga doméstica, y contexto del grupo
Los datos de la copa actual son complementarios. Si hay menos de 5 partidos en esa copa pero sí hay datos de liga doméstica → analiza y publica si el pick cumple stake 7+. Solo descarta si no existe ninguna fuente de datos confiable.

━━━━━━━━━━━━━━━━━━━
🎰 *COMBINADA DEL DÍA*
Mínimo 3 selecciones, máximo 5. Mercados y partidos distintos.

▸ [Local] vs [Visitante] → *[mercado]* | Cuota: *X.XX*
▸ [Local] vs [Visitante] → *[mercado]* | Cuota: *X.XX*
▸ [Local] vs [Visitante] → *[mercado]* | Cuota: *X.XX*

🏆 Stake combinada: *[X]/10*
💡 Cuota combinada estimada: *~X.XX*
━━━━━━━━━━━━━━━━━━━`;

// ─── Formatter-only prompt ────────────────────────────────────────────────────
// Usado cuando los picks ya fueron seleccionados matemáticamente.
// El LLM SOLO formatea — no selecciona, no descarta, no agrega picks.
const PICKS_HOY_FORMATTER_SYSTEM = `Eres el redactor del tipster profesional. Los picks ya fueron seleccionados por nuestro motor matemático de valor esperado. Tu función es ÚNICAMENTE escribir el texto final en español.

REGLAS ABSOLUTAS — SIN EXCEPCIÓN:
- Escribe EXACTAMENTE los picks que recibes, en el orden dado. NO añadas ni elimines ningún pick.
- NO cuestiones ni justifiques las selecciones.
- NO menciones EV%, xG, lambda, cornersLambda ni ningún valor técnico — son internos.
- NO uses # ## ### (se ven mal en Telegram).
- NO inventes estadísticas — usa solo lo que viene en statsLocal / statsVisitante.
- Si statsLocal o statsVisitante son null, escribe "datos limitados" en lugar de inventar.

FORMATO OBLIGATORIO para cada pick:
🌍 [country] — [liga]
⚽ [local] vs [visitante] | ⏰ [hora]
━━━━━━━━━━━━━━━━━━━

📊 *ESTADÍSTICAS CLAVE*
▸ [local] anota en casa: *[golesAnotadosHome]* por partido
▸ [visitante] anota fuera: *[golesAnotadosAway]* por partido
▸ [local] recibe en casa: *[golesRecibidosHome]* por partido
▸ Forma reciente [local]: *[forma5.forma]*
▸ Forma reciente [visitante]: *[forma5.forma]*

🎯 *PICK: [marketLabel]*
┌ Selección: [descripción exacta del pick]
├ Razonamiento: [1-2 líneas con el dato concreto de las estadísticas que lo justifica]
├ Probabilidad: *[prob]%*
├ 🏆 Stake: *[stake]/10*
├ 💡 Cuota mínima: *[odds]*
└ ⚠️ Riesgo: [1 línea máximo]

━━━━━━━━━━━━━━━━━━━

Después de todos los picks individuales, añade la combinada:

━━━━━━━━━━━━━━━━━━━
🎰 *COMBINADA DEL DÍA*
▸ [local] vs [visitante] → *[marketLabel]* | Cuota: *[odds]*
[una línea por pick]

🏆 Stake combinada: *3/10*
💡 Cuota combinada: *~[producto de todas las cuotas redondeado a 2 decimales]*
━━━━━━━━━━━━━━━━━━━

Responde en español. Sé conciso y directo.`;

const INPLAY_SYSTEM = `${TIPSTER_SYSTEM}

INSTRUCCIÓN ESPECIAL IN-PLAY:
Analiza el marcador, minuto y estadísticas en tiempo real.
Indica el tiempo restante estimado y cuándo actuar.

ANÁLISIS DE MOMENTUM (campo "momentumEnVivo"):
- score > 15: el local domina → favorece apuestas al local (siguiente gol, AH)
- score < -15: el visitante domina → favorece apuestas al visitante
- score entre -15 y 15: partido equilibrado → enfócate en corners y tarjetas
- intensity > 30: dominio muy claro → stake más alto permitido

${LEAGUE_STATS_CONTEXT}

PROYECCIONES EN TIEMPO REAL:
- CORNERS EN VIVO: proyeccionCorners tiene {current, projected, remaining, confidence}.
  Regla crítica: la línea mínima válida es current + 4. Si ya van 9 corners → línea mínima Over 13.5.
  Si current=8 y projected=13 → recomienda Over 12.5 o 13.5, NUNCA Over 9.5 ni Over 10.5 (ya casi se alcanzaron).
  Solo con confidence "alta" (min 30 min jugados) para picks de stake 7+.
  Si la línea válida (current+4) no tiene cuota atractiva (≥ 1.65), DESCARTA el mercado de corners.
- TARJETAS EN VIVO: línea mínima válida es current + 3. Si ya van 4 tarjetas → línea mínima Over 7.5.
  Si ya van 4 tarjetas al HT y proyecta 7 al final → recomienda Over 6.5 máximo, NUNCA Over 4.5 (ya casi garantizado).
  Si la línea válida (current+3) no tiene cuota ≥ 1.65, DESCARTA el mercado de tarjetas.
- Solo usa proyecciones con confidence "alta" para picks de stake 7+.
- CUOTAS EN VIVO: el campo cuotasVivo contiene cuotas reales del mercado en ese momento. USA ESAS cuotas, no inventes. Si cuotasVivo es null, indica la cuota como "verificar en casa de apuestas" en lugar de inventar un número.

PROHIBICIONES IN-PLAY ADICIONALES:
- PROHIBIDO usar # ## ### en el formato de respuesta.
- PROHIBIDO mostrar el valor numérico del score de momentum (solo el label: "Domina local", "Equilibrado", etc.).
- PROHIBIDO mostrar xG, lambdaRem, EV% u otros valores técnicos internos.
- PROHIBIDO recomendar DNB o Match Winner de un equipo que ya va ganando en el marcador.
- PROHIBIDO recomendar Over corners/tarjetas con línea ≤ current + 2.

FORMATO ADICIONAL IN-PLAY:
⏰ Actúa antes del min: [XX]
📈 Ritmo corners: [proyeccionCorners.pace] corners/90min → proyectados [proyeccionCorners.projected] al final
📊 Momentum: [momentumEnVivo.label]`;

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
- Para tarjetas: suma de amarillas totales de ambos equipos / partidos jugados
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

📐 ANÁLISIS DE MERCADOS
▸ Resultado FT: [Local X%] [Empate X%] [Visitante X%]
▸ Over 2.5 goles: X% | Over 3.5: X%
▸ BTTS: X%
▸ Corners proyectados: ~X | Over 10.5 válido: sí/no
▸ Tarjetas proyectadas: ~X | Over 4.5 válido: sí/no
▸ Goles 1T: Over 0.5 (X%) | Over 1.5 (X%)

🎯 PICKS (mínimo 3, de mercados DISTINTOS — nunca 3 picks del mismo mercado):

🎯 PICK 1: [nombre del mercado]
┌ Selección: [selección exacta]
├ Razonamiento: [2-3 líneas de contexto real, usa los datos del JSON]
├ Probabilidad: X%
├ 🏆 Stake: X/10
├ 💡 Cuota mínima: X.XX
└ ⚠️ Riesgo: [factor de fallo principal]

[repetir para PICK 2, PICK 3]

━━━━━━━━━━━━━━━━━━━
⛔ MERCADOS DESCARTADOS (menciona 2-3 que evaluaste pero no recomiendas y por qué)

REGLAS IRROMPIBLES:
- Cuota mínima 1.65 para cualquier pick
- PROHIBIDO DNB bajo ninguna circunstancia
- PROHIBIDO Over 1.5 FT (cuota siempre < 1.50)
- PROHIBIDO mostrar EV%, lambdas, campos técnicos internos
- PROHIBIDO usar # ## ### (se ven mal en Telegram)
- Para picks de resultado (1X2): SOLO si la probabilidad de ese resultado supera 60% Y la cuota es ≥ 1.75
- Si la victoria del favorito es a 1.30-1.70: BUSCA OTRO MERCADO (corners, tarjetas, BTTS, goles 2T)
- Usa *texto* para negritas (un solo asterisco, estilo Telegram)

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

  // Top 12 partidos por prioridad de liga
  const selected = [...fixtures]
    .sort((a, b) => (LEAGUE_PRIORITY[b.leagueId] || 0) - (LEAGUE_PRIORITY[a.leagueId] || 0))
    .slice(0, 12);

  console.log(`✅ Candidatos seleccionados: ${selected.length} partidos`);
  await bot.sendMessage(chatId, `📊 ${fixtures.length} partidos identificados. Recopilando estadísticas...`);

  // Fetch team stats en batches de 4 para no saturar la API
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

  // Construir enriched con probabilidades extendidas (HT + corners)
  // Usar promedios de liga como fallback cuando no hay stats del equipo
  const enriched = selected.map((f, i) => {
    const hStats = statsResults[i * 2].status === 'fulfilled' ? statsResults[i * 2].value : null;
    const aStats = statsResults[i * 2 + 1].status === 'fulfilled' ? statsResults[i * 2 + 1].value : null;

    // Fallback a promedios europeos cuando no hay stats: 1.3 goles en casa, 1.1 fuera
    const hFor  = parseFloat(hStats?.golesAnotadosHome) || 1.3;
    const hAgt  = parseFloat(hStats?.golesRecibidosHome) || 1.1;
    const aFor  = parseFloat(aStats?.golesAnotadosAway) || 1.0;
    const aAgt  = parseFloat(aStats?.golesRecibidosAway) || 1.3;
    // Siempre calculamos probs — con stats reales o con promedios de referencia
    const extProbs = calcExtendedProbs(hFor, hAgt, aFor, aAgt);

    return {
      fixtureId:      f.fixtureId,
      liga:           f.leagueName,
      country:        f.country,
      local:          f.homeTeam,
      visitante:      f.awayTeam,
      hora:           formatHour(f.date),
      fechaPartido:   f.date,
      statsLocal:     hStats,
      statsVisitante: aStats,
      _extendedProbs: extProbs,
      _statsSource:   (hStats && aStats) ? 'real' : hStats ? 'local_only' : aStats ? 'away_only' : 'fallback',
    };
  });

  // Fetch cuotas reales para todos los partidos
  await bot.sendMessage(chatId, `📈 Consultando cuotas reales (goles, corners, tarjetas, HT...)...`);
  const oddsResults = await Promise.allSettled(selected.map(f => getRealOdds(f.fixtureId)));
  let conOdds = 0;
  for (let i = 0; i < enriched.length; i++) {
    const odds = oddsResults[i].status === 'fulfilled' ? oddsResults[i].value : null;
    if (odds) { enriched[i].cuotasReales = odds; conOdds++; }
  }

  await bot.sendMessage(chatId, `🧮 Motor matemático calculando EV por mercado...`);

  // ── Debug logging detallado ───────────────────────────────────────────────
  console.log(`📊 DEBUG picks motor:`);
  console.log(`   Partidos analizados: ${enriched.length}`);
  console.log(`   Con cuotas reales: ${conOdds}/${enriched.length}`);
  const sinStats = enriched.filter(e => e._statsSource === 'fallback').length;
  console.log(`   Sin stats reales (usando fallback): ${sinStats}`);

  // ── Selección matemática de picks ────────────────────────────────────────
  const candidates = buildPickCandidates(enriched);
  const topPicks   = selectDiversePicks(candidates, 3);

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

  let picksText;

  if (topPicks.length >= 2) {
    // ── NUEVO: LLM solo formatea picks ya elegidos matemáticamente
    picksText = await sonnet(
      PICKS_HOY_FORMATTER_SYSTEM,
      `Fecha: ${today} (hora Colombia)\n\nPICKS SELECCIONADOS POR EL MOTOR MATEMÁTICO — NO añadas ni elimines ninguno:\n\n${JSON.stringify(topPicks, null, 2)}`
    );
  } else {
    // ── FALLBACK: si no hay suficientes picks con EV positivo, usar análisis LLM clásico
    // (ocurre cuando no hay cuotas disponibles o el mercado no tiene valor)
    console.log(`⚠️ Insuficientes picks con EV+ (${topPicks.length}) — fallback a análisis LLM`);
    const enrichedForLLM = enriched.map(e => ({
      ...e,
      _extendedProbs: undefined, // no enviar al LLM — es interno
      probabilidadesCalculadas: e._extendedProbs ? buildProbBlock(e.statsLocal, e.statsVisitante, [], selected.find(f => f.fixtureId === e.fixtureId)?.leagueId) : null,
    }));
    const predResults = await Promise.allSettled(selected.map(f => getApiPrediction(f.fixtureId)));
    for (let i = 0; i < enrichedForLLM.length; i++) {
      const pred = predResults[i].status === 'fulfilled' ? predResults[i].value : null;
      if (pred) enrichedForLLM[i].prediccionAPI = pred;
    }
    picksText = await sonnet(
      PICKS_HOY_SYSTEM,
      `Partidos del día ${today} (hora Colombia). DATOS REALES:\n\n${JSON.stringify(enrichedForLLM, null, 2)}\n\nEmite 3 picks individuales + 1 combinada. REGLAS IRROMPIBLES: (1) CUOTA MÍNIMA ABSOLUTA 1.65 (cuota real de mercado) — cualquier pick con cuota real menor se DESCARTA SIN EXCEPCIÓN. Al mostrar la cuota al usuario, súmale 0.15 (ejemplo: cuota real 1.80 → muestra 1.95). (2) PROHIBIDO DNB, (3) PROHIBIDO 1X2 directo a cuota real menor de 1.70. (4) Prioriza corners Over/Under, Over 1.5 goles 1T, BTTS con ambos marcando >65% en casa/fuera, Over 3.5 si ambos son goleadores. Si no hay picks con cuota real ≥1.65, responde SOLO: ⛔ Sin picks de valor real hoy.`
    );
  }

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
    if (i + 4 < statsPairs.length) await new Promise(r => setTimeout(r, 5000));
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
      statsLocal:   homeStats,
      statsVisitante: awayStats,
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
  const all = await fetchFixturesByDate(today);

  const STARTED_STATUSES_L = new Set(['1H','HT','2H','ET','P','BT','LIVE','INT']);
  let fixtures = leagueId
    ? all.filter(f => f.league.id === leagueId && !STARTED_STATUSES_L.has(f.fixture.status.short)).map(parseFixture)
    : all.filter(f => {
        const n = (f.league.name || '').toLowerCase();
        return n.includes(leagueName.toLowerCase()) && !STARTED_STATUSES_L.has(f.fixture.status.short);
      }).map(parseFixture);

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
    if (i + 4 < statsPairs.length) await new Promise(r => setTimeout(r, 6000));
  }

  // Enriquecer con probabilidades extendidas
  const enriched = fixtures.map((f, i) => {
    const hStats = statsResults[i * 2]?.status === 'fulfilled' ? statsResults[i * 2].value : null;
    const aStats = statsResults[i * 2 + 1]?.status === 'fulfilled' ? statsResults[i * 2 + 1].value : null;
    const hFor   = parseFloat(hStats?.golesAnotadosHome) || 0;
    const hAgt   = parseFloat(hStats?.golesRecibidosHome) || 0;
    const aFor   = parseFloat(aStats?.golesAnotadosAway) || 0;
    const aAgt   = parseFloat(aStats?.golesRecibidosAway) || 0;
    const extProbs = (hFor > 0 || aFor > 0) ? calcExtendedProbs(hFor, hAgt, aFor, aAgt) : null;
    return {
      fixtureId:      f.fixtureId,
      liga:           f.leagueName,
      country:        f.country,
      local:          f.homeTeam,
      visitante:      f.awayTeam,
      hora:           formatHour(f.date),
      fechaPartido:   f.date,
      statsLocal:     hStats,
      statsVisitante: aStats,
      _extendedProbs: extProbs,
    };
  });

  // Cuotas reales
  await bot.sendMessage(chatId, `📈 Consultando cuotas (goles, corners, tarjetas, HT)...`);
  const oddsResultsL = await Promise.allSettled(fixtures.map(f => getRealOdds(f.fixtureId)));
  for (let i = 0; i < enriched.length; i++) {
    const odds = oddsResultsL[i].status === 'fulfilled' ? oddsResultsL[i].value : null;
    if (odds) enriched[i].cuotasReales = odds;
  }

  await bot.sendMessage(chatId, `🧮 Motor matemático calculando EV...`);

  // Selección matemática de picks
  const candidatesL = buildPickCandidates(enriched);
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

  // Guardar en caché
  setPicksCache(cacheScope, picksText, enriched.map(f => f.fixtureId));
  await sendLong(chatId, `📅 *${displayName} — ${today}*\n\n${picksText}`, { parse_mode: 'Markdown' });
  recordPicks(picksText, enriched.map(f => ({ fixtureId: f.fixtureId, local: f.local, visitante: f.visitante, liga: f.liga, fechaPartido: f.fechaPartido }))).catch(e => console.error('recordPicks:', e.message));
}

async function handlePartido(chatId, teamName, countryHint = '') {
  await bot.sendMessage(chatId, `🔍 Buscando *${teamName}* en nuestra base de datos...`, { parse_mode: 'Markdown' });

  const teamData = await findTeamWithButtons(chatId, teamName, countryHint, { intencion: 'partido_especifico' });
  if (!teamData) return bot.sendMessage(chatId, `❌ No encontré el equipo "${teamName}" en nuestra base de datos.`);
  if (teamData === 'PENDING') return; // esperando selección del usuario

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
  const DOMESTIC_LEAGUE = { 2: 140, 3: 140, 135: 135, 78: 78, 61: 61, 94: 94 }; // CL/EL → LaLiga, etc.
  // Para CL/EL buscamos league doméstica del equipo local (heurística: usamos homeId)
  const isEuropean = [2, 3, 848].includes(leagueId);
  const domLeagueId = isEuropean ? null : null; // no necesitamos hardcodear, lo hacemos con ambos equipos

  const requests = [
    getH2H(homeId, awayId),
    getTeamStats(homeId, leagueId),
    getTeamStats(awayId, leagueId),
  ];
  if (isLive) requests.push(getFixtureStatistics(nextRaw.fixture.id));
  // Para ligas europeas (CL/EL): buscar también stats de LaLiga/Bundesliga del home team
  // Detectamos la liga doméstica probable buscando stats en las ligas más comunes
  if (isEuropean) {
    requests.push(
      // Intenta LaLiga(140), Bundesliga(78), PremierLeague(39), SerieA(135), Ligue1(61), Eredivisie(88), Liga NOS(94)
      Promise.any([140,78,39,135,61,88,94].map(lid => getTeamStats(homeId, lid).then(s => s ? s : Promise.reject()))).catch(() => null),
      Promise.any([140,78,39,135,61,88,94].map(lid => getTeamStats(awayId, lid).then(s => s ? s : Promise.reject()))).catch(() => null)
    );
  }

  const [h2hRes, homeStatsRes, awayStatsRes, liveStatsRes, homedomRes, awaydomRes] = await Promise.allSettled(requests);

  const h2hData         = h2hRes.status === 'fulfilled'       ? h2hRes.value       : [];
  const homeStatsData   = homeStatsRes.status === 'fulfilled'  ? homeStatsRes.value  : null;
  const awayStatsData   = awayStatsRes.status === 'fulfilled'  ? awayStatsRes.value  : null;
  const liveStatsData   = (isLive && liveStatsRes?.status === 'fulfilled') ? liveStatsRes.value : null;
  const homeDomStats    = isEuropean && homedomRes?.status === 'fulfilled' ? homedomRes.value : null;
  const awayDomStats    = isEuropean && awaydomRes?.status === 'fulfilled' ? awaydomRes.value : null;

  // Detectar contexto de eliminatoria (CL/EL Knockout o ronda específica)
  const round = nextRaw.league?.round || '';
  const isKnockout = /quarter|semi|final|round of|octavos|cuartos|semis|knockout/i.test(round);
  let contextoEliminatoria = null;
  if (isKnockout) {
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
  // Para europeas: usar stats domésticas si hay (más representativas)
  const homeBase = homeDomStats || homeStatsData;
  const awayBase = awayDomStats || awayStatsData;
  const probBlock = buildProbBlock(homeBase, awayBase, h2hData, leagueId);

  // Momentum y proyecciones en vivo
  const momentum   = isLive ? calcLiveMomentum(liveStatsData, homeTeam, awayTeam) : null;
  const elapsed    = nextRaw.fixture?.status?.elapsed || 0;
  const homeCorners= liveStatsData ? (homeStats(liveStatsData)?.['Corner Kicks'] ?? 0) : 0;
  const awayCorners= liveStatsData ? (awayStats(liveStatsData)?.['Corner Kicks'] ?? 0) : 0;
  const cornersProj= isLive && elapsed > 0
    ? calcLiveProjection(homeCorners + awayCorners, elapsed)
    : null;
  const homeCards  = liveStatsData
    ? ((homeStats(liveStatsData)?.['Yellow Cards'] ?? 0) + (homeStats(liveStatsData)?.['Red Cards'] ?? 0))
    : 0;
  const awayCards  = liveStatsData
    ? ((awayStats(liveStatsData)?.['Yellow Cards'] ?? 0) + (awayStats(liveStatsData)?.['Red Cards'] ?? 0))
    : 0;
  const cardsProj  = isLive && elapsed > 0
    ? calcLiveProjection(homeCards + awayCards, elapsed)
    : null;

  const analysisData = {
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
    },
    h2h:            h2hData,
    bttsEnH2H:      h2hData.filter(m => m.btts).length,
    statsLocal:     homeStatsData,
    statsVisitante: awayStatsData,
    ...(homeDomStats && { statsLocalLigaDomestica:   homeDomStats }),
    ...(awayDomStats && { statsVisitanteLigaDomestica: awayDomStats }),
    estadisticasVivo: liveStatsData,
    ...(contextoEliminatoria && { contextoEliminatoria }),
    ...(probBlock   && { probabilidadesCalculadas: probBlock }),
    ...(momentum    && { momentumEnVivo: momentum }),
    ...(cornersProj && { proyeccionCorners: cornersProj }),
    ...(cardsProj   && { proyeccionTarjetas: cardsProj }),
  };

  await bot.sendMessage(chatId, '⚡ Procesando análisis profundo...');

  // ── En vivo → INPLAY_SYSTEM (necesita contexto del partido activo) ──────────
  if (isLive) {
    const liveOdds = await getLiveOdds(nextRaw.fixture.id).catch(() => null);
    if (liveOdds) analysisData.cuotasVivo = liveOdds;
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

  // Usar stats domésticas para el motor si están disponibles (más partidos = más fiable)
  const hFor = parseFloat((homeDomStats || homeStatsData)?.golesAnotadosHome) || 1.3;
  const hAgt = parseFloat((homeDomStats || homeStatsData)?.golesRecibidosHome) || 1.1;
  const aFor = parseFloat((awayDomStats || awayStatsData)?.golesAnotadosAway) || 1.0;
  const aAgt = parseFloat((awayDomStats || awayStatsData)?.golesRecibidosAway) || 1.3;
  const extProbs = calcExtendedProbs(hFor, hAgt, aFor, aAgt);

  const fixtureForEngine = {
    fixtureId:      nextRaw.fixture.id,
    liga:           nextRaw.league.name,
    country:        nextRaw.league.country,
    local:          homeTeam,
    visitante:      awayTeam,
    hora:           formatHour(nextRaw.fixture.date),
    statsLocal:     homeDomStats || homeStatsData,
    statsVisitante: awayDomStats || awayStatsData,
    _extendedProbs: extProbs,
    cuotasReales:   realOdds || undefined,
  };

  const partidoCandidates = buildPickCandidates([fixtureForEngine]);
  const partidoPicks      = selectDiversePicks(partidoCandidates, 2);
  console.log(`🎯 handlePartido — candidatos JS: ${partidoCandidates.length} | picks: ${partidoPicks.length} | cuotas: ${realOdds ? 'sí' : 'no'} | knockout: ${isKnockout}`);

  const season = LEAGUE_SEASONS[leagueId] || 2025;
  let analysis;
  if (partidoPicks.length >= 1 && realOdds && !isKnockout) {
    // Motor JS encontró valor en partido normal → LLM solo formatea
    analysis = await sonnet(
      PICKS_HOY_FORMATTER_SYSTEM,
      `Partido: ${homeTeam} vs ${awayTeam} | ${nextRaw.league.name}\n\nPICKS SELECCIONADOS — NO cambies ni añadas:\n\n${JSON.stringify(partidoPicks, null, 2)}\n\nH2H reciente:\n${JSON.stringify(h2hData.slice(0, 5), null, 2)}\n\n${contextoEliminatoria ? 'CONTEXTO ELIMINATORIA:\n'+JSON.stringify(contextoEliminatoria) : ''}`
    );
  } else {
    // Análisis profundo completo (partido especial, eliminatoria, o sin picks automáticos)
    analysis = await sonnet(
      PARTIDO_DEEP_SYSTEM,
      `Analiza este partido en profundidad (temporada ${season}):\n\n${JSON.stringify(analysisData, null, 2)}`
    );
  }

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
    const homeCorners = liveStats ? (homeStats(liveStats)?.['Corner Kicks'] ?? 0) : 0;
    const awayCorners = liveStats ? (awayStats(liveStats)?.['Corner Kicks'] ?? 0) : 0;
    const cornersProj = elapsed > 0
      ? calcLiveProjection(homeCorners + awayCorners, elapsed)
      : null;

    // Proyección de tarjetas
    const homeCards = liveStats
      ? ((homeStats(liveStats)?.['Yellow Cards'] ?? 0) + (homeStats(liveStats)?.['Red Cards'] ?? 0))
      : 0;
    const awayCards = liveStats
      ? ((awayStats(liveStats)?.['Yellow Cards'] ?? 0) + (awayStats(liveStats)?.['Red Cards'] ?? 0))
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

  // Cuotas en vivo reales para cada partido
  await bot.sendMessage(chatId, '📈 Consultando cuotas en vivo...');
  const liveOddsResults = await Promise.allSettled(toAnalyze.map(f => getLiveOdds(f.fixtureId)));
  for (let i = 0; i < enriched.length; i++) {
    const lo = liveOddsResults[i].status === 'fulfilled' ? liveOddsResults[i].value : null;
    if (lo) enriched[i].cuotasVivo = lo;
  }

  await bot.sendMessage(chatId, '🎯 Identificando picks de valor...');
  const analysis = await sonnet(
    INPLAY_SYSTEM,
    `DATOS REALES EN VIVO de API-Football:\n\n${JSON.stringify(enriched, null, 2)}\n\nAnaliza y da picks de valor in-play. USA cuotasVivo para las cuotas reales. Si cuotasVivo es null para un partido, indica "verificar cuota en casa de apuestas".`
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
    const liveStats      = liveStatsResults[i].status  === 'fulfilled' ? liveStatsResults[i].value  : null;
    const homeStatsData  = homeStatsResults[i].status  === 'fulfilled' ? homeStatsResults[i].value  : null;
    const awayStatsData  = awayStatsResults[i].status  === 'fulfilled' ? awayStatsResults[i].value  : null;

    const alert = calcGoalAlert(parsed, liveStats, homeStatsData, awayStatsData);
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

async function handleVerPlanes(chatId, telegramId) {
  const tid = telegramId || chatId;
  const linkVip15 = wompiLink(WOMPI_LINKS.vip15, tid, 'vip15');
  const linkVip30 = wompiLink(WOMPI_LINKS.vip30, tid, 'vip30');
  const linkPro30 = wompiLink(WOMPI_LINKS.pro30, tid, 'pro30');

  await bot.sendMessage(chatId,
    `🏆 *TIPSTERAI MASTER PRO*\n` +
    `_El bot de análisis deportivo con IA más avanzado_\n\n` +
    `━━━━━━━━━━━━━━━━━━━\n` +
    `🆓 *FREEMIUM — Gratis*\n` +
    `▸ 1 consulta al día · 3 días de prueba\n` +
    `▸ Picks básicos del día\n\n` +
    `⚡ *VIP 15 días — $59.900 COP*\n` +
    `▸ 10 consultas diarias\n` +
    `▸ Picks del día con estadísticas reales\n` +
    `▸ Análisis profundo de cualquier partido\n` +
    `▸ Picks en vivo con datos en tiempo real\n` +
    `▸ Filtros por liga y equipo\n` +
    `▸ Alertas de gol en vivo\n\n` +
    `⚡ *VIP 30 días — $99.900 COP*\n` +
    `▸ Todo lo del VIP 15 días\n` +
    `▸ Mejor precio por día ($3.330/día)\n\n` +
    `🏆 *PRO 30 días — $179.900 COP*\n` +
    `▸ 50 consultas diarias\n` +
    `▸ Todo lo del plan VIP\n` +
    `▸ Análisis de imágenes en vivo 📸\n` +
    `▸ Sube foto del partido y recibe análisis al instante\n` +
    `▸ Acceso a todas las ligas del mundo\n` +
    `━━━━━━━━━━━━━━━━━━━\n\n` +
    `🇨🇴 *Si estás en Colombia:*\n` +
    `Paga con Nequi, Daviplata, PSE o Bancolombia:\n` +
    `💳 [VIP 15 días — $59.900](${linkVip15})\n` +
    `💳 [VIP 30 días — $99.900](${linkVip30})\n` +
    `💳 [PRO 30 días — $179.900](${linkPro30})\n\n` +
    `🌎 *Para el resto del mundo:*\n` +
    `Paga con tarjeta internacional:\n` +
    `🔗 [Suscribirse en Whop (USD)](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)`,
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
        `Tu período de prueba gratuito ha terminado 🏁\n\n` +
        `Espero que hayas visto el nivel del análisis. Para continuar elige tu plan:\n\n` +
        `⚡ *VIP 15 días — $59.900 COP*\n` +
        `▸ 10 consultas/día · picks · análisis · en vivo · alertas\n` +
        `💳 [Pagar con Wompi](${linkVip15})\n\n` +
        `⚡ *VIP 30 días — $99.900 COP*\n` +
        `▸ 10 consultas/día · todo lo del VIP · mejor precio\n` +
        `💳 [Pagar con Wompi](${linkVip30})\n\n` +
        `🏆 *PRO 30 días — $179.900 COP*\n` +
        `▸ 50 consultas/día · análisis de imágenes 📸 · todas las ligas\n` +
        `💳 [Pagar con Wompi](${linkPro30})\n\n` +
        `🇨🇴 Paga con Nequi, Bancolombia, tarjeta o PSE.\n` +
        `🌎 También en: [Whop (USD)](https://whop.com/joined/tipsterai-master-pro/products/tipsterai-master-pro-88/)`,
        { parse_mode: 'Markdown' }
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
        `⚠️ *Tu suscripción ${plan.toUpperCase()} ha expirado.*\n\n` +
        `Tu acceso ha cambiado al plan gratuito.\n\n` +
        `Para renovar escribe *"ver planes"* o elige aquí:\n\n` +
        `⚡ *VIP 30 días* — $99.900 COP\n` +
        `💳 [Renovar con Wompi](${linkVip30})\n\n` +
        `🏆 *PRO 30 días* — $179.900 COP\n` +
        `💳 [Renovar con Wompi](${linkPro30})`,
        { parse_mode: 'Markdown' }
      );
      return { allowed: false };
    }
  }

  // Verificar límite diario
  if (consultasHoy >= planConfig.consultas_diarias) {
    let msg;
    if (plan === 'free') {
      await bot.sendMessage(chatId,
        `Has usado tu consulta gratuita de hoy 🎯\n\n` +
        `Vuelve mañana para tu próxima consulta gratis 📅\n\n` +
        `¿Quieres acceso ilimitado sin esperar? Tenemos planes desde *$59.900 COP*`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            inline_keyboard: [[
              { text: '🚀 Ver planes disponibles', callback_data: 'show_planes' }
            ]]
          }
        }
      );
      console.log('CHECK ACCESS - resultado: bloqueado por límite diario (free)');
      return { allowed: false };
    } else if (plan === 'vip' || plan === 'vip15') {
      const linkPro30 = wompiLink(WOMPI_LINKS.pro30, telegramId, 'pro30');
      msg =
        `Has alcanzado tus 10 consultas de hoy ⚡\n\n` +
        `Tus consultas se renuevan a medianoche.\n` +
        `¿Quieres más? Upgrade a PRO:\n\n` +
        `🏆 *PRO 30 días* — $179.900 COP\n` +
        `50 consultas/día + análisis de imágenes en vivo 📸\n\n` +
        `💳 [Upgrade a PRO con Wompi](${linkPro30})`;
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

      // Sistema del Día — solo admin (no aparece en menú público)
      if (ADMIN_IDS.has(telegramId) && /sistema\s*(del?\s*)?(dia|hoy|d[íi]a)/.test(q)) {
        return { intencion: 'sistema_hoy', pregunta_especifica: t };
      }

      // Alerta de gol — debe ir ANTES de en_vivo para capturar "gol en vivo"
      if (/alerta.{0,6}gol|gol.{0,10}(en\s*vivo|vivo|live|ahora|ahora mismo)|probabilidad.{0,6}gol|donde.{0,10}gol|partido.{0,10}gol|next.{0,4}goal/.test(q)) {
        return { intencion: 'alerta_gol', pregunta_especifica: t };
      }

      // Picks del día general
      if (/^(picks?|apuestas?)\s*(de\s*)?(hoy|del\s*dia|para\s*hoy)/.test(q) || q === 'picks' || q === 'picks hoy') {
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
async function handlePartidoConTeam(chatId, teamData, intent = {}) {
  const teamId   = teamData.team.id;
  const teamFull = teamData.team.name;

  await bot.sendMessage(chatId, `⚽ Buscando próximo partido de *${teamFull}*...`, { parse_mode: 'Markdown' });

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

  await bot.sendMessage(chatId,
    `⚽ ${isLive ? '🔴 EN VIVO: ' : 'Próximo: '}*${homeTeam} vs ${awayTeam}*\n🏆 ${nextRaw.league.name} | ⏰ ${formatHour(nextRaw.fixture.date)}\n\n📊 Recopilando estadísticas...`,
    { parse_mode: 'Markdown' }
  );

  const requests = [getH2H(homeId, awayId), getTeamStats(homeId, leagueId), getTeamStats(awayId, leagueId)];
  if (isLive) requests.push(getFixtureStatistics(nextRaw.fixture.id));
  const [h2hRes, homeStatsRes, awayStatsRes, liveStatsRes] = await Promise.allSettled(requests);

  const h2hData       = h2hRes.status === 'fulfilled'       ? h2hRes.value       : [];
  const homeStatsData = homeStatsRes.status === 'fulfilled'  ? homeStatsRes.value  : null;
  const awayStatsData = awayStatsRes.status === 'fulfilled'  ? awayStatsRes.value  : null;
  const liveStatsData = (isLive && liveStatsRes?.status === 'fulfilled') ? liveStatsRes.value : null;
  const probBlock     = buildProbBlock(homeStatsData, awayStatsData, h2hData, leagueId);
  const momentum      = isLive ? calcLiveMomentum(liveStatsData, homeTeam, awayTeam) : null;
  const elapsed       = nextRaw.fixture?.status?.elapsed || 0;
  const cornersProj   = isLive && elapsed > 0 ? calcLiveProjection((homeStats(liveStatsData)?.['Corner Kicks']??0)+(awayStats(liveStatsData)?.['Corner Kicks']??0), elapsed) : null;
  const cardsProj     = isLive && elapsed > 0 ? calcLiveProjection(((homeStats(liveStatsData)?.['Yellow Cards']??0)+(homeStats(liveStatsData)?.['Red Cards']??0))+((awayStats(liveStatsData)?.['Yellow Cards']??0)+(awayStats(liveStatsData)?.['Red Cards']??0)), elapsed) : null;

  const analysisData = {
    partido: { liga: nextRaw.league.name, pais: nextRaw.league.country, fecha: nextRaw.fixture.date.split('T')[0], hora: formatHour(nextRaw.fixture.date), local: homeTeam, visitante: awayTeam, enVivo: isLive, minuto: elapsed || null, marcador: isLive ? `${nextRaw.goals?.home??0}-${nextRaw.goals?.away??0}` : null },
    h2h: h2hData, bttsEnH2H: h2hData.filter(m => m.btts).length,
    statsLocal: homeStatsData, statsVisitante: awayStatsData, estadisticasVivo: liveStatsData,
    ...(probBlock   && { probabilidadesCalculadas: probBlock }),
    ...(momentum    && { momentumEnVivo: momentum }),
    ...(cornersProj && { proyeccionCorners: cornersProj }),
    ...(cardsProj   && { proyeccionTarjetas: cardsProj }),
  };

  await bot.sendMessage(chatId, '⚡ Procesando análisis...');
  const system   = isLive ? INPLAY_SYSTEM : TIPSTER_SYSTEM;
  const season   = LEAGUE_SEASONS[leagueId] || 2025;
  const analysis = await sonnet(system, `Analiza este partido (temporada ${season}):\n\n${JSON.stringify(analysisData, null, 2)}`);
  try {
    await sendLong(chatId, `🎯 *${homeTeam} vs ${awayTeam}*\n\n${analysis}`, { parse_mode: 'Markdown' });
  } catch {
    await sendLong(chatId, `🎯 ${homeTeam} vs ${awayTeam}\n\n${analysis.replace(/[*_`]/g, '')}`);
  }
  recordPicks(analysis, [{ fixtureId: nextRaw.fixture.id, local: homeTeam, visitante: awayTeam, liga: nextRaw.league.name, fechaPartido: nextRaw.fixture.date }]).catch(() => {});
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
    // Ver planes (botón desde límite diario free)
    if (data === 'show_planes') {
      const telegramId = query.from?.id || chatId;
      await bot.editMessageText(
        `✅ Aquí están los planes disponibles:`,
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

// Polling errors: 429 espera el retry_after antes de reiniciar
bot.on('polling_error', err => {
  const msg = err.message || '';
  // Telegram 429: "retry after N" — extraer N y esperar
  const retryMatch = msg.match(/retry after (\d+)/i);
  if (retryMatch) {
    const retrySecs = Math.max(parseInt(retryMatch[1], 10), 10);
    console.warn(`⚠️  Telegram 429 — esperando ${retrySecs}s antes de reiniciar`);
    exitBot(`polling_error: ${msg}`, retrySecs * 1000);
  } else {
    exitBot(`polling_error: ${msg}`);
  }
});

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
      `🎉 *¡Pago confirmado! Bienvenido a ${planNombre}*\n\n` +
      `Tu suscripción está activa hasta el *${expires}*.\n\n` +
      `Ahora tienes acceso a:\n` +
      `• 🎯 *${consultasDia} consultas diarias*\n` +
      `• 📡 Picks en vivo con estadísticas en tiempo real\n` +
      `• 🔍 Análisis de cualquier equipo o partido\n` +
      `• 🏆 Picks del día con análisis estadístico real\n` +
      (puedeImagen ? `• 📸 Análisis de imágenes en vivo\n` : '') +
      `\nEscríbeme lo que necesitas para empezar. ¡Buena suerte! ⚽`,
      { parse_mode: 'Markdown' }
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
