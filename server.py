"""
Crypto Intelligence Server  v3
情报源（全部免费，无需付费 API）：
  1. CoinGecko      — 行情 / 趋势 / 全球数据        (免费，无 Key)
  2. RSS 媒体        — 6 个主流加密媒体              (免费，无 Key)
  3. Google News    — 按项目/关键词搜索             (免费，无 Key)
  4. DefiLlama      — 链上 TVL 数据                (免费，无 Key)
  5. CoinMarketCal  — 事件日历（可选 Key）
  6. Claude API     — AI 中文总结（可选 Key，后续接入）

自动分析：
  - 币种提及检测：识别每条新闻涉及哪些监控币种
  - 关键词频率：统计所有新闻中的高频词
  - 项目热度排行：哪些币被媒体最多提及
"""

import os, re, time
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED
import xml.etree.ElementTree as ET
import urllib.parse

# ── 代理设置（Clash Verge 7897，仅本机开发环境）──────────────────
# 让 RSS/CoinGecko/Google News 等外部请求走代理
# NO_PROXY 排除本机地址，避免 Flask 自身请求被拦截
# 部署到 Railway 等云平台时不存在本地代理，且这些环境会注入
# RAILWAY_ENVIRONMENT / RAILWAY_PROJECT_ID，据此自动跳过代理设置
_IS_CLOUD = bool(os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('RAILWAY_PROJECT_ID'))
if not _IS_CLOUD:
    os.environ.setdefault('HTTP_PROXY',  'http://127.0.0.1:7897')
    os.environ.setdefault('HTTPS_PROXY', 'http://127.0.0.1:7897')
    os.environ.setdefault('NO_PROXY',    '127.0.0.1,localhost,::1,0.0.0.0')

import requests
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

try:
    import anthropic as _anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = Flask(__name__, static_folder='.')
CORS(app)

COINMARKETCAL_KEY = os.getenv('COINMARKETCAL_KEY', '')
COINGECKO_KEY     = os.getenv('COINGECKO_KEY', '')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')
FRED_API_KEY      = os.getenv('FRED_API_KEY', '')

# ── RSS 媒体订阅（由 .env RSS_FEEDS 控制，此处不设默认） ────────
_MEDIA_RSS = []
# 已知 RSS Feed URL → 显示名称（避免出现 rss.app 这类无意义域名）
_KNOWN_FEEDS: dict[str, str] = {
    'zqBxhjoUINHRbkIW': 'Lookonchain',   # https://rss.app/feeds/zqBxhjoUINHRbkIW.xml
}

def _feed_name(url: str) -> str:
    """从 URL 推断 RSS 源显示名称"""
    for key, name in _KNOWN_FEEDS.items():
        if key in url:
            return name
    try:
        domain = url.split('/')[2].replace('www.', '')
        return domain.split('.')[0].capitalize()
    except Exception:
        return 'RSS'

# .env RSS_FEEDS 追加到媒体源之后（不替换，始终并列）
_extra_urls = [u.strip() for u in os.getenv('RSS_FEEDS', '').split(',') if u.strip()]
EXTRA_RSS = [(_feed_name(url), url) for url in _extra_urls]

# ── Google News 主题监控（10 个，免费，无需 Key） ─────────────
GOOGLE_TOPICS = [
    {'q': 'Worldcoin WLD',                         'label': 'Worldcoin / WLD',    'coins': ['WLD']},
    {'q': 'OpenAI crypto',                         'label': 'OpenAI + Crypto',    'coins': ['WLD']},
    {'q': 'Monad crypto blockchain',               'label': 'Monad / MON',        'coins': ['MON']},
    {'q': 'Solana ETF',                            'label': 'Solana ETF',         'coins': ['SOL']},
    {'q': 'Pudgy Penguins PENGU',                  'label': 'Pudgy Penguins',     'coins': ['PENGU']},
    {'q': 'AI cryptocurrency blockchain',           'label': 'AI + Crypto',        'coins': []},
    {'q': 'stablecoin regulation',                 'label': '稳定币监管',          'coins': ['ENA', 'ONDO']},
    {'q': 'Bitcoin ETF BTC',                       'label': 'Bitcoin ETF',        'coins': ['BTC']},
    {'q': 'Ethereum ETF ETH',                      'label': 'Ethereum ETF',       'coins': ['ETH']},
    {'q': 'RWA real world asset crypto tokenization','label': 'RWA',              'coins': ['ONDO']},
]

# ── 币种关键词映射（用于提及检测） ───────────────────────────
COIN_KEYWORDS: dict[str, list[str]] = {
    'BTC':   ['bitcoin', 'btc', 'satoshi', 'halving', 'sats'],
    'ETH':   ['ethereum', 'eth', 'vitalik', 'eip ', 'proof-of-stake'],
    'SOL':   ['solana', 'sol ', 'solana network'],
    'WLD':   ['worldcoin', 'world coin', ' wld ', 'world id', 'sam altman',
              'openai', 'tools for humanity', ' orb '],
    'ORDI':  ['ordinals', 'ordi', 'brc-20', 'brc20', 'bitcoin nft'],
    'PYTH':  ['pyth ', 'pyth network', 'pyth oracle'],
    'PENGU': ['pudgy penguins', 'pengu', 'pudgy ', 'luca netz'],
    'ENA':   ['ethena', ' ena ', ' usde ', 'ethena labs'],
    'ONDO':  ['ondo ', 'ondo finance'],
    'LINK':  ['chainlink', ' link ', 'link oracle', 'ccip'],
    'W':     ['wormhole', 'wormhole bridge'],
    'JUP':   ['jupiter', 'jup ', 'jupiter dex', 'jupiter exchange'],
    'MON':   ['monad', 'monad blockchain', 'monad network'],
    'XPL':   ['plasma ', ' xpl ', 'plasma finance'],
}

# 关键词提取：过滤通用词
STOPWORDS = {
    'the','a','an','in','of','to','and','or','is','it','that','for','on',
    'at','by','with','from','this','are','was','has','have','be','as',
    'will','its','can','but','not','also','after','over','about','more',
    'than','into','while','been','their','what','when','how','who','all',
    'says','said','say','report','reports','launch','launches','unveils',
    'update','updates','amid','first','latest','new','just','would',
    'crypto','cryptocurrency','blockchain','market','token','coin','coins',
    'defi','nft','trading','price','prices','billion','million','trillion',
}

# ── 币种注册表 ──────────────────────────────────────────────────
COINS = {
    'BTC':   {'name': 'Bitcoin',        'binance': 'BTCUSDT',   'okx': None},
    'ETH':   {'name': 'Ethereum',       'binance': 'ETHUSDT',   'okx': None},
    'SOL':   {'name': 'Solana',         'binance': 'SOLUSDT',   'okx': None},
    'WLD':   {'name': 'Worldcoin',      'binance': 'WLDUSDT',   'okx': None},
    'ORDI':  {'name': 'Ordinals',       'binance': 'ORDIUSDT',  'okx': None},
    'PYTH':  {'name': 'Pyth Network',   'binance': 'PYTHUSDT',  'okx': None},
    'PENGU': {'name': 'Pudgy Penguins', 'binance': 'PENGUUSDT', 'okx': None},
    'ENA':   {'name': 'Ethena',         'binance': 'ENAUSDT',   'okx': None},
    'ONDO':  {'name': 'Ondo Finance',   'binance': 'ONDOUSDT',  'okx': None},
    'LINK':  {'name': 'Chainlink',      'binance': 'LINKUSDT',  'okx': None},
    'W':     {'name': 'Wormhole',       'binance': 'WUSDT',     'okx': None},
    'JUP':   {'name': 'Jupiter',        'binance': 'JUPUSDT',   'okx': None},
    'MON':   {'name': 'Monad',          'binance': None,        'okx': 'MON-USDT'},
    'XPL':   {'name': 'Plasma',         'binance': None,        'okx': 'XPL-USDT'},
}

LLAMA_SLUGS = {'ENA':'ethena','ONDO':'ondo-finance','JUP':'jupiter','LINK':'chainlink','W':'wormhole'}

# ── 缓存 ────────────────────────────────────────────────────────
_cache: dict = {}

def cache_get(key):
    e = _cache.get(key)
    if e:
        val, ts, ttl = e
        if time.time() - ts < ttl:
            return val
    return None

def cache_set(key, val, ttl=300):
    _cache[key] = (val, time.time(), ttl)

# ── HTTP Session ─────────────────────────────────────────────────
_S = requests.Session()
_S.headers.update({'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'})

def get_json(url, params=None, headers=None, timeout=12):
    r = _S.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ════════════════════════════════════════════════════════════════
#  技术分析（K 线）
# ════════════════════════════════════════════════════════════════
def _sma(a, p):
    return [sum(a[i-p+1:i+1])/p if i>=p-1 else None for i in range(len(a))]

def _ema(a, p):
    k=2/(p+1); r=[a[0]]
    for v in a[1:]: r.append(v*k+r[-1]*(1-k))
    return r

def _rsi(closes, p=14):
    n=len(closes); res=[None]*n
    if n<p+1: return res
    ag=al=0.0
    for i in range(1, p+1):
        d=closes[i]-closes[i-1]
        if d>0: ag+=d
        else:   al-=d
    ag/=p; al/=p
    for i in range(p, n):
        if i>p:
            d=closes[i]-closes[i-1]
            ag=(ag*(p-1)+(d if d>0 else 0))/p
            al=(al*(p-1)+(-d if d<0 else 0))/p
        res[i]=100 if al==0 else 100-100/(1+ag/al)
    return res

def _macd(closes):
    e12=_ema(closes,12); e26=_ema(closes,26)
    dif=[e12[i]-e26[i] for i in range(len(closes))]
    return dif, _ema(dif,9)

def analyse(klines):
    closes=[float(k[4]) for k in klines]; vols=[float(k[5]) for k in klines]
    opens=[float(k[1]) for k in klines]; n=len(closes); L=n-1
    ma5=_sma(closes,5)[L]; ma20=_sma(closes,20)[L]
    ma60=_sma(closes,60)[L]; ma120=_sma(closes,120)[L] if n>=120 else None

    if ma120 and ma5>ma20>ma60>ma120:         ms='强势多头'
    elif ma5 and ma20 and ma60 and ma5>ma20>ma60: ms='多头排列'
    elif ma120 and ma5<ma20<ma60<ma120:        ms='强势空头'
    elif ma5 and ma20 and ma60 and ma5<ma20<ma60: ms='空头排列'
    else: ms='交叉整理'

    rsi=_rsi(closes)[L] or 50.0
    rd='超买' if rsi>70 else '超卖' if rsi<30 else '偏低' if rsi<40 else '偏高' if rsi>60 else '中性'
    dif,dea=_macd(closes)
    golden=len(dif)>1 and dif[-2]<dea[-2] and dif[-1]>=dea[-1]
    death =len(dif)>1 and dif[-2]>dea[-2] and dif[-1]<=dea[-1]
    md='金叉' if golden else '死叉' if death else ('多头' if dif[-1]>0 else '空头')
    vm=_sma(vols,20)[L] or 1.0; vr=vols[L]/vm
    vd=f'放量{"涨" if closes[L]>opens[L] else "跌"} {vr:.1f}x' if vr>2.5 else f'量能正常 {vr:.1f}x'

    score=0
    score += 2 if '强势多头' in ms else (1 if '多头' in ms else 0)
    score -= 2 if '强势空头' in ms else (1 if '空头' in ms else 0)
    score += 1 if golden else (-1 if death else 0)
    score += 1 if rsi<40 else (-1 if rsi>70 else 0)
    score += 1 if '放量涨' in vd else (-1 if '放量跌' in vd else 0)
    chg24=(closes[L]-closes[L-1])/closes[L-1]*100 if closes[L-1] else 0.0
    return {'price':closes[L],'chg24':round(chg24,4),'score':score,
            'ma':{'ma5':ma5,'ma20':ma20,'ma60':ma60,'ma120':ma120,'signal':ms},
            'rsi':{'value':round(rsi,2),'desc':f'{rd} {rsi:.1f}'},
            'macd':{'dif':round(dif[-1],6),'dea':round(dea[-1],6),'desc':md},
            'vol':{'ratio':round(vr,2),'desc':vd}}

def _klines_binance(sym, limit=200):
    d=get_json('https://api.binance.com/api/v3/klines',
               params={'symbol':sym,'interval':'1d','limit':limit})
    if not isinstance(d,list) or len(d)<30: raise ValueError('数据不足')
    return d

def _klines_okx(inst, limit=200):
    d=get_json('https://www.okx.com/api/v5/market/candles',
               params={'instId':inst,'bar':'1D','limit':limit})
    if d.get('code')!='0' or not d.get('data'): raise ValueError(d.get('msg','OKX err'))
    data=list(reversed(d['data']))
    if len(data)<30: raise ValueError('数据不足')
    return data

def fetch_coin_tech(ticker):
    c=COINS.get(ticker,{})
    if c.get('binance'): return analyse(_klines_binance(c['binance']))
    if c.get('okx'):     return analyse(_klines_okx(c['okx']))
    raise ValueError(f'{ticker} 无数据源')

# ════════════════════════════════════════════════════════════════
#  新闻分析工具
# ════════════════════════════════════════════════════════════════
def detect_coins(title: str) -> list[str]:
    """在标题中检测涉及哪些监控币种"""
    t = title.lower()
    return [ticker for ticker, kws in COIN_KEYWORDS.items()
            if any(kw in t for kw in kws)]

def extract_keywords(items: list, top_n=20) -> list[tuple]:
    """从所有标题提取高频关键词"""
    freq: dict[str, int] = {}
    for item in items:
        words = re.sub(r'[^\w\s]', ' ', item.get('title','').lower()).split()
        for w in words:
            if len(w) > 3 and w not in STOPWORDS and not w.isdigit():
                freq[w] = freq.get(w, 0) + 1
    return sorted(freq.items(), key=lambda x: x[1], reverse=True)[:top_n]

def count_coin_mentions(items: list) -> list[tuple]:
    """统计各币种被提及次数，返回排序列表"""
    counts: dict[str, int] = {}
    for item in items:
        for ticker in item.get('coins', []):
            counts[ticker] = counts.get(ticker, 0) + 1
    return sorted(counts.items(), key=lambda x: x[1], reverse=True)

# ════════════════════════════════════════════════════════════════
#  情报收集器
# ════════════════════════════════════════════════════════════════

# ── 1. CoinGecko 市场数据 ────────────────────────────────────
def collect_coingecko() -> dict:
    """三个子请求并行发出，总超时 10 秒，任意失败不影响其他"""
    cached = cache_get('cg')
    if cached: return cached

    hdr  = {'x-cg-demo-api-key': COINGECKO_KEY} if COINGECKO_KEY else {}
    BASE = 'https://api.coingecko.com/api/v3'

    def _global():
        try:
            g = get_json(BASE+'/global', headers=hdr, timeout=(4, 8))['data']
            return {
                'total_mcap':    g['total_market_cap']['usd'],
                'mcap_change':   g['market_cap_change_percentage_24h_usd'],
                'btc_dom':       g['market_cap_percentage']['btc'],
                'eth_dom':       g['market_cap_percentage'].get('eth', 0),
                'total_vol_24h': g['total_volume']['usd'],
                'active_coins':  g['active_cryptocurrencies'],
            }
        except Exception as e:
            return {'error': str(e)}

    def _fng():
        try:
            fg = get_json('https://api.alternative.me/fng/?limit=1', timeout=(4, 6))
            v  = int(fg['data'][0]['value'])
            lbl = ('极度恐惧' if v<25 else '恐惧' if v<45 else
                   '中性' if v<55 else '贪婪' if v<75 else '极度贪婪')
            return {'value': v, 'label': lbl}
        except:
            return {'value': None, 'label': '—'}

    def _trending():
        try:
            tr = get_json(BASE+'/search/trending', headers=hdr, timeout=(4, 8))['coins']
            return [
                {'name':   c['item']['name'],
                 'symbol': c['item']['symbol'].upper(),
                 'rank':   c['item']['market_cap_rank'],
                 'change': c['item'].get('data', {})
                              .get('price_change_percentage_24h', {}).get('usd')}
                for c in tr[:7]
            ]
        except:
            return []

    # 三个请求同时发出，最多等 10 秒
    with ThreadPoolExecutor(max_workers=3) as pool:
        gf = pool.submit(_global)
        ff = pool.submit(_fng)
        tf = pool.submit(_trending)
        done, _ = wait([gf, ff, tf], timeout=10, return_when=ALL_COMPLETED)

    result = {
        'global':     gf.result() if gf in done else {},
        'fear_greed': ff.result() if ff in done else {'value': None, 'label': '—'},
        'trending':   tf.result() if tf in done else [],
    }
    cache_set('cg', result, 300)
    return result

# ── 2. 币种技术指标 ──────────────────────────────────────────
def collect_prices(tickers: list) -> dict:
    """使用 wait(timeout=15) 代替 as_completed，确保永不卡死"""
    key = 'prices_' + ','.join(sorted(tickers))
    cached = cache_get(key)
    if cached: return cached

    def _one(t):
        try: return t, {'ok': True,  **fetch_coin_tech(t)}
        except Exception as e: return t, {'ok': False, 'error': str(e)}

    result = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(_one, t): t for t in tickers}
        done, not_done = wait(list(futs.keys()), timeout=15, return_when=ALL_COMPLETED)

        for fut in not_done:
            result[futs[fut]] = {'ok': False, 'error': '超时'}
            fut.cancel()

        for fut in done:
            try:
                t, data = fut.result()
                result[t] = data
            except Exception as e:
                result[futs[fut]] = {'ok': False, 'error': str(e)}

    cache_set(key, result, 300)
    return result

# ── 3. RSS 解析（支持 RSS 2.0 + Atom） ──────────────────────
NS_ATOM = 'http://www.w3.org/2005/Atom'

def _parse_feed(url: str, source_name: str) -> list[dict]:
    r = _S.get(url, timeout=(5, 8))   # (connect_timeout, read_timeout)
    r.raise_for_status()
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError:
        return []
    items = []

    # Atom 格式
    entries = root.findall(f'{{{NS_ATOM}}}entry') or root.findall('entry')
    if entries:
        for e in entries[:10]:
            def _a(tag): return (e.findtext(f'{{{NS_ATOM}}}{tag}') or e.findtext(tag) or '').strip()
            title = _a('title')
            pub   = (_a('published') or _a('updated'))[:10]
            el    = (e.find(f'{{{NS_ATOM}}}link[@rel="alternate"]') or
                     e.find(f'{{{NS_ATOM}}}link') or e.find('link'))
            link  = (el.get('href','') if el is not None else '').strip()
            if title and link:
                items.append({'title':title,'url':link,'source':source_name,'published':pub})
        return items

    # RSS 2.0 格式
    for item in root.findall('./channel/item')[:10]:
        title = (item.findtext('title') or '').strip()
        link  = (item.findtext('link') or '').strip()
        pub   = (item.findtext('pubDate') or '')[:16].strip()
        if title and link:
            items.append({'title':title,'url':link,'source':source_name,'published':pub})
    return items

def _google_news_feed(query: str, source_name: str) -> list[dict]:
    """Google News RSS 搜索，返回结果"""
    url = (f'https://news.google.com/rss/search?q='
           f'{urllib.parse.quote_plus(query)}&hl=en-US&gl=US&ceid=US:en')
    raw = _parse_feed(url, source_name)
    result = []
    for item in raw:
        # Google News 标题格式：「文章标题 - 来源名」，去除末尾来源
        title = re.sub(r'\s+-\s+[A-Z][^-]{2,40}$', '', item['title']).strip()
        if title:
            result.append({**item, 'title': title})
    return result

# ── 4. 综合 RSS 收集 + 自动分析 ─────────────────────────────
def collect_rss_intel() -> dict:
    cached = cache_get('rss_intel')
    if cached: return cached

    feed_tasks  = _MEDIA_RSS + EXTRA_RSS   # (name, url) pairs
    topic_tasks = GOOGLE_TOPICS            # {q, label, coins}

    all_media_items: list[dict] = []
    topic_results:   list[dict] = []
    feeds_detail:    list[dict] = []
    topics_detail:   list[dict] = []

    def _fetch_media(name_url):
        name, url = name_url
        t0 = time.time()
        try:
            items = _parse_feed(url, name)
            return 'ok', name, items, round(time.time()-t0, 1)
        except Exception as e:
            return 'fail', name, str(e)[:80], round(time.time()-t0, 1)

    def _fetch_topic(topic):
        t0 = time.time()
        try:
            items = _google_news_feed(topic['q'], topic['label'])
            return 'ok', topic, items, round(time.time()-t0, 1)
        except Exception as e:
            return 'fail', topic, str(e)[:80], round(time.time()-t0, 1)

    # 所有任务同时提交，单一 22 秒总超时（媒体+主题全并行）
    # 媒体每条有 (5,8)s 独立超时，Google News 每条有 (5,8)s 独立超时
    # 22s 内未完成的任务全部取消，不阻塞主线程
    COMBINED_TIMEOUT = 22

    with ThreadPoolExecutor(max_workers=14) as pool:
        m_futs = {pool.submit(_fetch_media,  t): ('m', t) for t in feed_tasks}
        t_futs = {pool.submit(_fetch_topic,  t): ('t', t) for t in topic_tasks}
        all_map = {**m_futs, **t_futs}

        done, not_done = wait(list(all_map.keys()), timeout=COMBINED_TIMEOUT,
                              return_when=ALL_COMPLETED)

        # 取消超时任务，记录到 detail
        for fut in not_done:
            kind, info = all_map[fut]
            if kind == 'm':
                name = info[0] if isinstance(info, tuple) else str(info)
                feeds_detail.append({'name': name, 'status': 'timeout'})
            else:
                topics_detail.append({'label': info.get('label','?'), 'status': 'timeout'})
            fut.cancel()

        # 处理已完成任务
        for fut in done:
            kind, info = all_map[fut]
            if kind == 'm':
                try:
                    status, name, result, elapsed = fut.result()
                    if status == 'ok':
                        all_media_items.extend(result)
                        feeds_detail.append({'name': name, 'status': 'ok',
                                             'count': len(result), 't': elapsed})
                    else:
                        feeds_detail.append({'name': name, 'status': 'fail',
                                             'error': result, 't': elapsed})
                except Exception as e:
                    feeds_detail.append({'name': '?', 'status': 'error', 'error': str(e)[:60]})
            else:
                try:
                    status, topic, result, elapsed = fut.result()
                    if status == 'ok' and result:
                        topic_results.append({'label': topic['label'],
                                              'coins': topic['coins'],
                                              'items': result[:4]})
                        topics_detail.append({'label': topic['label'], 'status': 'ok',
                                              'count': len(result), 't': elapsed})
                    else:
                        topics_detail.append({'label': topic.get('label','?'), 'status': 'fail'})
                except Exception as e:
                    topics_detail.append({'label': '?', 'status': 'error', 'error': str(e)[:60]})

    feeds_ok   = sum(1 for f in feeds_detail  if f['status'] == 'ok')
    feeds_fail = len(feeds_detail) - feeds_ok

    # 去重（按 URL）
    seen_urls: set = set()
    unique_items: list[dict] = []
    for item in all_media_items:
        if item['url'] not in seen_urls:
            seen_urls.add(item['url'])
            unique_items.append(item)

    for item in unique_items:
        item['coins'] = detect_coins(item['title'])

    all_items_for_analysis = unique_items[:]
    for t in topic_results:
        for item in t['items']:
            item.setdefault('coins', t['coins'] + detect_coins(item['title']))
            if item['url'] not in seen_urls:
                seen_urls.add(item['url'])
                all_items_for_analysis.append(item)

    coin_mentions = count_coin_mentions(all_items_for_analysis)
    top_keywords  = extract_keywords(all_items_for_analysis, top_n=20)

    unique_items.sort(key=lambda x: x.get('published',''), reverse=True)
    topic_results.sort(key=lambda x: x['label'])

    result = {
        'items':          unique_items[:25],
        'topics':         topic_results,
        'coin_mentions':  coin_mentions,
        'top_keywords':   top_keywords,
        'feeds_ok':       feeds_ok,
        'feeds_fail':     feeds_fail,
        'feeds_detail':   feeds_detail,
        'topics_detail':  topics_detail,
        'total_items':    len(all_items_for_analysis),
    }
    cache_set('rss_intel', result, 600)
    return result

# ── DefiLlama watchlist / filter config ─────────────────────
_LLAMA_EXCL        = {'CEX', 'Canonical Bridge'}
_WATCH_CHAINS      = ['World Chain', 'Solana', 'Monad', 'Aptos', 'Polygon', 'Hyperliquid', 'Plasma']
_WATCH_PROTOS      = ['Ethena', 'Ondo Finance']
_RWA_KW            = ['BUIDL', 'Ondo', 'Hashnote', 'PAX Gold', 'PAXG', 'Maple',
                      'Syrup', 'Mountain Protocol', 'OpenEden', 'Superstate']
_BRIDGE_KW         = ['Wormhole', 'Hyperlane', 'LayerZero', 'Lombard', 'Stargate',
                      'Across', 'deBridge', 'Portal', 'Celer', 'Hop Protocol',
                      'Synapse', 'Multichain', 'Allbridge']
# Chains that have DEX data on DefiLlama (exact URL-path names)
_DEX_CHAINS        = ['Hyperliquid', 'Solana', 'Polygon', 'Aptos', 'Base',
                      'Ethereum', 'BSC', 'Arbitrum', 'World Chain']

# Chains to fetch historical TVL for (covers top chains + watchlist)
# /v2/chains returns NO change data — must compute from historical endpoint
_HIST_CHAINS = [
    'Ethereum', 'Tron', 'BSC', 'Solana', 'Arbitrum', 'Bitcoin',
    'Base', 'Polygon', 'Aptos', 'Hyperliquid L1', 'Optimism',
    'Sui', 'Avalanche', 'World Chain', 'Plasma', 'Monad',
    'TON', 'Near', 'Mantle', 'Linea', 'Starknet',
]

# Known DefiLlama chain name aliases  (our name → possible real names, priority order)
_CHAIN_ALIASES: dict[str, list[str]] = {
    'world chain':  ['World Chain', 'Worldchain', 'WorldChain'],
    # ↓ DefiLlama uses "Hyperliquid L1" — confirmed via /v2/chains
    'hyperliquid':  ['Hyperliquid L1', 'Hyperliquid', 'HyperLiquid'],
    'plasma':       ['Plasma', 'Plasma Mainnet', 'PlasmaChain', 'Plasma BTC'],
    'monad':        ['Monad', 'Monad Mainnet', 'Monad Testnet'],
    'polygon':      ['Polygon', 'Polygon POS', 'Polygon PoS', 'Matic'],
    'aptos':        ['Aptos'],
    'solana':       ['Solana'],
}

def _chain_lookup(name: str, lk: dict):
    """Find a chain entry by name, trying aliases and partial matches."""
    # 1. Direct lowercase
    c = lk.get(name.lower())
    if c is not None: return c
    # 2. Alias list
    for alias in _CHAIN_ALIASES.get(name.lower(), []):
        c = lk.get(alias.lower())
        if c is not None: return c
    # 3. Partial containment (our name inside key, or key inside our name)
    nl = name.lower()
    for k, v in lk.items():
        if nl in k or k in nl:
            return v
    return None

def _stable_lookup(name: str, lk: dict):
    """Find stablecoin MCap by chain name using aliases."""
    v = lk.get(name.lower())
    if v: return v
    for alias in _CHAIN_ALIASES.get(name.lower(), []):
        v = lk.get(alias.lower())
        if v: return v
    return None

def _dex_lookup(name: str, dex_map: dict):
    """Find DEX volume by chain name using aliases."""
    # Try exact match first
    v = dex_map.get(name)
    if v is not None: return v
    # Try aliases
    for alias in _CHAIN_ALIASES.get(name.lower(), []):
        v = dex_map.get(alias)
        if v is not None: return v
    return None

# ── 5. DefiLlama TVL (Enhanced — 6 Layers) ──────────────────
def collect_defi_llama() -> dict:
    cached = cache_get('llama')
    if cached: return cached

    def _dex_url(n):
        return (f'https://api.llama.fi/overview/dexs/{urllib.parse.quote(n)}'
                '?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume')

    def _hist_url(n):
        return f'https://api.llama.fi/v2/historicalChainTvl/{urllib.parse.quote(n)}'

    # All parallel fetches in one pool (protocols + chains + stables + DEX + historical)
    with ThreadPoolExecutor(max_workers=40) as pool:
        fp   = pool.submit(lambda: get_json('https://api.llama.fi/protocols',               timeout=15))
        fc   = pool.submit(lambda: get_json('https://api.llama.fi/v2/chains',                timeout=12))
        fs   = pool.submit(lambda: get_json('https://stablecoins.llama.fi/stablecoins',     timeout=12))
        fsc  = pool.submit(lambda: get_json('https://stablecoins.llama.fi/stablecoinchains',timeout=10))
        fdex  = {n: pool.submit(lambda n=n: get_json(_dex_url(n),  timeout=8))  for n in _DEX_CHAINS}
        fhist = {n: pool.submit(lambda n=n: get_json(_hist_url(n), timeout=10)) for n in _HIST_CHAINS}

    protos = chains_raw = []
    stables_rw: dict = {}
    stablecoin_chains = []
    try: protos            = fp.result()
    except: pass
    try: chains_raw        = fc.result()
    except: pass
    try: stables_rw        = fs.result()
    except: pass
    try:
        r = fsc.result()
        stablecoin_chains = r if isinstance(r, list) else []
    except: pass

    # Historical chain TVL → compute change percentages
    def _cpct_from_hist(hist, days):
        if not hist or len(hist) < 2: return None
        try:
            now_ts  = int(hist[-1].get('date', 0))
            now_tvl = float(hist[-1].get('tvl', 0) or 0)
            target  = now_ts - days * 86400
            past    = next((e for e in reversed(hist[:-1])
                            if int(e.get('date', 0) or 0) <= target), hist[0])
            prev    = float(past.get('tvl', 0) or 0)
            return round((now_tvl - prev) / prev * 100, 2) if prev else None
        except: return None

    chain_hist: dict = {}   # lowercase(chain_name) → {change_1d, change_7d, change_1m}
    for n, f in fhist.items():
        try:
            hist = f.result()
            if isinstance(hist, list) and hist:
                chain_hist[n.lower()] = {
                    'change_1d': _cpct_from_hist(hist, 1),
                    'change_7d': _cpct_from_hist(hist, 7),
                    'change_1m': _cpct_from_hist(hist, 30),
                }
        except: pass

    # DEX volume per chain — store {vol24h, change_7d} per chain name
    dex_raw_by_chain: dict = {}   # chain → {vol24h, change_7d}
    for n, f in fdex.items():
        try:
            data = f.result()
            if isinstance(data, dict):
                dex_raw_by_chain[n] = {
                    'vol24h':   data.get('total24h'),
                    'change_7d':data.get('change_7d'),   # DEX vol 7d % change
                }
        except: pass
    # Flat vol-only dict for legacy _dex_lookup usage
    dex_by_chain = {k: v.get('vol24h') for k, v in dex_raw_by_chain.items()}

    # Stablecoin MCap per chain — lowercase key dict for alias-safe lookup
    stable_by_chain: dict = {}  # lowercase(chain_name) → USD amount
    for item in stablecoin_chains:
        if not isinstance(item, dict): continue
        nm  = item.get('name', '') or ''
        usd = (item.get('totalCirculatingUSD') or {}).get('peggedUSD', 0) or 0
        if nm: stable_by_chain[nm.lower()] = usd

    # Filtered, TVL-sorted protocol list (shared across layers)
    onchain = sorted(
        [p for p in protos
         if isinstance(p.get('tvl'), (int, float)) and (p.get('tvl') or 0) > 1_000_000
         and p.get('category', '') not in _LLAMA_EXCL],
        key=lambda x: x.get('tvl', 0), reverse=True
    ) if protos else []

    # ── Layer 1: Macro ─────────────────────────────────────
    total_tvl  = sum(p.get('tvl', 0) or 0 for p in protos
                     if isinstance(p.get('tvl'), (int, float))) if protos else 0
    chains_srt = sorted(chains_raw, key=lambda x: x.get('tvl', 0) or 0, reverse=True)

    # /v2/chains has NO change fields — all changes come from chain_hist (historical calls)
    def chain_item(c):
        nm  = c.get('name', '')
        tvl = c.get('tvl', 0)
        h   = chain_hist.get(nm.lower(), {})
        return {
            'name': nm, 'tvl': tvl,
            'change_1d': h.get('change_1d'),
            'change_7d': h.get('change_7d'),
            'change_1m': h.get('change_1m'),
        }
    def proto_item(p): return {
        'name': p.get('name', ''), 'category': p.get('category', ''),
        'tvl':  p.get('tvl', 0),  'change_1d': p.get('change_1d'),
        'change_7d': p.get('change_7d'), 'change_1m': p.get('change_1m')}

    # ── Layer 2: TVL Moves (gainers / losers) ──────────────
    moveable = [p for p in onchain
                if p.get('change_1d') is not None and (p.get('tvl', 0) or 0) > 10_000_000]

    # ── Layer 3: Watchlist ─────────────────────────────────
    chain_lk = {c.get('name', '').lower(): c for c in chains_raw}
    proto_lk  = {p.get('name', '').lower(): p for p in protos} if protos else {}

    watchlist = []
    for name in _WATCH_CHAINS:
        c = _chain_lookup(name, chain_lk)
        # Record the actual DefiLlama chain name found (for debugging)
        actual_name = c.get('name', name) if c else None
        dex_info = dex_raw_by_chain.get(actual_name) or dex_raw_by_chain.get(name) or {}
        c_tvl = c.get('tvl', 0) if c else None
        # Look up historical changes by actual DefiLlama name, fallback to display name
        h = chain_hist.get((actual_name or '').lower()) or chain_hist.get(name.lower()) or {}
        watchlist.append({
            'name':         name,
            'actual_name':  actual_name,
            'type':         'chain',
            'tvl':          c_tvl,
            'change_1d':    h.get('change_1d'),
            'change_7d':    h.get('change_7d'),
            'change_1m':    h.get('change_1m'),
            'stable_mcap':  _stable_lookup(actual_name or name, stable_by_chain),
            'dex_vol':      dex_info.get('vol24h'),
            'dex_change_7d':dex_info.get('change_7d'),
        })
    for name in _WATCH_PROTOS:
        key = name.lower()
        # Bidirectional partial match — require ≥4 chars to avoid short false positives
        p = proto_lk.get(key) or next(
            (v for k, v in proto_lk.items()
             if (key in k and len(k) >= 4) or (k in key and len(k) >= 4)), None)
        dex_info2 = dex_raw_by_chain.get(name) or {}
        watchlist.append({
            'name':         name,
            'actual_name':  p.get('name', name) if p else None,
            'type':         'protocol',
            'tvl':          p.get('tvl', 0)      if p else None,
            'change_1d':    p.get('change_1d')   if p else None,
            'change_7d':    p.get('change_7d')   if p else None,
            'change_1m':    p.get('change_1m')   if p else None,
            'stable_mcap':  _stable_lookup(name, stable_by_chain),
            'dex_vol':      dex_info2.get('vol24h'),
            'dex_change_7d':dex_info2.get('change_7d'),
        })

    # ── Layer 4: Stablecoins ───────────────────────────────
    pegged = stables_rw.get('peggedAssets', []) if stables_rw else []
    def susd(d): return (d or {}).get('peggedUSD', 0) or 0
    def pct(now, prev): return round((now - prev) / prev * 100, 2) if prev else None

    st = sum(susd(s.get('circulating'))        for s in pegged)
    pd = sum(susd(s.get('circulatingPrevDay'))  for s in pegged)
    pw = sum(susd(s.get('circulatingPrevWeek')) for s in pegged)
    pm = sum(susd(s.get('circulatingPrevMonth'))for s in pegged)

    # Preferred stablecoin display order — case-insensitive symbol match
    _STABLE_PREFERRED = ['USDT', 'USDC', 'USDS', 'USD1', 'DAI', 'USDe', 'PYUSD', 'BUIDL']
    _STABLE_PREF_UP   = {p.upper(): p for p in _STABLE_PREFERRED}  # USDE → USDe etc.
    peg_by_sym: dict = {}
    for s in pegged:
        sym_up = (s.get('symbol') or '').upper()
        if sym_up not in _STABLE_PREF_UP:
            continue
        mcap = susd(s.get('circulating', {}))
        canonical = _STABLE_PREF_UP[sym_up]
        if canonical not in peg_by_sym or mcap > susd(peg_by_sym[canonical].get('circulating', {})):
            peg_by_sym[canonical] = s

    stables_list = []
    for sym in _STABLE_PREFERRED:
        s = peg_by_sym.get(sym)
        mcap = susd(s.get('circulating', {})) if s else 0
        stables_list.append({
            'name':      s.get('name', sym) if s else sym,
            'symbol':    sym,
            'mcap':      mcap,
            'change_1d': pct(mcap, susd(s.get('circulatingPrevDay',  {}))) if s else None,
            'change_7d': pct(mcap, susd(s.get('circulatingPrevWeek', {}))) if s else None,
        })

    # ── Layer 5: RWA ───────────────────────────────────────
    rwa_cat = [p for p in protos if p.get('category', '') in ('RWA', 'RWA Lending')] if protos else []
    rwa_nm  = [p for p in protos
               if any(kw.lower() in (p.get('name', '') or '').lower() for kw in _RWA_KW)
               and p not in rwa_cat] if protos else []
    rwa_all = sorted(rwa_cat + rwa_nm, key=lambda x: x.get('tvl', 0) or 0, reverse=True)

    # ── Layer 6: Bridge Flow ────────────────────────────────
    bridge_cat = [p for p in protos
                  if p.get('category', '') in ('Bridge', 'Cross Chain')] if protos else []
    bridge_nm  = [p for p in protos
                  if any(kw.lower() in (p.get('name', '') or '').lower() for kw in _BRIDGE_KW)
                  and p not in bridge_cat] if protos else []
    bridge_all = sorted(bridge_cat + bridge_nm, key=lambda x: x.get('tvl', 0) or 0, reverse=True)

    result = {
        'source':            'DefiLlama',
        'total_tvl':         total_tvl,
        'top_chains':        [chain_item(c) for c in chains_srt[:15]],
        'top_protocols':     [proto_item(p) for p in onchain[:8]],
        'tvl_gainers':       [proto_item(p) for p in
                              sorted(moveable, key=lambda x: x.get('change_1d', 0), reverse=True)[:8]],
        'tvl_losers':        [proto_item(p) for p in
                              sorted(moveable, key=lambda x: x.get('change_1d', 0))[:8]],
        'watchlist':         watchlist,
        'stables_total':     st,
        'stables_change_1d': pct(st, pd),
        'stables_change_7d': pct(st, pw),
        'stables_change_1m': pct(st, pm),
        'stables':           stables_list[:15],
        'rwa':               [{'name': p.get('name', ''), 'tvl': p.get('tvl', 0),
                               'change_1d': p.get('change_1d'),
                               'change_7d': p.get('change_7d'),
                               'change_1m': p.get('change_1m'),
                               'category':  p.get('category', '')}
                              for p in rwa_all[:12]],
        'bridges':           [{'name': p.get('name', ''), 'tvl': p.get('tvl', 0),
                               'change_1d': p.get('change_1d'),
                               'change_7d': p.get('change_7d'),
                               'category':  p.get('category', '')}
                              for p in bridge_all[:12]],
        'dex_by_chain':       dex_by_chain,
        'dex_change_by_chain':{k: v.get('change_7d') for k, v in dex_raw_by_chain.items()
                               if v.get('change_7d') is not None},
        'stable_by_chain':    {k: v for k, v in stable_by_chain.items()
                               if v and v > 10_000_000},
        'top11':              [proto_item(p) for p in onchain[:11]],  # backward compat
    }
    cache_set('llama', result, 600)
    return result

# ── 6. Token Unlock Schedule（手动维护，按月更新）──────────────
# 数据为近似值，以各项目官方公告 / 链上合约为准
# pct = 占当前流通供应比例 | cat: VC / Team / Foundation / Ecosystem / Community
_UNLOCK_SCHEDULE = [
    # ── WLD ─────────────────────────────────────────────────
    {'token':'WLD', 'name':'Worldcoin',        'date':'2026-06-24','pct':2.6,'cat':'Foundation','note':'月度生态系统 + 用户奖励'},
    {'token':'WLD', 'name':'Worldcoin',        'date':'2026-07-24','pct':2.6,'cat':'Foundation','note':'月度生态系统 + 用户奖励'},
    {'token':'WLD', 'name':'Worldcoin',        'date':'2026-08-24','pct':2.6,'cat':'Foundation','note':'月度生态系统 + 用户奖励'},
    # ── APT ─────────────────────────────────────────────────
    {'token':'APT', 'name':'Aptos',            'date':'2026-06-12','pct':1.8,'cat':'Community', 'note':'基金会 + 社区月度释放'},
    {'token':'APT', 'name':'Aptos',            'date':'2026-07-12','pct':1.8,'cat':'Community', 'note':'基金会 + 社区月度释放'},
    {'token':'APT', 'name':'Aptos',            'date':'2026-08-12','pct':1.8,'cat':'Community', 'note':'基金会 + 社区月度释放'},
    # ── ARB ─────────────────────────────────────────────────
    {'token':'ARB', 'name':'Arbitrum',         'date':'2026-06-16','pct':1.1,'cat':'Team',      'note':'团队 + 投资人线性解锁'},
    {'token':'ARB', 'name':'Arbitrum',         'date':'2026-07-16','pct':1.1,'cat':'Team',      'note':'团队 + 投资人线性解锁'},
    {'token':'ARB', 'name':'Arbitrum',         'date':'2026-08-16','pct':1.1,'cat':'Team',      'note':'团队 + 投资人线性解锁'},
    # ── OP ──────────────────────────────────────────────────
    {'token':'OP',  'name':'Optimism',         'date':'2026-06-10','pct':1.6,'cat':'Ecosystem', 'note':'OP Foundation + 追溯激励'},
    {'token':'OP',  'name':'Optimism',         'date':'2026-07-10','pct':1.6,'cat':'Ecosystem', 'note':'OP Foundation + 追溯激励'},
    {'token':'OP',  'name':'Optimism',         'date':'2026-08-10','pct':1.6,'cat':'Ecosystem', 'note':'OP Foundation + 追溯激励'},
    # ── SUI ─────────────────────────────────────────────────
    {'token':'SUI', 'name':'Sui',              'date':'2026-06-01','pct':2.2,'cat':'VC',        'note':'早期投资人 + 团队归属'},
    {'token':'SUI', 'name':'Sui',              'date':'2026-07-01','pct':2.2,'cat':'VC',        'note':'早期投资人 + 团队归属'},
    {'token':'SUI', 'name':'Sui',              'date':'2026-08-01','pct':2.2,'cat':'VC',        'note':'早期投资人 + 团队归属'},
    # ── TIA ─────────────────────────────────────────────────
    {'token':'TIA', 'name':'Celestia',         'date':'2026-10-30','pct':5.2,'cat':'VC',        'note':'种子轮 + 核心贡献者持续线性归属'},
    # ── ZK ──────────────────────────────────────────────────
    {'token':'ZK',  'name':'ZKsync Era',       'date':'2026-06-17','pct':3.4,'cat':'Ecosystem', 'note':'月度生态系统 + 投资人线性归属'},
    {'token':'ZK',  'name':'ZKsync Era',       'date':'2026-07-17','pct':3.4,'cat':'Ecosystem', 'note':'月度生态系统归属'},
    {'token':'ZK',  'name':'ZKsync Era',       'date':'2026-08-17','pct':3.4,'cat':'Ecosystem', 'note':'月度生态系统归属'},
    # ── STRK ────────────────────────────────────────────────
    {'token':'STRK','name':'Starknet',         'date':'2026-06-15','pct':2.8,'cat':'VC',        'note':'投资人 + 核心贡献者月度'},
    {'token':'STRK','name':'Starknet',         'date':'2026-07-15','pct':2.8,'cat':'VC',        'note':'投资人 + 核心贡献者月度'},
    {'token':'STRK','name':'Starknet',         'date':'2026-08-15','pct':2.8,'cat':'VC',        'note':'投资人 + 核心贡献者月度'},
    # ── ENA ─────────────────────────────────────────────────
    {'token':'ENA', 'name':'Ethena',           'date':'2026-06-02','pct':1.5,'cat':'Team',      'note':'核心贡献者归属'},
    {'token':'ENA', 'name':'Ethena',           'date':'2026-07-02','pct':1.5,'cat':'Team',      'note':'核心贡献者归属'},
    {'token':'ENA', 'name':'Ethena',           'date':'2026-08-02','pct':1.5,'cat':'Team',      'note':'核心贡献者归属'},
    # ── W (Wormhole) ────────────────────────────────────────
    {'token':'W',   'name':'Wormhole',         'date':'2026-06-17','pct':1.6,'cat':'VC',        'note':'投资人 + 核心贡献者线性归属'},
    {'token':'W',   'name':'Wormhole',         'date':'2026-07-17','pct':1.6,'cat':'VC',        'note':'投资人 + 核心贡献者线性归属'},
    {'token':'W',   'name':'Wormhole',         'date':'2026-08-17','pct':1.6,'cat':'VC',        'note':'投资人 + 核心贡献者线性归属'},
    # ── PYTH ────────────────────────────────────────────────
    {'token':'PYTH','name':'Pyth Network',     'date':'2026-06-20','pct':1.4,'cat':'Ecosystem', 'note':'网络参与者 + 生态月度激励'},
    {'token':'PYTH','name':'Pyth Network',     'date':'2026-07-20','pct':1.4,'cat':'Ecosystem', 'note':'网络参与者 + 生态月度激励'},
    {'token':'PYTH','name':'Pyth Network',     'date':'2026-08-20','pct':1.4,'cat':'Ecosystem', 'note':'网络参与者 + 生态月度激励'},
    # ── JUP ─────────────────────────────────────────────────
    {'token':'JUP', 'name':'Jupiter',          'date':'2026-06-18','pct':1.5,'cat':'Team',      'note':'团队 + 流动性激励月度归属'},
    {'token':'JUP', 'name':'Jupiter',          'date':'2026-07-18','pct':1.5,'cat':'Team',      'note':'团队 + 流动性激励月度归属'},
    {'token':'JUP', 'name':'Jupiter',          'date':'2026-08-18','pct':1.5,'cat':'Team',      'note':'团队 + 流动性激励月度归属'},
    # ── PENGU ───────────────────────────────────────────────
    {'token':'PENGU','name':'Pudgy Penguins',  'date':'2026-06-15','pct':1.8,'cat':'Team',      'note':'团队 + 早期投资人线性归属'},
    {'token':'PENGU','name':'Pudgy Penguins',  'date':'2026-07-15','pct':1.8,'cat':'Team',      'note':'团队 + 早期投资人线性归属'},
    {'token':'PENGU','name':'Pudgy Penguins',  'date':'2026-08-15','pct':1.8,'cat':'Team',      'note':'团队 + 早期投资人线性归属'},
    # ORDI → BRC-20 无传统归属计划 | XPL/MONAD → 主网前无代币归属数据
]

@app.route('/api/unlocks')
def api_unlocks():
    today   = date.today()
    window  = today + timedelta(days=45)
    today_s = today.strftime('%Y-%m-%d')
    win_s   = window.strftime('%Y-%m-%d')
    # Use dict copies to avoid mutating _UNLOCK_SCHEDULE in place
    events = [dict(e) for e in _UNLOCK_SCHEDULE if today_s <= e['date'] <= win_s]
    events.sort(key=lambda x: x['date'])
    for e in events:
        e['days_until'] = (date.fromisoformat(e['date']) - today).days
    return jsonify({'items': events, 'source': '近似数据 · 以项目官方公告为准',
                    'generated': today_s, 'window_days': 45})

# ── 7. CoinMarketCal 事件（可选 Key） ───────────────────────
def collect_events() -> dict:
    cached = cache_get('events')
    if cached: return cached
    if not COINMARKETCAL_KEY:
        return {'items':[],'has_key':False}
    today  = date.today().strftime('%Y-%m-%d')
    future = (date.today()+timedelta(days=21)).strftime('%Y-%m-%d')
    try:
        d = get_json('https://developers.coinmarketcal.com/v1/events',
                     params={'dateRangeStart':today,'dateRangeEnd':future,
                             'sortBy':'created_desc','page':1,'max':30},
                     headers={'x-api-key':COINMARKETCAL_KEY,'Accept':'application/json'},timeout=12)
        watched = set(COINS.keys())
        items = []
        for ev in d.get('body',[]):
            coins=[c.get('symbol','').upper() for c in ev.get('coins',[])]
            if not any(c in watched for c in coins) and ev.get('percent',0)<60: continue
            items.append({'title':ev.get('title',{}).get('en',''),'date':ev.get('date_event','')[:10],
                          'coins':coins,'categories':[c.get('name','') for c in ev.get('categories',[])][:2],
                          'importance':ev.get('percent',0)})
        result={'items':items[:20],'has_key':True}
    except Exception as e:
        result={'items':[],'has_key':True,'error':str(e)}
    cache_set('events', result, 1800)
    return result

# ════════════════════════════════════════════════════════════════
#  FRED 宏观流动性数据（美联储资产负债表 / RRP / M2）
# ════════════════════════════════════════════════════════════════
# value 单位统一换算为美元（不是百万/十亿），方便前端用 fmtL 格式化
_FRED_SERIES = {
    'walcl': ('WALCL',     1e6),  # 美联储总资产，周频，单位：百万美元
    'rrp':   ('RRPONTSYD', 1e9),  # 隔夜逆回购，日频，单位：十亿美元
    'm2':    ('M2SL',      1e9),  # 美国 M2 货币供应，月频，单位：十亿美元
}

def _fred_series(series_id: str, scale: float) -> dict:
    # 注意：FRED 走代理时若带浏览器 User-Agent 会被代理路由到无法访问该域名的节点而卡死，
    # 这里覆盖为默认 UA 以确保走可用节点
    d = get_json('https://api.stlouisfed.org/fred/series/observations',
                 params={'series_id': series_id, 'api_key': FRED_API_KEY,
                         'file_type': 'json', 'sort_order': 'desc', 'limit': 2},
                 headers={'User-Agent': 'python-requests'},
                 timeout=10)
    obs = [o for o in d.get('observations', []) if o.get('value') not in (None, '.')]
    if not obs:
        return {'error': 'no data'}
    latest = obs[0]
    value = float(latest['value']) * scale
    change_pct = None
    if len(obs) > 1:
        prev = float(obs[1]['value'])
        if prev:
            change_pct = round((value - prev * scale) / (prev * scale) * 100, 3)
    return {'value': value, 'date': latest['date'], 'change_pct': change_pct}

# ════════════════════════════════════════════════════════════════
#  宏观指标（Yahoo Finance：DXY / 美债 / 股指 / VIX / 商品）
# ════════════════════════════════════════════════════════════════
_YAHOO_MACRO_SYMBOLS = ['DX-Y.NYB','^IRX','^TNX','^IXIC','^GSPC','^VIX','GC=F','CL=F']

def _yahoo_quote(sym: str):
    d = get_json(f'https://query2.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym)}',
                 params={'interval':'1d','range':'2d','includePrePost':'false'}, timeout=8)
    meta = d.get('chart',{}).get('result',[{}])[0].get('meta',{})
    price = meta.get('regularMarketPrice')
    if price is None:
        return None
    prev = meta.get('previousClose') or meta.get('chartPreviousClose')
    chg = (price - prev) / prev * 100 if prev else None
    return {'price': price, 'chg': chg}

def collect_macro_yahoo() -> dict:
    cached = cache_get('macro_yahoo')
    if cached: return cached
    result = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {sym: pool.submit(_yahoo_quote, sym) for sym in _YAHOO_MACRO_SYMBOLS}
        for sym, fut in futs.items():
            try:
                q = fut.result(timeout=10)
                if q: result[sym] = q
            except Exception:
                pass
    cache_set('macro_yahoo', result, 60)
    return result

def collect_fred_macro() -> dict:
    if not FRED_API_KEY:
        return {'configured': False}
    cached = cache_get('fred')
    if cached: return cached
    result = {'configured': True}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futs = {k: pool.submit(_fred_series, sid, scale) for k, (sid, scale) in _FRED_SERIES.items()}
        for k, fut in futs.items():
            try:
                result[k] = fut.result(timeout=12)
            except Exception as e:
                result[k] = {'error': str(e)}
    cache_set('fred', result, 21600)  # 6 小时缓存，FRED 数据更新不频繁
    return result

# ════════════════════════════════════════════════════════════════
#  API 端点
# ════════════════════════════════════════════════════════════════

@app.route('/api/status')
def api_status():
    return jsonify({
        'coinmarketcal_key': bool(COINMARKETCAL_KEY),
        'coingecko_key':     bool(COINGECKO_KEY),
        'anthropic_key':     bool(ANTHROPIC_API_KEY),
        'fred_key':          bool(FRED_API_KEY),
        'has_anthropic_lib': HAS_ANTHROPIC,
        'media_feeds':       len(_MEDIA_RSS),
        'extra_feeds':       len(EXTRA_RSS),
        'google_topics':     len(GOOGLE_TOPICS),
        'coins':             list(COINS.keys()),
    })

@app.route('/api/prices')
def api_prices():
    raw = request.args.get('coins', ','.join(COINS.keys()))
    tickers = [t.upper().strip() for t in raw.split(',') if t.strip()]
    return jsonify(collect_prices(tickers))

@app.route('/api/market')
def api_market():
    return jsonify(collect_coingecko())

@app.route('/api/rss')
def api_rss():
    return jsonify(collect_rss_intel())

@app.route('/api/events')
def api_events():
    return jsonify(collect_events())

@app.route('/api/onchain')
def api_onchain():
    return jsonify(collect_defi_llama())

@app.route('/api/fred')
def api_fred():
    return jsonify(collect_fred_macro())

@app.route('/api/macro')
def api_macro():
    return jsonify(collect_macro_yahoo())

@app.route('/api/debug_llama')
def api_debug_llama():
    """诊断端点：返回 DefiLlama 实际链名 / 稳定币链名 / DEX 测试结果"""
    try:
        with ThreadPoolExecutor(max_workers=6) as pool:
            fc   = pool.submit(lambda: get_json('https://api.llama.fi/v2/chains',               timeout=12))
            fsc  = pool.submit(lambda: get_json('https://stablecoins.llama.fi/stablecoinchains',timeout=10))
            fdex = {n: pool.submit(lambda n=n: get_json(
                        f'https://api.llama.fi/overview/dexs/{urllib.parse.quote(n)}'
                        '?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume',
                        timeout=8))
                    for n in ['Hyperliquid','World Chain','Worldchain','Plasma','Solana','Aptos']}

        chains_raw = fc.result()
        chain_names = sorted([c.get('name','') for c in chains_raw])
        chain_lk    = {c.get('name','').lower(): c for c in chains_raw}

        sc_raw   = fsc.result()
        sc_names = sorted([x.get('name','') for x in (sc_raw if isinstance(sc_raw, list) else [])])
        sc_lk    = {x.get('name','').lower(): (x.get('totalCirculatingUSD') or {}).get('peggedUSD',0)
                    for x in (sc_raw if isinstance(sc_raw, list) else [])}

        dex_results = {}
        for n, f in fdex.items():
            try:
                data = f.result()
                dex_results[n] = {'ok': True, 'total24h': data.get('total24h')}
            except Exception as e:
                dex_results[n] = {'ok': False, 'error': str(e)[:120]}

        watchlist_debug = []
        for name in _WATCH_CHAINS + _WATCH_PROTOS:
            c = chain_lk.get(name.lower())
            watchlist_debug.append({
                'name':       name,
                'found':      c is not None,
                'tvl':        c.get('tvl')       if c else None,
                'change_7d':  c.get('change_7d') if c else None,
                'stable_mcap':sc_lk.get(name.lower()),
            })

        return jsonify({
            'chain_names_sample': chain_names[:80],   # first 80 sorted
            'stable_chain_names': sc_names[:60],
            'dex_test':           dex_results,
            'watchlist_debug':    watchlist_debug,
        })
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/intel')
def api_intel():
    """全部情报一次性返回（前端单次请求）"""
    cached = cache_get('intel_all')
    if cached: return jsonify(cached)

    tickers = list(COINS.keys())
    with ThreadPoolExecutor(max_workers=5) as pool:
        tasks = {
            'prices':  pool.submit(collect_prices,    tickers),
            'market':  pool.submit(collect_coingecko),
            'rss':     pool.submit(collect_rss_intel),
            'events':  pool.submit(collect_events),
            'onchain': pool.submit(collect_defi_llama),
        }
        result = {}
        for key, future in tasks.items():
            try:    result[key] = future.result(timeout=25)
            except Exception as e: result[key] = {'error':str(e)}

    result['timestamp'] = datetime.now().isoformat()
    cache_set('intel_all', result, 300)
    return jsonify(result)

@app.route('/api/analyze', methods=['POST'])
def api_analyze():
    """Claude 市场叙事分析（Narrative Analyst）"""
    if not ANTHROPIC_API_KEY:
        return jsonify({'error': '请在 .env 中配置 ANTHROPIC_API_KEY，然后重启服务器'}), 500
    if not HAS_ANTHROPIC:
        return jsonify({'error': '请运行: pip install anthropic'}), 500

    body           = request.get_json(silent=True) or {}
    ticker         = body.get('coin', '').upper()
    pdata          = body.get('price_data', {})
    relevant_news  = body.get('relevant_news',  [])   # 该币相关新闻
    lookonchain    = body.get('lookonchain_news', [])  # Lookonchain 专项
    topic_items    = body.get('topic_items', [])       # Google News 主题
    topic_label    = body.get('topic_label', ticker)
    mention_cnt    = body.get('mention_count', 0)
    keywords       = body.get('related_keywords', [])

    coin_name = COINS.get(ticker, {}).get('name', ticker)
    ma   = pdata.get('ma',   {})
    rsi  = pdata.get('rsi',  {})
    macd = pdata.get('macd', {})
    vol  = pdata.get('vol',  {})

    # ── 格式化各数据块 ────────────────────────────────────────
    news_txt = '\n'.join(
        f'• {n["title"]}  [{n.get("source","")} {n.get("published","")}]'
        for n in relevant_news[:6]
    ) or '（无相关新闻）'

    lkc_txt = '\n'.join(
        f'• {n["title"]}'
        for n in lookonchain[:5]
    ) or '（无 Lookonchain 数据）'

    topic_txt = '\n'.join(
        f'• {n["title"]}'
        for n in topic_items[:5]
    ) or '（无 Google News 结果）'

    kw_txt = '、'.join(keywords[:12]) or '（无关键词）'

    score = pdata.get('score', 0)
    score_str = f'{score:+d}/6（{"偏多" if score>=2 else "偏空" if score<=-2 else "中性"}）'

    # ── Narrative Analyst Prompt ──────────────────────────────
    prompt = f"""你是一名专业的加密货币市场叙事分析师（Crypto Narrative Analyst）。

你的任务：综合以下所有来源的数据，对【{ticker}（{coin_name}）】做"市场叙事质量"判断，而不是简单重复新闻。

分析重点：
① 叙事是否有机升温，还是资本试探/短期炒作？
② 链上信号与媒体热度是否匹配？
③ 当前处于哪个阶段：底部横盘 / 启动初期 / 拉升途中 / 高位分发？
④ 操作建议要具体，不能模糊

════════════════════════════════
【技术面】
价格：${pdata.get('price',0):.6g}（24h {pdata.get('chg24',0):+.2f}%）
均线：{ma.get('signal','—')}（MA5={ma.get('ma5',0):.4g} / MA20={ma.get('ma20',0):.4g}）
RSI：{rsi.get('desc','—')}
MACD：{macd.get('desc','—')}
成交量：{vol.get('desc','—')}
综合评分：{score_str}

【媒体热度】
今日被提及次数：{mention_cnt} 次（跨所有 RSS 源）
相关高频词：{kw_txt}

【相关新闻（RSS 媒体）】
{news_txt}

【Lookonchain 链上动态】
{lkc_txt}

【Google News · {topic_label}】
{topic_txt}
════════════════════════════════

请严格按以下格式输出（总字数不超过 300 字，不要废话）：

【{ticker} · 今日叙事分析】

📊 价格概况
（一句话：价格位置 + 当前技术信号最关键的一点）

🔍 市场情报
• （信号1：从新闻/链上/关键词综合得出的核心叙事观察，不是重复标题）
• （信号2：另一个维度的信号）
• （信号3：可选，若有额外有价值的信号才写）

🧠 AI 判断
（综合叙事质量：有机升温 / 资本试探 / 短期炒作 / 叙事衰退
 当前阶段：底部横盘 / 启动初期 / 拉升途中 / 高位分发
 用2-3句话说清楚判断逻辑）

⚡ 操作建议
【选1-2个】观察 / 不动 / 小额补仓 / 分批止盈 / 风险升高 / 等待确认
（一句理由，15字以内）

⚠️ 仅供参考，不构成投资建议。"""

    try:
        client = _anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        msg    = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=750,
            messages=[{'role': 'user', 'content': prompt}],
        )
        return jsonify({
            'coin':      ticker,
            'summary':   msg.content[0].text,
            'timestamp': datetime.now().strftime('%H:%M'),
            'model':     'claude-sonnet-4-6',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/narrative', methods=['POST'])
def api_narrative():
    """Step 4 — 今日市场叙事主线（Claude 综合所有 RSS + 价格数据）"""
    if not ANTHROPIC_API_KEY:
        return jsonify({'error': '请配置 ANTHROPIC_API_KEY'}), 500
    if not HAS_ANTHROPIC:
        return jsonify({'error': '请安装 anthropic 库'}), 500

    body          = request.get_json(silent=True) or {}
    news_items    = body.get('news_items', [])[:20]
    top_keywords  = body.get('top_keywords', [])[:12]
    coin_mentions = body.get('coin_mentions', [])[:8]
    price_movers  = body.get('price_movers', [])

    news_txt  = '\n'.join(
        f'• {n["title"]}  [{n.get("source","")}]'
        for n in news_items
    ) or '（无数据）'

    kw_txt    = '、'.join(w for w,_ in top_keywords) or '（无）'
    hot_txt   = '、'.join(f'{t}({c}次)' for t,c in coin_mentions) or '（无）'
    mover_txt = '\n'.join(
        f'• {m["ticker"]}: {m.get("chg24",0):+.2f}%  技术{m.get("score",0):+d}/6'
        for m in sorted(price_movers, key=lambda x: abs(x.get('chg24',0)), reverse=True)[:8]
    ) or '（无）'

    prompt = f"""你是一名宏观加密市场叙事研究员（Crypto Narrative Researcher）。

根据以下今日数据，识别市场正在交易的主叙事，判断每个叙事的热度和方向。
重要：要综合判断，不要简单罗列新闻。

════════════════════════════════
【今日新闻标题（前20条）】
{news_txt}

【高频关键词】
{kw_txt}

【媒体热议币种】
{hot_txt}

【价格异动】
{mover_txt}
════════════════════════════════

识别今日最主要的 3-5 个市场叙事，按热度排列。

输出格式：

**🔥 叙事1：[叙事名称]**
相关资产：[币种或赛道]
信号：[2-3个关键证据，综合判断而非重复标题]
热度：🔥🔥🔥（满分5🔥）
方向：升温 / 稳定 / 降温

（继续叙事2、3、4...）

最后一行：⚠️ 仅供参考，不构成投资建议。"""

    try:
        client = _anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        msg    = client.messages.create(
            model='claude-sonnet-4-6', max_tokens=900,
            messages=[{'role': 'user', 'content': prompt}],
        )
        return jsonify({'narrative': msg.content[0].text,
                        'timestamp': datetime.now().strftime('%H:%M')})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# 静态文件白名单：只允许这些扩展名通过 /<path:path> 访问，
# 防止 server.py / .env / requirements.txt 等敏感文件被直接下载
_STATIC_EXT_WHITELIST = {
    '.html', '.css', '.js', '.json', '.png', '.jpg', '.jpeg',
    '.svg', '.ico', '.woff', '.woff2', '.ttf', '.map',
}

@app.route('/')
def root(): return send_from_directory('.', 'index.html')

@app.route('/<path:path>')
def static_file(path):
    ext = os.path.splitext(path)[1].lower()
    if ext not in _STATIC_EXT_WHITELIST or any(part.startswith('.') for part in path.split('/')):
        return jsonify({'error': 'not found'}), 404
    return send_from_directory('.', path)

# ════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    import sys
    # Windows GBK 终端不支持 emoji，统一用 UTF-8 输出
    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

    sep = '=' * 54
    ck  = lambda v: '[OK]' if v else '[--]'
    print('\n' + sep)
    print('  Crypto Intelligence Server  v3')
    print(sep)
    print(f'  Media RSS     : {len(_MEDIA_RSS)} feeds (free)')
    print(f'  Extra RSS     : {len(EXTRA_RSS)} feeds (from .env)')
    print(f'  Google News   : {len(GOOGLE_TOPICS)} topics')
    print(f'  DefiLlama TVL : free, no key')
    print(f'  CoinGecko     : free {"+ key" if COINGECKO_KEY else ""}')
    print(f'  CoinMarketCal : {ck(COINMARKETCAL_KEY)} {"configured" if COINMARKETCAL_KEY else "not set"}')
    print(f'  Claude API    : {ck(ANTHROPIC_API_KEY)} {"configured" if ANTHROPIC_API_KEY else "not set"}')
    print(f'  FRED API      : {ck(FRED_API_KEY)} {"configured" if FRED_API_KEY else "not set (Fed/RRP/M2 will show as pending)"}')
    print()
    _host = os.environ.get('HOST', '127.0.0.1')
    _port = int(os.environ.get('PORT', 8888))
    print('  Browser URLs:')
    print(f'  Price   -> http://localhost:{_port}/')
    print(f'  Tech    -> http://localhost:{_port}/analysis.html')
    print(f'  Intel   -> http://localhost:{_port}/intelligence.html')
    print(sep + '\n')
    app.run(host=_host, port=_port, debug=False, threaded=True)
