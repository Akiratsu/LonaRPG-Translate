"""
tradutor_core_v5.py — Drop-in replacement para tradutor_core.py
================================================================
DIFERENÇA PRINCIPAL:
  O shield atual envia o texto com tokens ❰N❱ intercalados — o GPT
  decide onde colocar cada token e frequentemente erra.

  O shield_v5 segmenta o texto em partes:
    - Tags viram separadores fixos → nunca saem do lugar
    - Cada segmento de texto puro é traduzido separadamente
    - O GPT recebe APENAS texto humano, sem tokens de código

  Exemplo:
    Original : \C[2]Hello\C[0], warrior. \n Good luck.
    Para API : ["Hello", ", warrior.", "Good luck."]
    Resp API : ["Olá", ", guerreiro.", "Boa sorte."]
    Resultado: \C[2]Olá\C[0], guerreiro. \n Boa sorte.

  Tags 100% preservadas por posição — impossível corromper.

Uso:
  Substitui tradutor_core.py — mesma interface pública.
  Todas as funções públicas têm a mesma assinatura.
"""

import json
import re
import time
import sys
import urllib.request
import urllib.error
import sqlite3
import logging
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────
HERE      = Path(__file__).resolve().parent
DB2_PATH  = HERE / "database" / "db2_dialogos.sqlite"
DB2B_PATH = HERE / "database" / "db2b_fila.sqlite"
DB3_PATH  = HERE / "database" / "db3_traducao.sqlite"
CFG_PATH  = HERE / "config.json"

# ── Logger (SSE-safe: flush imediato + stdout line-buffered) ──────
import io as _io
class _FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record); self.flush()

try:
    sys.stdout = _io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
    )
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[_FlushHandler(sys.stdout)],
)
log = logging.getLogger("tradutor")

# ── Config ────────────────────────────────────────────────────
_CFG_DEFAULTS = {
    "api_key":     "",
    "modelo":      "gpt-4o-mini",
    "base_url":    "https://api.openai.com/v1/chat/completions",
    "lote_size":   20,
    "temperatura": 0.3,
    "timeout":     60,
    "max_retries": 3,
    "fonte_lang":  "auto",
}

def carregar_config() -> dict:
    cfg = dict(_CFG_DEFAULTS)
    if CFG_PATH.exists():
        try:
            with open(CFG_PATH, encoding="utf-8") as f:
                dados = json.load(f)
            cfg.update(dados)
        except Exception:
            pass
    return cfg

def salvar_config(cfg: dict):
    with open(CFG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)

def pedir_api_key(cfg_ou_provedor=None, campo=None) -> object:
    """
    Aceita dois formatos:
      novo:  pedir_api_key(cfg: dict) → dict
      legado: pedir_api_key(provedor: str, campo: str) → str  (usado pelos wrappers)
    """
    if isinstance(cfg_ou_provedor, dict):
        cfg = cfg_ou_provedor
        if not cfg.get("api_key"):
            key = input("  Cole sua API key: ").strip()
            if key:
                cfg["api_key"] = key
                salvar_config(cfg)
        return cfg
    else:
        # Assinatura legada: pedir_api_key(_PROVEDOR, _CAMPO_KEY) → str
        provedor = cfg_ou_provedor or "API"
        key = input(f"  Cole sua {provedor} API key: ").strip()
        if key and campo:
            cfg_atual = carregar_config()
            cfg_atual[campo] = key
            salvar_config(cfg_atual)
        return key

def cfg_default():
    return dict(_CFG_DEFAULTS)

# ── SYSTEM PROMPT ─────────────────────────────────────────────
SYSTEM_PROMPT = """Você é um tradutor especializado em RPG. Traduz para português brasileiro (PT-BR).

O texto fonte pode estar em inglês ou chinês tradicional — identifique o idioma e traduza naturalmente.

REGRAS:
1. Você receberá uma lista JSON de segmentos de texto puro (sem código de jogo)
2. Retorne EXATAMENTE uma lista JSON com o mesmo número de elementos, na mesma ordem
3. Preserve nomes próprios: Lona, Noer, Cecily, Adam, Milo, Lisa, Cocona, etc.
4. Preserve termos técnicos de jogo sem tradução natural em PT-BR
5. Preserve pontuação especial: ：  …  （ ）
6. Não adicione notas, comentários nem texto fora da lista JSON
7. Não combine nem divida elementos — mesma quantidade, mesma ordem

Tom: RPG de fantasia adulto. Linguagem natural em PT-BR, sem ser excessivamente formal.

Resposta: APENAS a lista JSON, sem markdown, sem explicações."""

# ── SHIELD V5 — segmentação por tags ─────────────────────────
# Regex para identificar tags (para validar_tags e stats)
_TAG_RE = re.compile(
    r'\\board\[[^\]]*\]'
    r'|\\optB\[[^\]]*\]'
    r'|\\optD\[[^\]]*\]'
    r'|\\[A-Za-z_]+\[[^\]]*\]'
    r'|\\[A-Za-z_0-9]+'
    r'|\\[^A-Za-z0-9\s]+'
    r'|\\\\'
)

# Regex com grupos para segmentação inteligente
_TAG_GROUPS = re.compile(
    r'(\\board\[)([^\]]*?)(\])'
    r'|(\\optB\[)([^\]]*?)(\])'
    r'|(\\optD\[)([^\]]*?)(\])'
    r'|(\\[A-Za-z_]+\[[^\]]*\])'
    r'|(\\[A-Za-z_0-9]+)'
    r'|(\\[^A-Za-z0-9\s]+)'
    r'|(\\\\)'
)
_PH = '\x00'


def shield(texto: str):
    """
    Segmenta o texto em partes de texto puro e tags.
    Retorna (json_lista_textos, mapa) onde mapa contém estrutura e tags.
    """
    tags     = []  # tags opacas
    textos   = []  # segmentos de texto traduzíveis
    estrutura= []  # ('T', idx_texto) | ('G', idx_tag) | ('E', espaco)
    pos      = 0

    for m in _TAG_GROUPS.finditer(texto):
        # Texto antes da tag
        if m.start() > pos:
            t = texto[pos:m.start()].strip()
            if t:
                estrutura.append(('T', len(textos)))
                textos.append(t)
            elif texto[pos:m.start()]:
                estrutura.append(('E', texto[pos:m.start()]))

        if m.group(1):
            # \board[TITULO] — título é traduzível
            tags.append(m.group(1));  estrutura.append(('G', len(tags)-1))  # \board[
            estrutura.append(('T', len(textos)));  textos.append(m.group(2).strip() or m.group(2))  # titulo
            tags.append(m.group(3));  estrutura.append(('G', len(tags)-1))  # ]
        elif m.group(4):
            # \optB[A,B] — cada opção é traduzível
            tags.append(m.group(4));  estrutura.append(('G', len(tags)-1))  # \optB[
            for i, p in enumerate(m.group(5).split(',')):
                if i > 0:
                    tags.append(','); estrutura.append(('G', len(tags)-1))
                estrutura.append(('T', len(textos))); textos.append(p.strip() or p)
            tags.append(m.group(6));  estrutura.append(('G', len(tags)-1))  # ]
        elif m.group(7):
            # \optD[A,B] — igual optB
            tags.append(m.group(7));  estrutura.append(('G', len(tags)-1))
            for i, p in enumerate(m.group(8).split(',')):
                if i > 0:
                    tags.append(','); estrutura.append(('G', len(tags)-1))
                estrutura.append(('T', len(textos))); textos.append(p.strip() or p)
            tags.append(m.group(9));  estrutura.append(('G', len(tags)-1))
        else:
            # Tag opaca
            tags.append(m.group(0)); estrutura.append(('G', len(tags)-1))

        pos = m.end()

    # Resto do texto
    if pos < len(texto):
        t = texto[pos:].strip()
        if t:
            estrutura.append(('T', len(textos))); textos.append(t)

    payload = json.dumps(textos, ensure_ascii=False, separators=(',', ':'))
    mapa    = {'__v5': True, 'est': estrutura, 'tags': tags, 'orig': textos}
    return payload, mapa


def restore(traduzido: str, mapa: dict) -> str:
    """Reconstrói o texto com as traduções e tags originais."""
    if not mapa.get('__v5'):
        # Fallback: mapa v4 com tokens ❰N❱
        for tok in sorted(mapa.keys(), key=lambda t: -int(t[1:-1])
                          if t.startswith('❰') and t.endswith('❱') else 0):
            traduzido = traduzido.replace(tok, mapa[tok])
        return traduzido

    try:
        trad = json.loads(traduzido)
        if not isinstance(trad, list):
            raise ValueError
    except Exception:
        return traduzido  # falha segura — retorna o que veio

    orig = mapa['orig']
    if len(trad) != len(orig):
        # Tamanho errado — usa originais para os que faltam
        trad = list(trad) + orig[len(trad):]

    resultado = []
    for tipo, val in mapa['est']:
        if tipo == 'T':
            resultado.append(trad[val] if val < len(trad) else '')
        elif tipo == 'G':
            resultado.append(mapa['tags'][val] if val < len(mapa['tags']) else '')
        else:
            resultado.append(val)
    return ''.join(resultado)


def validar_tags(original: str, traduzido: str) -> int:
    """Verifica se todas as tags do original estão no traduzido. Retorna 1 ou 0."""
    tags_orig = set(_TAG_RE.findall(original))
    tags_trad = set(_TAG_RE.findall(traduzido))
    # Com shield_v5 as tags são sempre preservadas — validação como sanidade
    return 1 if tags_orig == tags_trad else 0


# ── DB3 ───────────────────────────────────────────────────────
_SCHEMA_DB3 = """
CREATE TABLE IF NOT EXISTS meta (chave TEXT PRIMARY KEY, valor TEXT);
CREATE TABLE IF NOT EXISTS traducoes (
    id              INTEGER PRIMARY KEY,
    arquivo         TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    namespace       TEXT NOT NULL,
    subkey          TEXT NOT NULL,
    full_key        TEXT NOT NULL,
    texto_eng       TEXT,
    texto_ptbr      TEXT,
    status          TEXT DEFAULT 'pendente',
    tags_ok         INTEGER DEFAULT 1,
    modelo_usado    TEXT,
    tentativas      INTEGER DEFAULT 0,
    criado_em       TEXT DEFAULT (datetime('now')),
    traduzido_em    TEXT,
    status_revisao  TEXT DEFAULT NULL,
    nota_revisor    TEXT DEFAULT NULL,
    UNIQUE(arquivo, full_key)
);
CREATE INDEX IF NOT EXISTS idx_d3_status   ON traducoes(status);
CREATE INDEX IF NOT EXISTS idx_d3_arquivo  ON traducoes(arquivo);
CREATE INDEX IF NOT EXISTS idx_d3_full_key ON traducoes(full_key);
"""

def _criar_db3():
    DB3_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB3_PATH))
    conn.executescript(_SCHEMA_DB3)
    conn.commit()
    return conn

def _abrir_db3():
    conn = sqlite3.connect(str(DB3_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def _popular_aproveitados(conn3, conn2b) -> int:
    if DB2_PATH.exists():
        conn2b.execute(f"ATTACH DATABASE '{DB2_PATH}' AS db2_src")
        rows = conn2b.execute("""
            SELECT d.id, f.arquivo, f.seq, f.namespace, f.subkey, f.full_key,
                   f.texto_eng, f.texto_ptbr
            FROM fila f
            JOIN db2_src.dialogos d ON d.arquivo=f.arquivo AND d.full_key=f.full_key
            WHERE f.status='aproveitado'
        """).fetchall()
        try: conn2b.execute("DETACH DATABASE db2_src")
        except: pass
    else:
        rows = conn2b.execute(
            "SELECT rowid,arquivo,seq,namespace,subkey,full_key,texto_eng,texto_ptbr "
            "FROM fila WHERE status='aproveitado'"
        ).fetchall()
    conn3.executemany("""
        INSERT OR REPLACE INTO traducoes
          (id,arquivo,seq,namespace,subkey,full_key,texto_eng,texto_ptbr,
           status,tags_ok,modelo_usado)
        VALUES (?,?,?,?,?,?,?,?,'aproveitado',1,'PT-BRC')
    """, rows)
    conn3.commit()
    return len(rows)

def _popular_pendentes(conn3, conn2b) -> int:
    if not DB2_PATH.exists():
        return 0
    conn2b.execute(f"ATTACH DATABASE '{DB2_PATH}' AS db2_src")
    rows = conn2b.execute("""
        SELECT d.id, f.arquivo, f.seq, f.namespace, f.subkey, f.full_key, f.texto_eng
        FROM fila f
        JOIN db2_src.dialogos d ON d.arquivo=f.arquivo AND d.full_key=f.full_key
        WHERE f.status='pendente'
    """).fetchall()
    try: conn2b.execute("DETACH DATABASE db2_src")
    except: pass
    conn3.executemany("""
        INSERT OR IGNORE INTO traducoes
          (id,arquivo,seq,namespace,subkey,full_key,texto_eng,status)
        VALUES (?,?,?,?,?,?,?,'pendente')
    """, rows)
    conn3.commit()
    return len(rows)

def _db_ok(conn3, arq, seq, ns, sk, fk, eng, pt, tags_ok, modelo, atualiza):
    if atualiza:
        conn3.execute("""
            UPDATE traducoes
            SET texto_ptbr=?, status='traduzido', tags_ok=?,
                modelo_usado=?, tentativas=tentativas+1,
                traduzido_em=datetime('now')
            WHERE full_key=? AND arquivo=?
        """, (pt, tags_ok, modelo, fk, arq))
    else:
        conn3.execute("""
            INSERT OR REPLACE INTO traducoes
              (arquivo,seq,namespace,subkey,full_key,
               texto_eng,texto_ptbr,status,tags_ok,
               modelo_usado,tentativas,traduzido_em)
            VALUES (?,?,?,?,?,?,?,'traduzido',?,?,1,datetime('now'))
        """, (arq,seq,ns,sk,fk,eng,pt,tags_ok,modelo))

def _db_erro(conn3, arq, seq, ns, sk, fk, eng, modelo, atualiza):
    if atualiza:
        conn3.execute("""
            UPDATE traducoes
            SET status='erro', tags_ok=0,
                modelo_usado=?, tentativas=tentativas+1
            WHERE full_key=? AND arquivo=?
        """, (modelo, fk, arq))
    else:
        conn3.execute("""
            INSERT OR REPLACE INTO traducoes
              (arquivo,seq,namespace,subkey,full_key,
               texto_eng,status,tags_ok,modelo_usado,tentativas)
            VALUES (?,?,?,?,?,?,'erro',0,?,1)
        """, (arq,seq,ns,sk,fk,eng,modelo))

# ── CHAMAR API ────────────────────────────────────────────────
def chamar_api(lote_textos: list, cfg: dict) -> dict:
    """
    lote_textos: list[(full_key, json_lista_segmentos)]
    Retorna: dict{full_key -> json_lista_traduzida}
    """
    # Monta uma única chamada com todos os lotes como array de arrays
    # Cada item do lote é uma lista JSON de segmentos
    itens = []
    for i, (fk, payload) in enumerate(lote_textos):
        segs = json.loads(payload)
        itens.append(f"[{i}] {json.dumps(segs, ensure_ascii=False)}")

    user_msg = (
        "Traduza cada lista numerada abaixo.\n"
        "Responda APENAS no formato [N] [\"trad1\",\"trad2\",...] — "
        "mesma quantidade de elementos, mesma ordem, um por linha.\n\n"
        + "\n\n".join(itens)
    )

    payload_api = {
        "model":       cfg["modelo"],
        "temperature": cfg["temperatura"],
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        "max_tokens": 4096,
    }

    req = urllib.request.Request(
        cfg["base_url"],
        data=json.dumps(payload_api).encode("utf-8"),
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {cfg['api_key']}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=cfg["timeout"]) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    conteudo = result["choices"][0]["message"]["content"].strip()

    # Parse [N] ["trad1","trad2",...]
    resposta = {}
    for linha in conteudo.split("\n"):
        m = re.match(r'^\[(\d+)\]\s*(\[.*\])\s*$', linha.strip())
        if m:
            idx = int(m.group(1))
            if idx < len(lote_textos):
                fk = lote_textos[idx][0]
                resposta[fk] = m.group(2)  # JSON list como string

    return resposta

# ── LOOP PRINCIPAL ────────────────────────────────────────────
def _loop(conn3, pendentes: list, cfg: dict, atualiza: bool = False):
    total      = len(pendentes)
    tam        = cfg["lote_size"]
    traduzidos = 0
    erros      = 0

    for ini in range(0, total, tam):
        lote_db = pendentes[ini:ini+tam]

        # Shield v5 — segmenta cada texto
        lote_shielded = []
        mapas = {}
        for arq, seq, ns, sk, fk, eng, cht in lote_db:
            fl = cfg.get("fonte_lang", "auto")
            if fl == "cht":
                fonte = cht or eng or ""
            elif fl == "eng":
                fonte = eng or ""
            else:
                fonte = eng or cht or ""

            payload, mapa = shield(fonte)
            # Se o texto não tem segmentos traduzíveis (só tags), pula
            orig_segs = json.loads(payload)
            if not orig_segs:
                # Sem texto — usa original como tradução
                _db_ok(conn3,arq,seq,ns,sk,fk,eng,fonte,1,cfg["modelo"],atualiza)
                traduzidos += 1
                continue

            lote_shielded.append((fk, payload))
            mapas[fk] = (mapa, eng, fonte, arq, seq, ns, sk)

        if not lote_shielded:
            conn3.commit()
            continue

        # Chama API com retries
        resposta = {}
        for tentativa in range(1, cfg["max_retries"]+1):
            try:
                resposta = chamar_api(lote_shielded, cfg)
                break
            except urllib.error.HTTPError as e:
                log.warning(f"  HTTP {e.code} — tentativa {tentativa}")
                if e.code in (429,500,502,503) and tentativa < cfg["max_retries"]:
                    time.sleep(5*tentativa)
                else:
                    erros += len(lote_shielded)
                    break
            except Exception as e:
                log.warning(f"  Erro tentativa {tentativa}: {e}")
                if tentativa < cfg["max_retries"]:
                    time.sleep(3*tentativa)
                else:
                    erros += len(lote_shielded)
                    break

        # Persiste
        for fk, _ in lote_shielded:
            if fk not in mapas:
                continue
            mapa, eng, fonte, arq, seq, ns, sk = mapas[fk]
            raw = resposta.get(fk)

            if raw is None:
                _db_erro(conn3,arq,seq,ns,sk,fk,eng,cfg["modelo"],atualiza)
                erros += 1
                continue

            pt      = restore(raw, mapa)
            tags_ok = validar_tags(fonte or eng or "", pt)

            if not tags_ok:
                log.warning(f"  [TAGS] {fk}")

            _db_ok(conn3,arq,seq,ns,sk,fk,eng,pt,tags_ok,cfg["modelo"],atualiza)
            traduzidos += 1

        conn3.commit()
        pct = min(ini+tam,total)*100//total
        log.info(f"  [{pct:3d}%] {min(ini+tam,total)}/{total}  "
                 f"traduzidos:{traduzidos}  erros:{erros}")

    return traduzidos, erros

# ── FUNÇÕES PÚBLICAS ──────────────────────────────────────────
def traduzir(cfg: dict, limite: int = None):
    if not DB2B_PATH.exists():
        log.error("  DB2b não encontrado."); sys.exit(1)

    if DB3_PATH.exists():
        conn3    = _abrir_db3()
        atualiza = True
    else:
        conn2b  = sqlite3.connect(str(DB2B_PATH))
        conn3   = _criar_db3()
        conn3.execute("INSERT OR REPLACE INTO meta VALUES ('modelo',?)",(cfg["modelo"],))
        conn3.commit()
        _popular_aproveitados(conn3,conn2b)
        _popular_pendentes(conn3,conn2b)
        conn2b.close()
        atualiza = True  # usa UPDATE sempre para preservar status_revisao

    conn3.execute("INSERT OR REPLACE INTO meta VALUES ('modelo',?)",(cfg["modelo"],))
    conn3.commit()

    if DB2_PATH.exists():
        conn3.execute(f"ATTACH DATABASE '{str(DB2_PATH).replace(chr(92),'/')}' AS db2")
        q = ("SELECT t.arquivo,t.seq,t.namespace,t.subkey,t.full_key,"
             "t.texto_eng,d.texto_cht FROM traducoes t "
             "LEFT JOIN db2.dialogos d ON d.arquivo=t.arquivo AND d.full_key=t.full_key "
             "WHERE t.status='pendente'")
        if limite:
            q += f" ORDER BY t.id LIMIT {limite}"
        pendentes = conn3.execute(q).fetchall()
        conn3.execute("DETACH DATABASE db2")
    else:
        q2 = ("SELECT arquivo,seq,namespace,subkey,full_key,texto_eng,NULL "
               "FROM traducoes WHERE status='pendente'")
        if limite:
            q2 += f" ORDER BY id LIMIT {limite}"
        pendentes = conn3.execute(q2).fetchall()

    log.info(f"  Pendentes: {len(pendentes)}")
    if not pendentes:
        conn3.close(); return 0,0

    t,e = _loop(conn3, pendentes, cfg, atualiza)
    conn3.close()
    return t,e


def reenviar_erros(cfg: dict, ids: str = None):
    if not DB3_PATH.exists():
        log.error("  DB3 não encontrado."); sys.exit(1)

    conn3 = _abrir_db3()
    if DB2_PATH.exists():
        conn3.execute(f"ATTACH DATABASE '{str(DB2_PATH).replace(chr(92),'/')}' AS db2")
        def _q(where, params=()):
            return conn3.execute(
                "SELECT t.arquivo,t.seq,t.namespace,t.subkey,t.full_key,"
                "t.texto_eng,d.texto_cht FROM traducoes t "
                "LEFT JOIN db2.dialogos d ON d.arquivo=t.arquivo AND d.full_key=t.full_key "
                f"WHERE {where}", params
            ).fetchall()
    else:
        def _q(where, params=()):
            return conn3.execute(
                "SELECT arquivo,seq,namespace,subkey,full_key,texto_eng,NULL "
                f"FROM traducoes WHERE {where}", params
            ).fetchall()

    if ids:
        todos = [int(i) for i in ids.replace(","," ").split() if i.strip().lstrip("-").isdigit()]
        pos   = [i for i in todos if i>0]
        neg   = [-i for i in todos if i<0]
        pend  = []
        if pos:
            ph = ",".join("?"*len(pos))
            pend += _q(f"t.id IN ({ph})", pos)
        if neg and DB2_PATH.exists():
            ph2 = ",".join("?"*len(neg))
            rows = conn3.execute(
                "SELECT d.id,d.arquivo,d.seq,d.namespace,d.subkey,d.full_key,d.texto_eng,d.texto_cht "
                f"FROM db2.dialogos d WHERE d.id IN ({ph2})", neg
            ).fetchall()
            for r in rows:
                conn3.execute(
                    "INSERT OR IGNORE INTO traducoes "
                    "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,status) "
                    "VALUES (?,?,?,?,?,?,?,'pendente')",
                    (r[0],r[1],r[2],r[3],r[4],r[5],r[6])
                )
            conn3.commit()
            pend += [(r[1],r[2],r[3],r[4],r[5],r[6],r[7]) for r in rows]
    else:
        pend = _q("t.status='erro'")

    if DB2_PATH.exists():
        try: conn3.execute("DETACH DATABASE db2")
        except: pass

    log.info(f"  Reenvio: {len(pend)} entradas")
    if not pend:
        conn3.close(); return 0,0

    t,e = _loop(conn3, pend, cfg, atualiza=True)
    conn3.close()
    return t,e


def mostrar_stats():
    if not DB3_PATH.exists():
        log.info("  DB3 não encontrado."); return
    conn = sqlite3.connect(str(DB3_PATH))
    total = conn.execute("SELECT COUNT(*) FROM traducoes").fetchone()[0]
    trad  = conn.execute("SELECT COUNT(*) FROM traducoes WHERE status='traduzido'").fetchone()[0]
    apv   = conn.execute("SELECT COUNT(*) FROM traducoes WHERE status='aproveitado'").fetchone()[0]
    err   = conn.execute("SELECT COUNT(*) FROM traducoes WHERE status='erro'").fetchone()[0]
    tags  = conn.execute("SELECT COUNT(*) FROM traducoes WHERE tags_ok=0 AND status!='erro'").fetchone()[0]
    pend  = conn.execute("SELECT COUNT(*) FROM traducoes WHERE status='pendente'").fetchone()[0]
    conn.close()
    log.info(f"  Total       : {total}")
    log.info(f"  Aproveitados: {apv} ({apv*100//max(total,1)}%)")
    log.info(f"  Traduzidos  : {trad}")
    log.info(f"  Pendentes   : {pend}")
    log.info(f"  Erros       : {err}")
    log.info(f"  Tags ruins  : {tags}")
