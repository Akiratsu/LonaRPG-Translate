"""
LonaRPG DB1 - Extrator de Estrutura
=====================================
Fonte: pasta CHT (versao chinesa do jogo)

Para cada entrada no CHT:
  - Texto SEM caracteres chineses = CODIGO   -> texto copiado para DB1
  - Texto COM caracteres chineses = DIALOGO  -> salva so a marcacao, texto=NULL

Resultado: DB1 contem o mapa completo de todas as chaves.
  tipo='codigo'  -> texto real do motor (igual em todos os idiomas)
  tipo='dialogo' -> texto=NULL: o DB2 vai buscar esse texto em todos os idiomas

Estrutura esperada (dentro da mesma pasta que este script):
  Pipeline/
    extrator.py  <- este arquivo
    CHT/         <- OBRIGATORIO: fonte da verdade
    ENG/         <- opcional (DB2 vai usar)
    KOR/         <- opcional
    RUS/         <- opcional
    PT-BRC/      <- opcional
    database/    <- criado automaticamente
"""

import re
import sys
import sqlite3
import logging
from pathlib import Path

HERE     = Path(__file__).resolve().parent
ROOT     = HERE.parent   # Pipeline\ — onde ficam CHT\, ENG\, PT-BR\ etc.
DB_PATH  = HERE / "database" / "db1_estrutura.sqlite"
LOG_PATH = HERE / "db1_extrator.log"

import io
# Força UTF-8 no stdout (evita UnicodeEncodeError no Windows com cp1252)
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler(str(LOG_PATH), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

KEY_RE = re.compile(r'^([A-Za-z0-9_]+)/([A-Za-z0-9_]+)\s*$')
CJK_RE = re.compile(r'[\u4e00-\u9fff\u3400-\u4dbf\u3040-\u30ff\uac00-\ud7af]')


def tem_cjk(texto):
    return bool(CJK_RE.search(texto or ""))


def achar_pasta_cht():
    for nome in ["CHT", "ZH", "ZH-TW", "ZH-CN", "Chinese", "chinese", "cht"]:
        p = ROOT / nome
        if p.is_dir() and any(p.rglob("*.txt")):
            return p
    return None


def listar_pastas():
    res = []
    IGNORAR = {"database", "mods", "__pycache__"}
    for d in sorted(ROOT.iterdir()):
        if d.is_dir() and d.name.lower() not in IGNORAR:
            txts = list(d.rglob("*.txt"))
            if txts:
                res.append((d.name, len(txts)))
    return res


# ═══════════════════════════════════════════════════════════════
# PARSER
# ═══════════════════════════════════════════════════════════════

def parse_arquivo(caminho):
    entradas = []
    seq = 0
    cur_ns = cur_sk = cur_key = None
    cur_linhas = []
    cur_ini = 0

    try:
        linhas = caminho.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    except Exception as e:
        log.error(f"  [ERRO] {caminho}: {e}")
        return []

    def flush():
        nonlocal seq
        if cur_key is None:
            return
        texto = "".join(cur_linhas).rstrip("\n\r ")
        entradas.append({
            "seq"      : seq,
            "namespace": cur_ns,
            "subkey"   : cur_sk,
            "full_key" : cur_key,
            "texto"    : texto,
            "linha_ini": cur_ini,
        })
        seq += 1

    for num, linha in enumerate(linhas, start=1):
        s = linha.rstrip("\n\r")
        if s.lstrip().startswith("#"):
            flush()
            cur_ns = cur_sk = cur_key = None
            cur_linhas = []
            continue
        if s.strip() == "" and cur_key is None:
            continue
        m = KEY_RE.match(s)
        if m:
            flush()
            cur_ns  = m.group(1)
            cur_sk  = m.group(2)
            cur_key = f"{cur_ns}/{cur_sk}"
            cur_linhas = []
            cur_ini = num
            continue
        if cur_key is not None:
            cur_linhas.append(linha)

    flush()
    return entradas


# ═══════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    chave TEXT PRIMARY KEY,
    valor TEXT
);
CREATE TABLE IF NOT EXISTS arquivos (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    rel_path     TEXT NOT NULL UNIQUE,
    qtd_codigo   INTEGER DEFAULT 0,
    qtd_dialogo  INTEGER DEFAULT 0,
    extraido_em  TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS entradas (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    arquivo_id  INTEGER NOT NULL REFERENCES arquivos(id),
    seq         INTEGER NOT NULL,
    namespace   TEXT NOT NULL,
    subkey      TEXT NOT NULL,
    full_key    TEXT NOT NULL,
    tipo        TEXT NOT NULL DEFAULT 'codigo',
    texto       TEXT,
    linha_ini   INTEGER,
    UNIQUE(arquivo_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_full_key ON entradas(full_key);
CREATE INDEX IF NOT EXISTS idx_tipo     ON entradas(tipo);
CREATE INDEX IF NOT EXISTS idx_arq_seq  ON entradas(arquivo_id, seq);
"""


def criar_db(pasta_cht):
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA)
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('pasta_cht', ?)", (str(pasta_cht),))
    conn.commit()
    return conn


# ═══════════════════════════════════════════════════════════════
# EXTRACAO
# ═══════════════════════════════════════════════════════════════

def extrair(pasta_cht):
    txts = sorted(pasta_cht.rglob("*.txt"))

    log.info("")
    log.info("=" * 62)
    log.info("  DB1 - EXTRACAO DE ESTRUTURA")
    log.info(f"  Fonte: CHT -> {pasta_cht}")
    log.info(f"  Arquivos  : {len(txts)}")
    log.info(f"  DB        : {DB_PATH}")
    log.info("")
    log.info("  Regra aplicada:")
    log.info("    texto CHT sem chines  ->  tipo='codigo'   (copia texto)")
    log.info("    texto CHT com chines  ->  tipo='dialogo'  (texto=NULL)")
    log.info("=" * 62)
    log.info("")

    conn = criar_db(pasta_cht)

    total_arqs   = 0
    total_codigo = 0
    total_diag   = 0

    for arq in txts:
        rel = str(arq.relative_to(pasta_cht)).replace("\\", "/")
        entradas = parse_arquivo(arq)

        if not entradas:
            log.info(f"  [VAZIO]  {rel}")
            continue

        n_cod = 0
        n_dlg = 0
        rows  = []

        for e in entradas:
            if tem_cjk(e["texto"]):
                # CHT tem chines = dialogo -> texto=NULL, so a chave fica marcada
                tipo  = "dialogo"
                texto = None
                n_dlg += 1
            else:
                # CHT sem chines = codigo puro do motor -> copia texto
                tipo  = "codigo"
                texto = e["texto"]
                n_cod += 1

            rows.append((
                e["seq"],
                e["namespace"],
                e["subkey"],
                e["full_key"],
                tipo,
                texto,
                e["linha_ini"],
            ))

        # Insere arquivo
        conn.execute(
            "INSERT OR REPLACE INTO arquivos (rel_path, qtd_codigo, qtd_dialogo) VALUES (?,?,?)",
            (rel, n_cod, n_dlg)
        )
        arq_id = conn.execute(
            "SELECT id FROM arquivos WHERE rel_path=?", (rel,)
        ).fetchone()[0]

        # Insere entradas
        conn.executemany(
            """INSERT OR REPLACE INTO entradas
               (arquivo_id, seq, namespace, subkey, full_key, tipo, texto, linha_ini)
               VALUES (?,?,?,?,?,?,?,?)""",
            [(arq_id, r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows]
        )
        conn.commit()

        total_arqs   += 1
        total_codigo += n_cod
        total_diag   += n_dlg

        log.info(f"  {rel:45s}  cod:{n_cod:4d}  dlg:{n_dlg:4d}")

    log.info("")
    log.info("=" * 62)
    log.info("  CONCLUIDO")
    log.info(f"  Arquivos             : {total_arqs}")
    log.info(f"  tipo=codigo          : {total_codigo}  <- codigo do motor (texto copiado)")
    log.info(f"  tipo=dialogo         : {total_diag}  <- marcadores para o DB2")
    log.info(f"  DB salvo             : {DB_PATH}")
    log.info("=" * 62)
    log.info("")

    conn.close()
    return total_arqs, total_codigo, total_diag


# ═══════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════

def mostrar_stats():
    if not DB_PATH.exists():
        print("  DB1 nao encontrado. Execute a extracao primeiro.")
        return
    conn = sqlite3.connect(str(DB_PATH))
    pasta = conn.execute("SELECT valor FROM meta WHERE chave='pasta_cht'").fetchone()
    arqs  = conn.execute("SELECT COUNT(*) FROM arquivos").fetchone()[0]
    cod   = conn.execute("SELECT COUNT(*) FROM entradas WHERE tipo='codigo'").fetchone()[0]
    dlg   = conn.execute("SELECT COUNT(*) FROM entradas WHERE tipo='dialogo'").fetchone()[0]
    total = cod + dlg

    print()
    print("  DB1 - Banco de Estrutura")
    print(f"  CHT fonte    : {pasta[0] if pasta else '?'}")
    print(f"  Arquivos     : {arqs}")
    print(f"  Total chaves : {total}")
    print(f"  tipo=codigo  : {cod:5d}  <- copiado do motor")
    print(f"  tipo=dialogo : {dlg:5d}  <- marcadores p/ DB2")
    print()
    print("  Top 5 arquivos com mais dialogos:")
    for r in conn.execute(
        "SELECT rel_path, qtd_dialogo, qtd_codigo FROM arquivos ORDER BY qtd_dialogo DESC LIMIT 5"
    ):
        print(f"    {r[0]:42s}  dlg:{r[1]:4d}  cod:{r[2]:4d}")
    print()
    print("  Amostra de entradas codigo:")
    for r in conn.execute(
        "SELECT full_key, texto FROM entradas WHERE tipo='codigo' AND texto!='' LIMIT 5"
    ):
        print(f"    {r[0]:35s}  {repr(r[1][:45])}")
    conn.close()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    pasta_cht = achar_pasta_cht()

    if not pasta_cht:
        print()
        print("  ERRO: Pasta CHT nao encontrada.")
        print(f"  Pastas disponiveis em: {HERE}")
        for nome, n in listar_pastas():
            print(f"    {nome}/  ({n} arquivos .txt)")
        print()
        print("  Renomeie a pasta com os arquivos em chines para 'CHT'.")
        sys.exit(1)

    extrair(pasta_cht)
    mostrar_stats()


if __name__ == "__main__":
    main()
