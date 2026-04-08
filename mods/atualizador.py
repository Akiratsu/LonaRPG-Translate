"""
LonaRPG DB1 - Atualizador
===========================
Atualiza o DB1 apos um patch do jogo.
Relê os arquivos CHT e detecta:
  NOVO      : chave nao existia -> insere
  ALTERADO  : tipo mudou OU texto codigo mudou -> atualiza + salva historico
  REMOVIDO  : chave sumiu do CHT -> marca como removido

Uso:
  python atualizador.py              <- aplica mudancas
  python atualizador.py --dry-run   <- so mostra, nao aplica
"""

import re
import sys
import sqlite3
import hashlib
import logging
from pathlib import Path
from datetime import datetime

HERE     = Path(__file__).resolve().parent
ROOT     = HERE.parent   # Pipeline\ — onde ficam CHT\, ENG\ etc.
DB_PATH  = HERE / "database" / "db1_estrutura.sqlite"
LOG_PATH = HERE / "db1_atualizador.log"

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

def md5(texto):
    return hashlib.md5((texto or "").encode("utf-8")).hexdigest()


MIGRACOES = [
    "ALTER TABLE entradas ADD COLUMN status TEXT DEFAULT 'ativo'",
    "ALTER TABLE entradas ADD COLUMN texto_hash TEXT",
    "ALTER TABLE entradas ADD COLUMN atualizado_em TEXT",
    "ALTER TABLE arquivos ADD COLUMN arquivo_hash TEXT",
    "ALTER TABLE arquivos ADD COLUMN atualizado_em TEXT",
]

SCHEMA_EXTRA = """
CREATE TABLE IF NOT EXISTS historico (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    data         TEXT DEFAULT (datetime('now')),
    arquivo      TEXT NOT NULL,
    full_key     TEXT NOT NULL,
    tipo_evento  TEXT NOT NULL,
    tipo_antes   TEXT,
    tipo_depois  TEXT,
    texto_antes  TEXT,
    texto_depois TEXT
);
CREATE TABLE IF NOT EXISTS resumos_update (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    data       TEXT DEFAULT (datetime('now')),
    novos      INTEGER DEFAULT 0,
    alterados  INTEGER DEFAULT 0,
    removidos  INTEGER DEFAULT 0,
    arqs_novos INTEGER DEFAULT 0
);
"""

def migrar(conn):
    for sql in MIGRACOES:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass
    conn.executescript(SCHEMA_EXTRA)
    # preenche hashes faltando
    rows = conn.execute("SELECT id, texto FROM entradas WHERE texto_hash IS NULL").fetchall()
    conn.executemany("UPDATE entradas SET texto_hash=? WHERE id=?",
                     [(md5(r[1]), r[0]) for r in rows])
    conn.commit()


def parse_arquivo(caminho):
    resultado = {}
    seq = 0
    cur_ns = cur_sk = cur_key = None
    cur_linhas = []
    cur_ini = 0
    try:
        linhas = caminho.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    except Exception as e:
        log.error(f"  [ERRO] {caminho}: {e}")
        return {}

    def flush():
        nonlocal seq
        if cur_key is None:
            return
        texto = "".join(cur_linhas).rstrip("\n\r ")
        resultado[cur_key] = {"seq": seq, "namespace": cur_ns, "subkey": cur_sk,
                               "full_key": cur_key, "texto": texto, "linha_ini": cur_ini}
        seq += 1

    for num, linha in enumerate(linhas, start=1):
        s = linha.rstrip("\n\r")
        if s.lstrip().startswith("#"):
            flush(); cur_ns = cur_sk = cur_key = None; cur_linhas = []; continue
        if s.strip() == "" and cur_key is None:
            continue
        m = KEY_RE.match(s)
        if m:
            flush()
            cur_ns = m.group(1); cur_sk = m.group(2)
            cur_key = f"{cur_ns}/{cur_sk}"; cur_linhas = []; cur_ini = num
            continue
        if cur_key is not None:
            cur_linhas.append(linha)
    flush()
    return resultado


def hash_arq(p):
    try:
        return hashlib.md5(p.read_bytes()).hexdigest()
    except:
        return None


def atualizar_arquivo(conn, arq, rel, dry_run, resumo):
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    h_arq = hash_arq(arq)

    row_arq = conn.execute(
        "SELECT id, arquivo_hash FROM arquivos WHERE rel_path=?", (rel,)
    ).fetchone()

    if row_arq and row_arq[1] == h_arq:
        return 0, 0, 0

    disco = parse_arquivo(arq)
    if not disco:
        return 0, 0, 0

    novos = alt = rem = 0

    # Arquivo totalmente novo
    if not row_arq:
        if not dry_run:
            n_cod = sum(1 for e in disco.values() if not tem_cjk(e["texto"]))
            n_dlg = len(disco) - n_cod
            conn.execute(
                "INSERT INTO arquivos (rel_path, qtd_codigo, qtd_dialogo, arquivo_hash, atualizado_em) VALUES (?,?,?,?,?)",
                (rel, n_cod, n_dlg, h_arq, agora)
            )
            arq_id = conn.execute("SELECT id FROM arquivos WHERE rel_path=?", (rel,)).fetchone()[0]
            for i, e in enumerate(disco.values()):
                tipo  = "dialogo" if tem_cjk(e["texto"]) else "codigo"
                texto = None if tipo == "dialogo" else e["texto"]
                conn.execute(
                    "INSERT INTO entradas (arquivo_id,seq,namespace,subkey,full_key,tipo,texto,texto_hash,status,atualizado_em,linha_ini) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (arq_id, i, e["namespace"], e["subkey"], e["full_key"], tipo, texto, md5(texto), "ativo", agora, e["linha_ini"])
                )
            conn.execute(
                "INSERT INTO historico (data,arquivo,full_key,tipo_evento) VALUES (?,?,?,?)",
                (agora, rel, f"[{len(disco)} entradas]", "ARQUIVO_NOVO")
            )
            conn.commit()
        log.info(f"  [ARQUIVO_NOVO] {rel}  ({len(disco)} entradas)")
        resumo["arqs_novos"] += 1
        return len(disco), 0, 0

    arq_id = row_arq[0]
    db_map = {r[0]: {"tipo": r[1], "texto": r[2], "hash": r[3], "id": r[4], "status": r[5]}
              for r in conn.execute(
                  "SELECT full_key, tipo, texto, texto_hash, id, status FROM entradas WHERE arquivo_id=?",
                  (arq_id,)
              )}

    chaves_disco = set(disco.keys())
    chaves_db    = set(db_map.keys())

    for key in sorted(chaves_disco - chaves_db):
        e     = disco[key]
        tipo  = "dialogo" if tem_cjk(e["texto"]) else "codigo"
        texto = None if tipo == "dialogo" else e["texto"]
        log.info(f"  [NOVO]     {rel:35s}  {key}  ({tipo})")
        if not dry_run:
            seq_n = conn.execute("SELECT COALESCE(MAX(seq),0)+1 FROM entradas WHERE arquivo_id=?", (arq_id,)).fetchone()[0]
            conn.execute(
                "INSERT INTO entradas (arquivo_id,seq,namespace,subkey,full_key,tipo,texto,texto_hash,status,atualizado_em,linha_ini) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (arq_id, seq_n, e["namespace"], e["subkey"], key, tipo, texto, md5(texto), "ativo", agora, e["linha_ini"])
            )
            conn.execute(
                "INSERT INTO historico (data,arquivo,full_key,tipo_evento,tipo_depois,texto_depois) VALUES (?,?,?,?,?,?)",
                (agora, rel, key, "NOVO", tipo, texto)
            )
        novos += 1

    for key in sorted(chaves_db - chaves_disco):
        if db_map[key]["status"] == "removido":
            continue
        log.info(f"  [REMOVIDO] {rel:35s}  {key}")
        if not dry_run:
            conn.execute("UPDATE entradas SET status='removido', atualizado_em=? WHERE id=?",
                         (agora, db_map[key]["id"]))
            conn.execute(
                "INSERT INTO historico (data,arquivo,full_key,tipo_evento,tipo_antes,texto_antes) VALUES (?,?,?,?,?,?)",
                (agora, rel, key, "REMOVIDO", db_map[key]["tipo"], db_map[key]["texto"])
            )
        rem += 1

    for key in sorted(chaves_disco & chaves_db):
        e      = disco[key]
        novo_tipo  = "dialogo" if tem_cjk(e["texto"]) else "codigo"
        novo_texto = None if novo_tipo == "dialogo" else e["texto"]
        novo_hash  = md5(novo_texto)

        tipo_ant  = db_map[key]["tipo"]
        hash_ant  = db_map[key]["hash"]

        if novo_tipo == tipo_ant and novo_hash == hash_ant:
            continue

        log.info(f"  [ALTERADO] {rel:35s}  {key}  ({tipo_ant} -> {novo_tipo})")
        if not dry_run:
            conn.execute(
                "UPDATE entradas SET tipo=?, texto=?, texto_hash=?, status='ativo', atualizado_em=? WHERE id=?",
                (novo_tipo, novo_texto, novo_hash, agora, db_map[key]["id"])
            )
            conn.execute(
                "INSERT INTO historico (data,arquivo,full_key,tipo_evento,tipo_antes,tipo_depois,texto_antes,texto_depois) VALUES (?,?,?,?,?,?,?,?)",
                (agora, rel, key, "ALTERADO", tipo_ant, novo_tipo, db_map[key]["texto"], novo_texto)
            )
        alt += 1

    if not dry_run and (novos or alt or rem):
        n_cod = conn.execute("SELECT COUNT(*) FROM entradas WHERE arquivo_id=? AND tipo='codigo' AND status='ativo'", (arq_id,)).fetchone()[0]
        n_dlg = conn.execute("SELECT COUNT(*) FROM entradas WHERE arquivo_id=? AND tipo='dialogo' AND status='ativo'", (arq_id,)).fetchone()[0]
        conn.execute("UPDATE arquivos SET arquivo_hash=?, qtd_codigo=?, qtd_dialogo=?, atualizado_em=? WHERE id=?",
                     (h_arq, n_cod, n_dlg, agora, arq_id))
        conn.commit()

    return novos, alt, rem


def atualizar(dry_run=False):
    if not DB_PATH.exists():
        print("  DB1 nao encontrado. Execute a extracao primeiro (opcao 1).")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    migrar(conn)

    pasta_cht = conn.execute("SELECT valor FROM meta WHERE chave='pasta_cht'").fetchone()
    if not pasta_cht or not Path(pasta_cht[0]).is_dir():
        print("  Pasta CHT nao encontrada no meta. Reextraia com opcao 1.")
        conn.close(); sys.exit(1)

    pasta_cht = Path(pasta_cht[0])
    txts = sorted(pasta_cht.rglob("*.txt"))
    modo = "SIMULACAO" if dry_run else "APLICANDO"

    log.info("")
    log.info("=" * 58)
    log.info(f"  DB1 - ATUALIZADOR  [{modo}]")
    log.info(f"  CHT : {pasta_cht}")
    log.info("=" * 58)
    log.info("")

    resumo = {"novos": 0, "alterados": 0, "removidos": 0, "arqs_novos": 0, "sem_mudanca": 0}

    for arq in txts:
        rel = str(arq.relative_to(pasta_cht)).replace("\\", "/")
        n, a, r = atualizar_arquivo(conn, arq, rel, dry_run, resumo)
        resumo["novos"]     += n
        resumo["alterados"] += a
        resumo["removidos"] += r
        if n == 0 and a == 0 and r == 0:
            resumo["sem_mudanca"] += 1

    rel_disco = {str(a.relative_to(pasta_cht)).replace("\\", "/") for a in txts}
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for row in conn.execute("SELECT rel_path FROM arquivos"):
        if row[0] not in rel_disco:
            log.info(f"  [ARQUIVO_REMOVIDO] {row[0]}")
            if not dry_run:
                conn.execute(
                    "UPDATE entradas SET status='removido', atualizado_em=? WHERE arquivo_id=(SELECT id FROM arquivos WHERE rel_path=?) AND status='ativo'",
                    (agora, row[0])
                )
                conn.commit()
            resumo["removidos"] += 1

    if not dry_run:
        conn.execute("INSERT INTO resumos_update (novos,alterados,removidos,arqs_novos) VALUES (?,?,?,?)",
                     (resumo["novos"], resumo["alterados"], resumo["removidos"], resumo["arqs_novos"]))
        conn.commit()
    conn.close()

    log.info("")
    log.info("=" * 58)
    log.info("  SIMULACAO CONCLUIDA" if dry_run else "  ATUALIZACAO CONCLUIDA")
    log.info(f"  Sem mudanca    : {resumo['sem_mudanca']} arquivos")
    log.info(f"  Arquivos novos : {resumo['arqs_novos']}")
    log.info(f"  Entradas novas : {resumo['novos']}")
    log.info(f"  Alteradas      : {resumo['alterados']}")
    log.info(f"  Removidas      : {resumo['removidos']}")
    log.info("=" * 58)
    log.info("")

    if all(v == 0 for k, v in resumo.items() if k != "sem_mudanca"):
        log.info("  Tudo em dia. Nenhuma mudanca detectada.")


if __name__ == "__main__":
    atualizar("--dry-run" in sys.argv)
