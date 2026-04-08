"""
LonaRPG DB2 - Extrator de Dialogos
=====================================
Le todas as chaves tipo='dialogo' do DB1 e busca o texto
em todos os idiomas disponiveis na pasta do script.

Resultado:
  DB2  -> todos os idiomas lado a lado (referencia completa)
  DB2b -> so ENG + PT-BR (fila limpa para a API GPT)

Idiomas suportados (pastas dentro de Pipeline/):
  ENG     <- fonte da verdade para traducao
  CHT     <- referencia (chines tradicional)
  KOR     <- referencia (coreano)
  RUS     <- referencia (russo)
  UKR     <- referencia (ucraniano)
  PT-BRC  <- traducao da comunidade (aproveitada se tags OK)
  PT-BR   <- traducao anterior se existir

Estrutura esperada:
  Pipeline/
    extrator_db2.py  <- este arquivo
    database/
      db1_estrutura.sqlite  <- criado pelo extrator.py
      db2_dialogos.sqlite   <- criado aqui
      db2b_fila.sqlite      <- criado aqui
"""

import re
import sys
import sqlite3
import logging
from pathlib import Path

HERE      = Path(__file__).resolve().parent
ROOT      = HERE.parent   # Pipeline\ — onde ficam CHT\, ENG\, UKR\ etc.
DB1_PATH  = HERE / "database" / "db1_estrutura.sqlite"
DB2_PATH  = HERE / "database" / "db2_dialogos.sqlite"
DB2B_PATH = HERE / "database" / "db2b_fila.sqlite"
LOG_PATH  = HERE / "db2_extrator.log"

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
TAG_RE = re.compile(r'\\[A-Za-z]+(?:\[.*?\])?')

# Idiomas a carregar e suas pastas possiveis
IDIOMAS = {
    "eng"   : ["ENG", "EN", "English"],
    "cht"   : ["CHT", "ZH", "ZH-TW"],
    "kor"   : ["KOR", "KO", "Korean"],
    "rus"   : ["RUS", "RU", "Russian"],
    "ukt"   : ["UKR", "UKT", "UK", "Ukrainian"],
    "ptbrc" : ["PT-BRC", "PTBRC", "PT-BR-C"],
    "pt"    : ["PT-BR", "PTBR"],   # nossa tradução final (gerador salva aqui)
}

# Labels de exibição no log (substitui cod.upper())
IDIOMA_LABEL = {
    "eng"  : "ENG",
    "cht"  : "CHT",
    "kor"  : "KOR",
    "rus"  : "RUS",
    "ukt"  : "UKR",
    "ptbrc": "PTBRC",
    "pt"   : "PT-BR",
}


# ═══════════════════════════════════════════════════════════════
# LOCALIZA PASTAS DE IDIOMA
# ═══════════════════════════════════════════════════════════════

def achar_pastas():
    """
    Retorna dict { codigo_idioma: Path } para cada idioma encontrado.
    Procura em ROOT (pai de mods) e em HERE (dentro de mods).
    Ex: { 'eng': Path(...), 'cht': Path(...), 'kor': Path(...) }
    """
    encontradas = {}
    BUSCA = [ROOT, HERE]   # procura fora de mods\ primeiro, depois dentro
    for cod, nomes in IDIOMAS.items():
        for base in BUSCA:
            for nome in nomes:
                p = base / nome
                if p.is_dir() and any(p.rglob("*.txt")):
                    encontradas[cod] = p
                    break
            if cod in encontradas:
                break
    return encontradas


# ═══════════════════════════════════════════════════════════════
# PARSER
# ═══════════════════════════════════════════════════════════════

def parse_arquivo(caminho):
    """
    Le um arquivo .txt do jogo.
    Retorna dict: { full_key -> texto }
    """
    resultado = {}
    cur_key   = None
    cur_linhas = []

    try:
        linhas = caminho.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    except Exception as e:
        log.error(f"  [ERRO leitura] {caminho}: {e}")
        return {}

    def flush():
        if cur_key is not None:
            resultado[cur_key] = "".join(cur_linhas).rstrip("\n\r ")

    for linha in linhas:
        s = linha.rstrip("\n\r")
        if s.lstrip().startswith("#"):
            flush(); cur_key = None; cur_linhas = []; continue
        if s.strip() == "" and cur_key is None:
            continue
        m = KEY_RE.match(s)
        if m:
            flush()
            cur_key    = f"{m.group(1)}/{m.group(2)}"
            cur_linhas = []
            continue
        if cur_key is not None:
            cur_linhas.append(linha)

    flush()
    return resultado


def indexar_idioma(pasta):
    """
    Le todos os .txt de uma pasta recursivamente.
    Retorna: { "rel/path.txt" -> { full_key -> texto } }
    """
    idx = {}
    for arq in sorted(pasta.rglob("*.txt")):
        rel = str(arq.relative_to(pasta)).replace("\\", "/")
        idx[rel] = parse_arquivo(arq)
    return idx


# ═══════════════════════════════════════════════════════════════
# VALIDACAO TAGS PT-BRC
# ═══════════════════════════════════════════════════════════════

def extrair_tags(texto):
    """Extrai lista ordenada de tags RPG do texto."""
    return TAG_RE.findall(texto or "")


def validar_ptbrc(texto_eng, texto_ptbrc):
    """
    Compara a estrutura de tags RPG entre ENG e PT-BRC.

    Retorna:
      'aproveitado'  -> tags identicas, texto PT-BRC pode ser usado
      'tags_erradas' -> tags diferentes ou na ordem errada
      'ausente'      -> chave nao existe no PT-BRC
      'orphao'       -> nao deve acontecer aqui (tratado antes)
    """
    if texto_ptbrc is None:
        return "ausente"

    # String vazia não é tradução — é ausente
    # (ocorre quando ENG e PT-BRC ambos estão vazios: [] == [] seria falso positivo)
    if texto_ptbrc.strip() == "":
        return "ausente"

    tags_eng  = extrair_tags(texto_eng or "")
    tags_ptbr = extrair_tags(texto_ptbrc)

    if tags_eng == tags_ptbr:
        return "aproveitado"
    else:
        return "tags_erradas"


# ═══════════════════════════════════════════════════════════════
# SCHEMA DB2
# ═══════════════════════════════════════════════════════════════

SCHEMA_DB2 = """
CREATE TABLE IF NOT EXISTS meta (
    chave TEXT PRIMARY KEY,
    valor TEXT
);

CREATE TABLE IF NOT EXISTS dialogos (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    arquivo       TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    namespace     TEXT NOT NULL,
    subkey        TEXT NOT NULL,
    full_key      TEXT NOT NULL,
    texto_eng     TEXT,
    texto_cht     TEXT,
    texto_kor     TEXT,
    texto_rus     TEXT,
    texto_ukt     TEXT,
    texto_ptbrc   TEXT,
    status_ptbrc  TEXT DEFAULT 'ausente',
    extraido_em   TEXT DEFAULT (datetime('now')),
    UNIQUE(arquivo, full_key)
);

CREATE INDEX IF NOT EXISTS idx_d2_arquivo  ON dialogos(arquivo);
CREATE INDEX IF NOT EXISTS idx_d2_full_key ON dialogos(full_key);
CREATE INDEX IF NOT EXISTS idx_d2_status   ON dialogos(status_ptbrc);
"""

SCHEMA_DB2B = """
CREATE TABLE IF NOT EXISTS meta (
    chave TEXT PRIMARY KEY,
    valor TEXT
);

CREATE TABLE IF NOT EXISTS fila (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    arquivo     TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    namespace   TEXT NOT NULL,
    subkey      TEXT NOT NULL,
    full_key    TEXT NOT NULL,
    texto_eng   TEXT,
    texto_cht   TEXT,
    texto_ptbr  TEXT,
    status      TEXT DEFAULT 'pendente',
    gerado_em   TEXT DEFAULT (datetime('now')),
    UNIQUE(arquivo, full_key)
);

CREATE INDEX IF NOT EXISTS idx_d2b_status   ON fila(status);
CREATE INDEX IF NOT EXISTS idx_d2b_arquivo  ON fila(arquivo);
CREATE INDEX IF NOT EXISTS idx_d2b_full_key ON fila(full_key);
"""


def _migrar_db2b(conn2b):
    """Adiciona colunas novas ao DB2b existente sem perder dados."""
    migracoes = [
        "ALTER TABLE fila ADD COLUMN texto_cht TEXT",
    ]
    for sql in migracoes:
        try:
            conn2b.execute(sql)
            conn2b.commit()
        except Exception:
            pass  # coluna já existe


def criar_dbs():
    DB2_PATH.parent.mkdir(parents=True, exist_ok=True)

    if DB2_PATH.exists():  DB2_PATH.unlink()
    if DB2B_PATH.exists(): DB2B_PATH.unlink()

    conn2  = sqlite3.connect(str(DB2_PATH))
    conn2b = sqlite3.connect(str(DB2B_PATH))

    conn2.executescript(SCHEMA_DB2)
    conn2b.executescript(SCHEMA_DB2B)
    _migrar_db2b(conn2b)  # garante colunas novas mesmo em banco recém-criado

    return conn2, conn2b


# ═══════════════════════════════════════════════════════════════
# EXTRACAO PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def extrair():
    # ── verifica DB1 ──────────────────────────────────────────
    if not DB1_PATH.exists():
        log.error("  ERRO: DB1 nao encontrado.")
        log.error(f"  Esperado em: {DB1_PATH}")
        log.error("  Execute o extrator.py primeiro (opcao 1 no MENU).")
        sys.exit(1)

    # ── localiza pastas de idioma ─────────────────────────────
    pastas = achar_pastas()

    log.info("")
    log.info("=" * 62)
    log.info("  DB2 - EXTRACAO DE DIALOGOS")
    log.info(f"  DB1 fonte : {DB1_PATH}")
    log.info("")
    log.info("  Idiomas encontrados:")
    for cod, p in pastas.items():
        txts = len(list(p.rglob("*.txt")))
        log.info(f"    {IDIOMA_LABEL.get(cod,cod.upper()):8s}  {str(p):35s}  ({txts} arquivos)")

    idiomas_ausentes = [IDIOMA_LABEL.get(cod,cod.upper()) for cod in IDIOMAS if cod not in pastas]
    if idiomas_ausentes:
        log.info("")
        log.info(f"  Idiomas ausentes (colunas ficam NULL): {', '.join(idiomas_ausentes)}")

    if "eng" not in pastas:
        log.error("")
        log.error("  ERRO: Pasta ENG nao encontrada. ENG e obrigatorio.")
        sys.exit(1)

    log.info("=" * 62)
    log.info("")

    # ── indexa todos os idiomas na memoria ───────────────────
    log.info("  Carregando idiomas na memoria...")
    indices = {}
    for cod, pasta in pastas.items():
        indices[cod] = indexar_idioma(pasta)
        total_chaves = sum(len(v) for v in indices[cod].values())
        log.info(f"    {IDIOMA_LABEL.get(cod,cod.upper()):8s}  {len(indices[cod]):4d} arquivos  {total_chaves:6d} chaves")

    log.info("")

    # ── abre DB1 e cria DB2/DB2b ─────────────────────────────
    db1   = sqlite3.connect(str(DB1_PATH))
    conn2, conn2b = criar_dbs()

    pasta_cht = db1.execute("SELECT valor FROM meta WHERE chave='pasta_cht'").fetchone()
    conn2.execute("INSERT OR REPLACE INTO meta VALUES ('pasta_cht', ?)",
                  (pasta_cht[0] if pasta_cht else "",))
    conn2.execute("INSERT OR REPLACE INTO meta VALUES ('idiomas', ?)",
                  (",".join(pastas.keys()),))
    conn2b.execute("INSERT OR REPLACE INTO meta VALUES ('gerado_de', ?)", (str(DB2_PATH),))
    conn2.commit(); conn2b.commit()

    # ── processa arquivo por arquivo ─────────────────────────
    arquivos = db1.execute(
        "SELECT id, rel_path FROM arquivos WHERE qtd_dialogo > 0 ORDER BY rel_path"
    ).fetchall()

    log.info(f"  Processando {len(arquivos)} arquivos com dialogos...")
    log.info("")

    stats = {
        "total_dlg"    : 0,
        "aproveitado"  : 0,
        "tags_erradas" : 0,
        "ausente"      : 0,
        "orphao_ptbrc" : 0,
    }

    for arq_id, rel_path in arquivos:
        # Busca chaves dialogo deste arquivo no DB1
        chaves = db1.execute("""
            SELECT seq, namespace, subkey, full_key
            FROM entradas
            WHERE arquivo_id=? AND tipo='dialogo'
            ORDER BY seq
        """, (arq_id,)).fetchall()

        if not chaves:
            continue

        n_aprov = n_tags = n_ausen = 0
        rows_db2  = []
        rows_db2b = []

        for seq, ns, sk, full_key in chaves:
            # Busca texto em cada idioma
            def get(cod):
                arq_idx = indices.get(cod, {})
                arq_map = arq_idx.get(rel_path, {})
                return arq_map.get(full_key)

            t_eng   = get("eng")
            t_cht   = get("cht")
            t_kor   = get("kor")
            t_rus   = get("rus")
            t_ukt   = get("ukt")
            t_ptbrc = get("ptbrc")
            t_pt    = get("pt")    # nossa tradução final (PT-BR\)

            # PT-BR final tem prioridade máxima — aproveitado direto sem validar tags
            # (já foi traduzido e revisado por nós; não precisa revalidar)
            if t_pt:
                status = "aproveitado"
                t_ptbrc = t_pt   # usa PT-BR como o ptbrc a ser copiado para DB2b
                stats["aproveitado"] += 1
                n_aprov += 1
            # Sem PT-BR final: lógica original
            elif t_eng is None:
                if t_cht is not None:
                    # Sem ENG mas tem CHT — entra na fila como pendente CHT-only
                    status = "pendente_cht"
                    stats["orphao_ptbrc"] += 1
                else:
                    # Sem ENG e sem CHT — orphao real, ignora
                    status = "orphao"
                    stats["orphao_ptbrc"] += 1
            else:
                status = validar_ptbrc(t_eng, t_ptbrc)
                stats[status] += 1

            # n_aprov já incrementado acima se t_pt — aqui só conta aproveitado via ptbrc
            if status == "aproveitado" and not t_pt:
                n_aprov += 1
            n_tags  += (status == "tags_erradas")
            n_ausen += (status == "ausente")

            rows_db2.append((
                rel_path, seq, ns, sk, full_key,
                t_eng, t_cht, t_kor, t_rus, t_ukt, t_ptbrc,
                status if status != "pendente_cht" else "orphao"  # DB2 mantém orphao
            ))

            # DB2b: aproveitados, pendentes normais E pendentes CHT-only
            if status == "aproveitado":
                texto_ptbr_inicial = t_ptbrc
                status_fila        = "aproveitado"
            elif status in ("ausente", "tags_erradas", "pendente_cht"):
                texto_ptbr_inicial = None
                status_fila        = "pendente"
            else:
                # orphao real (sem ENG e sem CHT): nao entra na fila
                continue

            rows_db2b.append((
                rel_path, seq, ns, sk, full_key,
                t_eng, t_cht, texto_ptbr_inicial,
                status_fila
            ))

        # Insere em lote
        conn2.executemany("""
            INSERT OR REPLACE INTO dialogos
              (arquivo, seq, namespace, subkey, full_key,
               texto_eng, texto_cht, texto_kor, texto_rus, texto_ukt,
               texto_ptbrc, status_ptbrc)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows_db2)

        conn2b.executemany("""
            INSERT OR REPLACE INTO fila
              (arquivo, seq, namespace, subkey, full_key,
               texto_eng, texto_cht, texto_ptbr, status)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, rows_db2b)

        conn2.commit(); conn2b.commit()

        stats["total_dlg"] += len(chaves)
        log.info(
            f"  {rel_path:45s}"
            f"  tot:{len(chaves):4d}"
            f"  apv:{n_aprov:3d}"
            f"  tag:{n_tags:3d}"
            f"  aus:{n_ausen:3d}"
        )

    # ── chaves orphao do PT-BRC (existem no ptbrc mas nao no DB1) ──
    if "ptbrc" in indices:
        db1_chaves = set(
            r[0] for r in db1.execute("SELECT full_key FROM entradas WHERE tipo='dialogo'")
        )
        orphaos = 0
        for rel, mapa in indices["ptbrc"].items():
            for key in mapa:
                if key not in db1_chaves:
                    orphaos += 1
        if orphaos:
            log.info("")
            log.info(f"  Chaves orphao no PT-BRC (nao existem no DB1): {orphaos}")
            log.info("  (ignoradas - nao entram no DB2)")

    db1.close()
    conn2.close()
    conn2b.close()

    return stats


# ═══════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════

def mostrar_stats():
    print()
    # DB2
    if DB2_PATH.exists():
        c2 = sqlite3.connect(str(DB2_PATH))
        total = c2.execute("SELECT COUNT(*) FROM dialogos").fetchone()[0]
        apv   = c2.execute("SELECT COUNT(*) FROM dialogos WHERE status_ptbrc='aproveitado'").fetchone()[0]
        tag   = c2.execute("SELECT COUNT(*) FROM dialogos WHERE status_ptbrc='tags_erradas'").fetchone()[0]
        aus   = c2.execute("SELECT COUNT(*) FROM dialogos WHERE status_ptbrc='ausente'").fetchone()[0]
        orp   = c2.execute("SELECT COUNT(*) FROM dialogos WHERE status_ptbrc='orphao'").fetchone()[0]
        arqs  = c2.execute("SELECT COUNT(DISTINCT arquivo) FROM dialogos").fetchone()[0]
        c2.close()

        print("  DB2 - Banco de Dialogos (referencia)")
        print(f"  Arquivos          : {arqs}")
        print(f"  Total dialogos    : {total}")
        print(f"  PT-BRC aproveit.  : {apv:5d}  ({apv*100//total if total else 0}%)")
        print(f"  PT-BRC tags_errad.: {tag:5d}  ({tag*100//total if total else 0}%)")
        print(f"  PT-BRC ausente    : {aus:5d}  ({aus*100//total if total else 0}%)")
        print(f"  PT-BRC orphao     : {orp:5d}")
        print()

    # DB2b
    if DB2B_PATH.exists():
        c2b = sqlite3.connect(str(DB2B_PATH))
        total_f  = c2b.execute("SELECT COUNT(*) FROM fila").fetchone()[0]
        pend     = c2b.execute("SELECT COUNT(*) FROM fila WHERE status='pendente'").fetchone()[0]
        aprov    = c2b.execute("SELECT COUNT(*) FROM fila WHERE status='aproveitado'").fetchone()[0]
        c2b.close()

        print("  DB2b - Fila para API GPT")
        print(f"  Total na fila     : {total_f}")
        print(f"  Pendentes (API)   : {pend:5d}  <- GPT vai traduzir")
        print(f"  Aproveitados      : {aprov:5d}  <- PT-BRC reaproveitado")
        print()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    stats = extrair()

    log.info("")
    log.info("=" * 62)
    log.info("  CONCLUIDO")
    log.info(f"  Total dialogos     : {stats['total_dlg']}")
    log.info(f"  PT-BRC aproveit.   : {stats['aproveitado']:5d}  <- copiado para DB2b")
    log.info(f"  PT-BRC tags_errad. : {stats['tags_erradas']:5d}  <- pendente API")
    log.info(f"  PT-BRC ausente     : {stats['ausente']:5d}  <- pendente API")
    log.info(f"  PT-BRC orphao      : {stats['orphao_ptbrc']:5d}  <- ignorado")
    log.info(f"  DB2  salvo em      : {DB2_PATH}")
    log.info(f"  DB2b salvo em      : {DB2B_PATH}")
    log.info("=" * 62)
    log.info("")

    mostrar_stats()


if __name__ == "__main__":
    main()