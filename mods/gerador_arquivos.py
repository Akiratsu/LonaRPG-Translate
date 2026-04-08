"""
LonaRPG - Gerador de Arquivos Finais PT-BR
==========================================
Lê DB1 (estrutura) + DB3 (tradução) e reconstrói os .txt
no formato exato do jogo, salvando em PT-BRC\\

Lógica por tipo de entrada:
  tipo='codigo'  -> copia o texto original do DB1 (igual em todos os idiomas)
  tipo='dialogo' -> usa texto_ptbr do DB3 (traduzido/aproveitado)
                    se não encontrar, usa fallback do ENG

Saída: PT-BRC\\ (mesma estrutura de subpastas do CHT)

Uso:
  python gerador_arquivos.py                    (gera todos os arquivos)
  python gerador_arquivos.py --arquivo menu.txt (gera só um arquivo)
  python gerador_arquivos.py --dry-run          (simula sem gravar)
  python gerador_arquivos.py --relatorio        (só mostra estatísticas)
"""

import sys
import sqlite3
import logging
import argparse
from pathlib import Path

HERE     = Path(__file__).resolve().parent
DB1_PATH = HERE / "database" / "db1_estrutura.sqlite"
DB2_PATH = HERE / "database" / "db2_dialogos.sqlite"
DB3_PATH = HERE / "database" / "db3_traducao.sqlite"
ROOT     = HERE.parent      # Pipeline\
SAIDA    = ROOT / "PT-BR"   # pasta de saída — fora de mods\
LOG_PATH = HERE / "db_gerador.log"

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


# ═══════════════════════════════════════════════════════════════
# VERIFICAÇÕES
# ═══════════════════════════════════════════════════════════════

def verificar_bancos():
    ok = True
    if not DB1_PATH.exists():
        log.error(f"  [ERRO] DB1 não encontrado: {DB1_PATH}")
        log.error("         Execute a opção [1] primeiro.")
        ok = False
    if not DB3_PATH.exists():
        log.error(f"  [ERRO] DB3 não encontrado: {DB3_PATH}")
        log.error("         Execute a opção [4] primeiro.")
        ok = False
    return ok


# ═══════════════════════════════════════════════════════════════
# CARREGAMENTO DOS BANCOS
# ═══════════════════════════════════════════════════════════════

def carregar_db1(filtro_arquivo=None):
    """
    Retorna dict: rel_path -> lista de entradas ordenada por seq
    Cada entrada: (seq, full_key, tipo, texto, namespace, subkey)
    """
    conn = sqlite3.connect(str(DB1_PATH))
    conn.row_factory = sqlite3.Row

    if filtro_arquivo:
        rows = conn.execute(
            """
            SELECT a.rel_path, e.seq, e.full_key, e.tipo, e.texto, e.namespace, e.subkey
            FROM entradas e
            JOIN arquivos a ON a.id = e.arquivo_id
            WHERE a.rel_path = ?
            ORDER BY a.rel_path, e.seq
            """,
            (filtro_arquivo,)
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT a.rel_path, e.seq, e.full_key, e.tipo, e.texto, e.namespace, e.subkey
            FROM entradas e
            JOIN arquivos a ON a.id = e.arquivo_id
            ORDER BY a.rel_path, e.seq
            """
        ).fetchall()

    conn.close()

    db1 = {}
    for r in rows:
        arq = r["rel_path"]
        if arq not in db1:
            db1[arq] = []
        db1[arq].append({
            "seq"     : r["seq"],
            "full_key": r["full_key"],
            "tipo"    : r["tipo"],
            "texto"   : r["texto"],
            "namespace": r["namespace"],
            "subkey"  : r["subkey"],
        })

    return db1


def carregar_db3(filtro_arquivo=None):
    """
    Retorna dict: (arquivo, full_key) -> {texto_ptbr, texto_eng, texto_ptbrc, status, tags_ok}
    Inclui texto_ptbrc do DB2 como fallback para entradas com texto_eng e texto_ptbr vazios.
    """
    conn = sqlite3.connect(str(DB3_PATH))
    conn.row_factory = sqlite3.Row

    if filtro_arquivo:
        rows = conn.execute(
            "SELECT arquivo, full_key, texto_ptbr, texto_eng, status, tags_ok "
            "FROM traducoes WHERE arquivo = ?",
            (filtro_arquivo,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT arquivo, full_key, texto_ptbr, texto_eng, status, tags_ok FROM traducoes"
        ).fetchall()

    conn.close()

    db3 = {}
    for r in rows:
        chave = (r["arquivo"], r["full_key"])
        db3[chave] = {
            "texto_ptbr" : r["texto_ptbr"],
            "texto_eng"  : r["texto_eng"],
            "texto_ptbrc": None,   # preenchido abaixo se DB2 existir
            "status"     : r["status"],
            "tags_ok"    : r["tags_ok"],
        }

    # Se DB2 existir, busca texto_ptbrc como fallback adicional
    # (útil para entradas com texto_eng vazio mas tradução PT-BRC disponível)
    if DB2_PATH.exists():
        try:
            c2 = sqlite3.connect(str(DB2_PATH))
            if filtro_arquivo:
                ptbrc_rows = c2.execute(
                    "SELECT arquivo, full_key, texto_ptbrc FROM dialogos WHERE arquivo=?",
                    (filtro_arquivo,)
                ).fetchall()
            else:
                ptbrc_rows = c2.execute(
                    "SELECT arquivo, full_key, texto_ptbrc FROM dialogos"
                ).fetchall()
            c2.close()
            for arq, fk, ptbrc in ptbrc_rows:
                k = (arq, fk)
                if k in db3 and ptbrc:
                    db3[k]["texto_ptbrc"] = ptbrc
        except Exception:
            pass

    return db3


# ═══════════════════════════════════════════════════════════════
# GERAÇÃO DE UM ARQUIVO
# ═══════════════════════════════════════════════════════════════

def gerar_arquivo(rel_path, entradas, db3, dry_run=False):
    """
    Gera o conteúdo final de um .txt e salva em PT-BRC\\.
    Retorna estatísticas: (total, ok_ptbr, fallback_eng, sem_traducao)
    """
    linhas = []
    total = ok_ptbr = fallback_eng = sem_traducao = 0

    prev_namespace = None

    for e in entradas:
        total += 1
        ns  = e["namespace"]
        sk  = e["subkey"]
        fk  = e["full_key"]
        tipo = e["tipo"]

        # Separador de namespace (melhora legibilidade)
        if ns != prev_namespace:
            if linhas:  # não adiciona no início
                linhas.append("")
            prev_namespace = ns

        if tipo == "codigo":
            # Código do motor: copia exato
            texto = e["texto"] or ""
            linhas.append(fk)
            linhas.append(texto)
            linhas.append("")
            ok_ptbr += 1

        else:  # tipo == 'dialogo'
            # Tenta DB3 (arquivo exato, depois qualquer arquivo com essa chave)
            entrada_db3 = db3.get((rel_path, fk))

            # Fallback: busca em outros arquivos (chave pode aparecer em múltiplos)
            if entrada_db3 is None:
                for (arq, k), v in db3.items():
                    if k == fk:
                        entrada_db3 = v
                        break

            if entrada_db3 and entrada_db3["texto_ptbr"]:
                # Tradução PT-BR disponível
                texto = entrada_db3["texto_ptbr"]
                ok_ptbr += 1
            elif entrada_db3 and entrada_db3.get("texto_ptbrc"):
                # Fallback: PT-BRC da comunidade (ocorre quando texto_eng era vazio
                # na extração mas a comunidade tinha tradução — chaves "orphao")
                texto = entrada_db3["texto_ptbrc"]
                ok_ptbr += 1
                log.debug(f"  [ptbrc fallback] {fk}")
            elif entrada_db3 and entrada_db3["texto_eng"]:
                # Fallback para ENG (mantém jogável sem PT-BR)
                texto = entrada_db3["texto_eng"]
                fallback_eng += 1
            else:
                # Sem conteúdo em nenhum idioma (entrada vazia no jogo)
                texto = ""
                sem_traducao += 1

            linhas.append(fk)
            if texto:
                linhas.append(texto)
            linhas.append("")

    conteudo = "\n".join(linhas)
    if not conteudo.endswith("\n"):
        conteudo += "\n"

    if not dry_run:
        destino = SAIDA / rel_path
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text(conteudo, encoding="utf-8")

    return total, ok_ptbr, fallback_eng, sem_traducao


# ═══════════════════════════════════════════════════════════════
# RELATÓRIO
# ═══════════════════════════════════════════════════════════════

def mostrar_relatorio(db1, db3):
    log.info("")
    log.info("  ─────────────────────────────────────────────────")
    log.info("  RELATÓRIO PRÉ-GERAÇÃO")
    log.info("  ─────────────────────────────────────────────────")

    total_arqs     = len(db1)
    total_entradas = sum(len(v) for v in db1.values())
    total_codigo   = sum(1 for v in db1.values() for e in v if e["tipo"] == "codigo")
    total_dialogo  = sum(1 for v in db1.values() for e in v if e["tipo"] == "dialogo")

    # DB3 stats
    traduzidos  = sum(1 for v in db3.values() if v["texto_ptbr"] and v["status"] != "erro")
    erros       = sum(1 for v in db3.values() if v["status"] == "erro" or not v["texto_ptbr"])
    tags_ruins  = sum(1 for v in db3.values() if not v["tags_ok"] and v["texto_ptbr"])

    log.info(f"  DB1  arquivos     : {total_arqs}")
    log.info(f"  DB1  entradas     : {total_entradas}")
    log.info(f"       codigo       : {total_codigo}  (copiados direto)")
    log.info(f"       dialogo      : {total_dialogo}  (precisam de tradução)")
    log.info(f"")
    log.info(f"  DB3  com PT-BR    : {traduzidos}")
    log.info(f"  DB3  sem tradução : {erros}  (vão usar ENG ou ficam vazios)")
    log.info(f"  DB3  tags ruins   : {tags_ruins}  (tradução existe mas tags alteradas)")

    cobertura = traduzidos * 100 / max(total_dialogo, 1)
    log.info(f"")
    log.info(f"  Cobertura PT-BR   : {cobertura:.1f}%")
    log.info("  ─────────────────────────────────────────────────")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Gera arquivos PT-BR finais")
    parser.add_argument("--arquivo",   help="Gera só um arquivo específico (ex: menu.txt)")
    parser.add_argument("--dry-run",   action="store_true", help="Simula sem gravar")
    parser.add_argument("--relatorio", action="store_true", help="Só mostra estatísticas")
    args = parser.parse_args()

    log.info("")
    log.info("  =====================================================")
    log.info("   LonaRPG - Gerador de Arquivos Finais PT-BR")
    log.info("  =====================================================")

    if not verificar_bancos():
        sys.exit(1)

    log.info("")
    log.info("  Carregando DB1 (estrutura)...")
    db1 = carregar_db1(args.arquivo)

    log.info("  Carregando DB3 (traduções)...")
    db3 = carregar_db3(args.arquivo)

    if not db1:
        if args.arquivo:
            log.error(f"  [ERRO] Arquivo '{args.arquivo}' não encontrado no DB1.")
        else:
            log.error("  [ERRO] DB1 está vazio.")
        sys.exit(1)

    mostrar_relatorio(db1, db3)

    if args.relatorio:
        return

    if args.dry_run:
        log.info("")
        log.info("  [DRY-RUN] Simulando geração sem gravar arquivos...")
    else:
        log.info("")
        if SAIDA.exists():
            arqs_existentes = len(list(SAIDA.rglob("*.txt")))
            log.info(f"  Destino: {SAIDA}")
            log.info(f"  Pasta já existe ({arqs_existentes} arquivo(s)) — arquivos serão atualizados.")
        else:
            SAIDA.mkdir(parents=True, exist_ok=True)
            log.info(f"  Destino: {SAIDA}  (pasta criada)")
        log.info("  NOTA: PT-BRC\\ não será tocada (fonte do patcher anterior)")

    # ── Geração ──────────────────────────────────────────────
    total_arqs = 0
    total_ent  = 0
    total_ok   = 0
    total_fb   = 0
    total_sem  = 0
    arquivos_incompletos = []

    arquivos_lista = sorted(db1.keys())
    n = len(arquivos_lista)

    log.info("")
    for i, rel_path in enumerate(arquivos_lista, 1):
        entradas = db1[rel_path]
        t, ok, fb, sem = gerar_arquivo(rel_path, entradas, db3, dry_run=args.dry_run)

        total_arqs += 1
        total_ent  += t
        total_ok   += ok
        total_fb   += fb
        total_sem  += sem

        pct     = i * 100 // n
        status  = "OK" if sem == 0 else f"! {sem} sem tradução"
        log.info(f"  [{pct:3d}%] {rel_path:<40} {status}")

        if sem > 0:
            arquivos_incompletos.append((rel_path, sem))

    # ── Resumo ───────────────────────────────────────────────
    log.info("")
    log.info("  ─────────────────────────────────────────────────")
    log.info("  RESUMO")
    log.info("  ─────────────────────────────────────────────────")
    log.info(f"  Arquivos gerados      : {total_arqs}")
    log.info(f"  Entradas totais       : {total_ent}")
    log.info(f"  Com tradução PT-BR    : {total_ok}")
    log.info(f"  Fallback para ENG     : {total_fb}  (sem bloqueio, ENG usado)")
    log.info(f"  Sem tradução (vazio)  : {total_sem}")

    if arquivos_incompletos:
        log.info("")
        log.info("  Arquivos com entradas sem tradução:")
        for arq, sem in sorted(arquivos_incompletos, key=lambda x: -x[1]):
            log.info(f"    {arq:<40} {sem} entradas")

    if args.dry_run:
        log.info("")
        log.info("  [DRY-RUN] Nenhum arquivo foi gravado.")
    else:
        log.info("")
        log.info(f"  Arquivos salvos em: {SAIDA}")
        log.info("  =====================================================")
        log.info("  Concluído.")

    log.info("")


if __name__ == "__main__":
    main()
