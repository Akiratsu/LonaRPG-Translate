"""
tradutor_google.py  —  LonaRPG Translator
==========================================
Tradutor gratuito via deep_translator (Google Translate).
Sem necessidade de API key.

Uso:
  python tradutor_google.py                  traduz todos os pendentes
  python tradutor_google.py --teste 5        traduz só 5 entradas
  python tradutor_google.py --reenviar-erros reenvia entradas com erro
  python tradutor_google.py --lote 30        tamanho do lote (default: 30)
  python tradutor_google.py --fonte cht      usa CHT como fonte (default: eng)
"""

import re
import sys
import time
import sqlite3
import argparse
from pathlib import Path

HERE      = Path(__file__).resolve().parent
DB2_PATH  = HERE / "database" / "db2_dialogos.sqlite"
DB2B_PATH = HERE / "database" / "db2b_fila.sqlite"
DB3_PATH  = HERE / "database" / "db3_traducao.sqlite"

from tradutor_core import (
    log,
    carregar_config,
    mostrar_stats,
    shield,
    restore,
    validar_tags,
    _criar_db3,
    _abrir_db3,
    _popular_aproveitados,
    _popular_pendentes,
    _db_ok,
    _db_erro,
)

_MODELO   = "google-translate"
_LOTE_PAD = 30
_PAUSA    = 1.5


def _checar_instalacao():
    try:
        from deep_translator import GoogleTranslator
        return GoogleTranslator
    except ImportError:
        log.error("")
        log.error("  ERRO: deep_translator não instalado.")
        log.error("  Execute: pip install deep-translator")
        log.error("")
        sys.exit(1)


def _traduzir_lote(textos: list, fonte: str = "en") -> list:
    """Traduz um lote via Google Translate. Retorna lista do mesmo tamanho."""
    GoogleTranslator = _checar_instalacao()

    mapa = {
        "eng": "en", "en": "en",
        "cht": "zh-TW", "zh-tw": "zh-TW",
        "kor": "ko", "rus": "ru", "ukr": "uk",
    }
    src = mapa.get(fonte.lower(), "en")

    for tentativa in range(1, 4):
        try:
            tr = GoogleTranslator(source=src, target="pt")
            resultado = tr.translate_batch(textos)
            return [r if r else textos[i] for i, r in enumerate(resultado)]
        except Exception as e:
            log.warning(f"    [Google tentativa {tentativa}] {e}")
            if tentativa < 3:
                time.sleep(3 * tentativa)

    return [None] * len(textos)


def _loop_google(conn3, pendentes: list, lote_sz: int, fonte: str, atualiza: bool = True):
    """Loop de tradução usando Google Translate."""
    total      = len(pendentes)
    traduzidos = 0
    erros      = 0

    for i in range(0, total, lote_sz):
        lote = pendentes[i:i + lote_sz]
        pct  = min(i + lote_sz, total) * 100 // total

        # Escolhe texto fonte
        textos_orig = []
        for row in lote:
            arq, seq, ns, sk, fk, eng, cht = row
            if fonte == "cht" and cht:
                textos_orig.append(cht)
            else:
                textos_orig.append(eng or "")

        # Tag Shield
        textos_sh, mapas = [], []
        for txt in textos_orig:
            sh, mp = shield(txt)
            textos_sh.append(sh)
            mapas.append(mp)

        log.info(f"  [{pct:3d}%] lote {i//lote_sz + 1}  ({lote[0][0]}...)")

        resultados = _traduzir_lote(textos_sh, fonte=fonte if fonte != "auto" else "en")

        for j, (row, resultado) in enumerate(zip(lote, resultados)):
            arq, seq, ns, sk, fk, eng, cht = row

            if resultado:
                texto_final = restore(resultado, mapas[j])
                tags_ok     = 1 if validar_tags(textos_orig[j], texto_final) else 0
                # Usa INSERT OR REPLACE para garantir que salva mesmo se entrada não existe no DB3
                conn3.execute("""
                    INSERT OR REPLACE INTO traducoes
                      (arquivo, seq, namespace, subkey, full_key,
                       texto_eng, texto_ptbr, status, tags_ok,
                       modelo_usado, tentativas, traduzido_em)
                    VALUES (?,?,?,?,?,?,?,'traduzido',?,?,1,datetime('now'))
                """, (arq, seq, ns, sk, fk, eng, texto_final, tags_ok, _MODELO))
                traduzidos += 1
                if not tags_ok:
                    log.warning(f"    [tags_bad] {arq} / {fk}")
            else:
                # Marca como erro — INSERT OR REPLACE também
                conn3.execute("""
                    INSERT OR REPLACE INTO traducoes
                      (arquivo, seq, namespace, subkey, full_key,
                       texto_eng, status, tags_ok, modelo_usado, tentativas, traduzido_em)
                    VALUES (?,?,?,?,?,?,'erro',0,?,1,datetime('now'))
                """, (arq, seq, ns, sk, fk, eng, _MODELO))
                erros += 1

        conn3.commit()

        if i + lote_sz < total:
            time.sleep(_PAUSA)

    return traduzidos, erros


def traduzir_google(lote_sz: int = _LOTE_PAD, limite: int = None, fonte: str = "auto"):
    if not DB2B_PATH.exists():
        log.error("  ERRO: DB2b não encontrado. Rode extrator_db2.py primeiro.")
        sys.exit(1)

    if DB3_PATH.exists():
        conn3    = _abrir_db3()
        atualiza = True
        log.info("  Modo incremental: DB3 existente, continuando...")
    else:
        conn2b   = sqlite3.connect(str(DB2B_PATH))
        conn3    = _criar_db3()
        conn3.execute("INSERT OR REPLACE INTO meta VALUES ('modelo', ?)", (_MODELO,))
        conn3.commit()
        n_aprov  = _popular_aproveitados(conn3, conn2b)
        n_pend   = _popular_pendentes(conn3, conn2b)
        conn2b.close()
        # Usa atualiza=True mesmo no primeiro run — assim o UPDATE preserva status_revisao
        atualiza = True
        log.info(f"  DB3 criado. Aproveitados: {n_aprov}  Pendentes: {n_pend}")

    conn3.execute("INSERT OR REPLACE INTO meta VALUES ('modelo', ?)", (_MODELO,))
    conn3.commit()

    # Busca pendentes com texto_cht do DB2
    if DB2_PATH.exists():
        conn3.execute(f"ATTACH DATABASE '{str(DB2_PATH).replace(chr(92),'/')}' AS db2")
        q = (
            "SELECT t.arquivo, t.seq, t.namespace, t.subkey, t.full_key, "
            "t.texto_eng, d.texto_cht "
            "FROM traducoes t "
            "LEFT JOIN db2.dialogos d "
            "  ON d.arquivo=t.arquivo AND d.full_key=t.full_key "
            "WHERE t.status='pendente'"
        )
        if limite:
            q += f" ORDER BY t.id LIMIT {limite}"
        pendentes = conn3.execute(q).fetchall()
        conn3.execute("DETACH DATABASE db2")
    else:
        q = ("SELECT arquivo, seq, namespace, subkey, full_key, texto_eng, NULL "
             "FROM traducoes WHERE status='pendente'")
        if limite:
            q += f" ORDER BY id LIMIT {limite}"
        pendentes = conn3.execute(q).fetchall()

    total = len(pendentes)
    fonte_label = {"eng":"INGLÊS (ENG)","cht":"CHINÊS (CHT)","kor":"COREANO (KOR)",
                    "rus":"RUSSO (RUS)","ukt":"UCRANIANO (UKT)","auto":"AUTO"}.get(fonte.lower(), fonte.upper())
    log.info(f"  Pendentes : {total}")
    log.info(f"  Lote      : {lote_sz}")
    log.info(f"  Traduzindo: {fonte_label} → PT-BR")
    log.info(f"  Pausa     : {_PAUSA}s entre lotes")
    log.info("")

    if total == 0:
        log.info("  Nada a traduzir.")
        conn3.close()
        return 0, 0

    traduzidos, erros = _loop_google(conn3, pendentes, lote_sz, fonte, atualiza)
    conn3.close()
    return traduzidos, erros


def reenviar_erros_google(lote_sz: int = _LOTE_PAD, fonte: str = "auto", ids: str = None):
    """Reenvia erros ou IDs específicos via Google Translate."""
    if not DB3_PATH.exists():
        log.error("  ERRO: DB3 não encontrado.")
        sys.exit(1)

    conn3 = _abrir_db3()

    if DB2_PATH.exists():
        conn3.execute(f"ATTACH DATABASE '{str(DB2_PATH).replace(chr(92),'/')}' AS db2")
        if ids:
            # IDs específicos passados pela interface
            id_list = [i.strip() for i in ids.replace(",", " ").split() if i.strip().lstrip("-").isdigit()]
            ph = ",".join("?" * len(id_list))
            # Tenta por id do DB3 direto
            pendentes = conn3.execute(
                "SELECT t.arquivo, t.seq, t.namespace, t.subkey, t.full_key, "
                "t.texto_eng, d.texto_cht "
                "FROM traducoes t "
                "LEFT JOIN db2.dialogos d "
                "  ON d.arquivo=t.arquivo AND d.full_key=t.full_key "
                f"WHERE t.id IN ({ph})", id_list
            ).fetchall()
            if not pendentes:
                # Fallback: ids são do DB2, busca por arquivo+full_key
                db2_rows = conn3.execute(
                    f"SELECT arquivo, full_key FROM db2.dialogos WHERE id IN ({ph})", id_list
                ).fetchall()
                if db2_rows:
                    conds = " OR ".join(["(t.arquivo=? AND t.full_key=?)"] * len(db2_rows))
                    params = [v for r in db2_rows for v in (r[0], r[1])]
                    pendentes = conn3.execute(
                        "SELECT t.arquivo, t.seq, t.namespace, t.subkey, t.full_key, "
                        "t.texto_eng, d.texto_cht "
                        "FROM traducoes t "
                        "LEFT JOIN db2.dialogos d "
                        "  ON d.arquivo=t.arquivo AND d.full_key=t.full_key "
                        f"WHERE {conds}", params
                    ).fetchall()
        else:
            pendentes = conn3.execute(
                "SELECT t.arquivo, t.seq, t.namespace, t.subkey, t.full_key, "
                "t.texto_eng, d.texto_cht "
                "FROM traducoes t "
                "LEFT JOIN db2.dialogos d "
                "  ON d.arquivo=t.arquivo AND d.full_key=t.full_key "
                "WHERE t.status='erro'"
            ).fetchall()
        conn3.execute("DETACH DATABASE db2")
    else:
        pendentes = conn3.execute(
            "SELECT arquivo, seq, namespace, subkey, full_key, texto_eng, NULL "
            "FROM traducoes WHERE status='erro'"
        ).fetchall()

    log.info(f"  Entradas para traduzir: {len(pendentes)}")

    if not pendentes:
        log.info("  Nenhum erro encontrado.")
        conn3.close()
        return 0, 0

    traduzidos, erros = _loop_google(conn3, pendentes, lote_sz, fonte, atualiza=True)
    conn3.close()
    return traduzidos, erros


def main():
    parser = argparse.ArgumentParser(description="LonaRPG — Tradutor Google (gratuito)")
    parser.add_argument("--teste",          type=int, metavar="N",
                        help="Traduz apenas N entradas")
    parser.add_argument("--lote",           type=int, default=_LOTE_PAD,
                        help=f"Tamanho do lote (default: {_LOTE_PAD})")
    parser.add_argument("--reenviar-erros", action="store_true")
    parser.add_argument("--ids",            type=str,
                        help="IDs para reenviar (ex: 12,34,56)")
    parser.add_argument("--fonte",          type=str, default="auto")
    args = parser.parse_args()

    _checar_instalacao()

    log.info("")
    fonte_label = {"eng":"INGLÊS (ENG)","cht":"CHINÊS (CHT)","kor":"COREANO (KOR)",
                    "rus":"RUSSO (RUS)","ukt":"UCRANIANO (UKT)","auto":"AUTO"}.get(args.fonte.lower(), args.fonte.upper())
    log.info("=" * 60)
    log.info("  TRADUTOR GOOGLE — deep_translator (gratuito)")
    log.info(f"  Traduzindo : {fonte_label} → PORTUGUÊS (PT-BR)")
    log.info(f"  Lote       : {args.lote}")
    if args.teste:
        log.info(f"  MODO TESTE : {args.teste} entradas")
    if args.ids:
        log.info(f"  IDs        : {args.ids}")
    log.info("=" * 60)
    log.info("")

    if args.reenviar_erros or args.ids:
        traduzidos, erros = reenviar_erros_google(args.lote, args.fonte, ids=args.ids)
    else:
        traduzidos, erros = traduzir_google(args.lote, args.teste, args.fonte)

    log.info("")
    log.info("=" * 60)
    log.info("  CONCLUÍDO — Google Translate")
    log.info(f"  Traduzidos : {traduzidos}")
    log.info(f"  Erros      : {erros}")
    log.info("=" * 60)

    mostrar_stats()


if __name__ == "__main__":
    main()
