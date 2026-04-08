#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
servidor.py — LonaRPG Translator PT-BR · Backend Flask
════════════════════════════════════════════════════════
Liga o front-end HTML aos bancos de dados SQLite reais.

Instalar dependência única:
    pip install flask

Executar:
    python servidor.py          (abre browser automaticamente)
    python servidor.py --porta 5001   (porta alternativa)
    python servidor.py --sem-browser  (só inicia o servidor)
"""

import json
import os
import sqlite3
import subprocess
import sys
import queue
import threading
import webbrowser
import argparse
from pathlib import Path
from datetime import datetime

# ── Verificar Flask ────────────────────────────────────────────
try:
    from flask import Flask, jsonify, request, send_file, Response
except ImportError:
    print("\n" + "═"*54)
    print("  [ERRO] Flask não instalado.")
    print("  Execute um dos comandos abaixo e tente novamente:")
    print()
    print("  pip install flask")
    print("  pip install flask --break-system-packages")
    print("═"*54 + "\n")
    sys.exit(1)

# ── Caminhos ───────────────────────────────────────────────────
HERE    = Path(__file__).parent
DB_DIR  = HERE / "database"
DB1     = DB_DIR / "db1_estrutura.sqlite"
DB2     = DB_DIR / "db2_dialogos.sqlite"
DB2B    = DB_DIR / "db2b_fila.sqlite"
DB3     = DB_DIR / "db3_traducao.sqlite"
HTML    = HERE / "LonaTranslator.html"
CONFIG  = HERE / "config.json"

app = Flask(__name__)

# Permite que o HTML aberto como arquivo local acesse a API (fallback)
@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def db3_conn():
    """Retorna conexão ao DB3 com DB2 anexado (se existir)."""
    if not DB3.exists():
        return None, "db3_traducao.sqlite não encontrado. Execute etapa [4] primeiro."
    conn = sqlite3.connect(str(DB3))
    conn.row_factory = sqlite3.Row
    if DB2.exists():
        conn.execute(f"ATTACH DATABASE '{str(DB2).replace(chr(92), '/')}' AS db2")
    return conn, None

def _checar_google():
    try:
        from deep_translator import GoogleTranslator
        return True
    except ImportError:
        return False


def cfg_default():
    return {
        "api_key":          "",          # legado (retrocompat)
        "api_key_openai":   "",          # chave OpenAI dedicada
        "api_key_deepseek": "",          # chave DeepSeek dedicada
        "modelo":           "gpt-4o-mini",
        "base_url":         "https://api.openai.com/v1/chat/completions",
        "lote_size":        20,
        "temperatura":      0.3,
        "timeout":          60,
        "max_retries":      3,
    }

def load_config():
    if CONFIG.exists():
        try:
            return json.loads(CONFIG.read_text(encoding="utf-8"))
        except Exception:
            pass
    return cfg_default()

def migrate_db3():
    """
    Adiciona colunas extras e trigger de proteção ao DB3.
    Chamado automaticamente no startup.
    """
    if not DB3.exists():
        return
    try:
        conn = sqlite3.connect(str(DB3))
        # Adiciona colunas se não existem
        for col, defval in [
            ("status_revisao",  "NULL"),
            ("nota_revisor",    "NULL"),
            ("tags_traduz_ok",  "NULL"),  # NULL=não verificado, 1=aprovado, 0=rejeitado
        ]:
            try:
                conn.execute(f"ALTER TABLE traducoes ADD COLUMN {col} TEXT DEFAULT {defval}")
                conn.commit()
            except sqlite3.OperationalError:
                pass

        # Tabela auxiliar para preservar status_revisao durante INSERT OR REPLACE
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS _revisao_bak (
                arquivo  TEXT NOT NULL,
                full_key TEXT NOT NULL,
                status_revisao TEXT,
                nota_revisor   TEXT,
                PRIMARY KEY (arquivo, full_key)
            );

            -- Antes de deletar (passo 1 do INSERT OR REPLACE): salva status_revisao
            CREATE TRIGGER IF NOT EXISTS trg_bak_revisao
            BEFORE DELETE ON traducoes
            FOR EACH ROW
            WHEN OLD.status_revisao IS NOT NULL OR OLD.nota_revisor IS NOT NULL
            BEGIN
                INSERT OR REPLACE INTO _revisao_bak
                    (arquivo, full_key, status_revisao, nota_revisor)
                VALUES
                    (OLD.arquivo, OLD.full_key, OLD.status_revisao, OLD.nota_revisor);
            END;

            -- Após inserir (passo 2 do INSERT OR REPLACE): restaura status_revisao
            CREATE TRIGGER IF NOT EXISTS trg_restore_revisao
            AFTER INSERT ON traducoes
            FOR EACH ROW
            BEGIN
                UPDATE traducoes
                SET
                    status_revisao = COALESCE(
                        (SELECT status_revisao FROM _revisao_bak
                         WHERE arquivo=NEW.arquivo AND full_key=NEW.full_key),
                        NEW.status_revisao
                    ),
                    nota_revisor = COALESCE(
                        (SELECT nota_revisor FROM _revisao_bak
                         WHERE arquivo=NEW.arquivo AND full_key=NEW.full_key),
                        NEW.nota_revisor
                    )
                WHERE arquivo=NEW.arquivo AND full_key=NEW.full_key;
                -- Limpa o backup após restaurar
                DELETE FROM _revisao_bak
                WHERE arquivo=NEW.arquivo AND full_key=NEW.full_key;
            END;
        """)
        conn.commit()
        conn.close()
        print("  DB3 protegido: triggers de status_revisao ativos.")
    except Exception as e:
        print(f"  [WARN] migrate_db3: {e}")

# ══════════════════════════════════════════════════════════════
# ROTAS — FRONT-END
# ══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    if not HTML.exists():
        return Response(
            "<h2>LonaTranslator.html não encontrado ao lado de servidor.py</h2>",
            content_type="text/html", status=404
        )
    return send_file(str(HTML))

# ══════════════════════════════════════════════════════════════
# ROTAS — STATUS
# ══════════════════════════════════════════════════════════════

@app.route("/api/status")
def api_status():
    stats = {}
    if DB3.exists():
        try:
            c = sqlite3.connect(str(DB3))
            stats["total"]       = c.execute("SELECT COUNT(*) FROM traducoes").fetchone()[0]
            stats["aproveitado"] = c.execute("SELECT COUNT(*) FROM traducoes WHERE status='aproveitado'").fetchone()[0]
            stats["traduzido"]   = c.execute("SELECT COUNT(*) FROM traducoes WHERE status='traduzido'").fetchone()[0]
            stats["erro"]        = c.execute("SELECT COUNT(*) FROM traducoes WHERE status='erro'").fetchone()[0]
            stats["tags_bad"]      = c.execute("SELECT COUNT(*) FROM traducoes WHERE tags_ok=0 AND status!='erro'").fetchone()[0]
            stats["tags_traduz"]   = c.execute("SELECT COUNT(*) FROM traducoes WHERE tags_traduz_ok IS NULL AND status IN ('traduzido','aproveitado') AND (texto_ptbr LIKE '%\\board[%' OR texto_ptbr LIKE '%\\optB[%' OR texto_ptbr LIKE '%\\optD[%')").fetchone()[0]
            stats["tags_traduz_ok"]= c.execute("SELECT COUNT(*) FROM traducoes WHERE tags_traduz_ok=1").fetchone()[0]
            # Revisão humana
            try:
                stats["revisao_pendente"] = c.execute("SELECT COUNT(*) FROM traducoes WHERE status_revisao='pendente'").fetchone()[0]
                stats["revisao_ok"]       = c.execute("SELECT COUNT(*) FROM traducoes WHERE status_revisao='ok'").fetchone()[0]
            except Exception:
                stats["revisao_pendente"] = 0
                stats["revisao_ok"]       = 0
            c.close()
        except Exception as e:
            stats["db3_error"] = str(e)

    # ── Idiomas disponíveis no DB2 ──────────────────────────────
    langs = ["eng"]  # ENG sempre existe
    if DB2.exists():
        try:
            c_lang = sqlite3.connect(str(DB2))
            campo_lang = [
                ("texto_cht",  "cht"),
                ("texto_kor",  "kor"),
                ("texto_rus",  "rus"),
                ("texto_ukt",  "ukt"),
                ("texto_ptbrc","ptbrc"),
            ]
            for campo, lang in campo_lang:
                row = c_lang.execute(
                    f"SELECT 1 FROM dialogos WHERE {campo} IS NOT NULL AND {campo}!='' LIMIT 1"
                ).fetchone()
                if row:
                    langs.append(lang)
            c_lang.close()
        except Exception:
            pass

    if DB3.exists():
        langs.insert(1, "pt")  # PT-BR logo após ENG quando DB3 existe
    else:
        langs.insert(1, "pt")  # PT-BR sempre presente — é a coluna alvo de tradução

    # ── Stats do DB1 ─────────────────────────────────────────────
    if DB1.exists():
        try:
            c1 = sqlite3.connect(str(DB1))
            stats["db1_arquivos"] = c1.execute("SELECT COUNT(*) FROM arquivos").fetchone()[0]
            stats["db1_dialogos"] = c1.execute("SELECT COUNT(*) FROM entradas WHERE tipo='dialogo'").fetchone()[0]
            c1.close()
        except Exception:
            pass

    # ── Stats do DB2b ────────────────────────────────────────────
    if DB2B.exists():
        try:
            c2b = sqlite3.connect(str(DB2B))
            stats["db2b_total"]     = c2b.execute("SELECT COUNT(*) FROM fila").fetchone()[0]
            stats["db2b_pendente"]  = c2b.execute("SELECT COUNT(*) FROM fila WHERE status='pendente'").fetchone()[0]
            stats["db2b_traduzido"] = c2b.execute("SELECT COUNT(*) FROM fila WHERE texto_ptbr IS NOT NULL AND texto_ptbr!=''").fetchone()[0]
            c2b.close()
        except Exception:
            pass

    # ── Stats do DB2 ─────────────────────────────────────────────
    if DB2.exists():
        try:
            c2 = sqlite3.connect(str(DB2))
            stats["db2_total_dialogos"] = c2.execute("SELECT COUNT(*) FROM dialogos").fetchone()[0]
            stats["db2_com_ptbrc"]      = c2.execute("SELECT COUNT(*) FROM dialogos WHERE texto_ptbrc IS NOT NULL AND texto_ptbrc!=''").fetchone()[0]
            stats["db2_ausente"]        = c2.execute("SELECT COUNT(*) FROM dialogos WHERE status_ptbrc='ausente'").fetchone()[0]
            stats["db2_orphao"]         = c2.execute("SELECT COUNT(*) FROM dialogos WHERE status_ptbrc='orphao'").fetchone()[0]
            # Contadores por idioma — quantas strings cada idioma tem no DB2
            for col, key in [("texto_eng","db2_n_eng"),("texto_cht","db2_n_cht"),
                             ("texto_kor","db2_n_kor"),("texto_rus","db2_n_rus"),
                             ("texto_ukt","db2_n_ukt"),("texto_ptbrc","db2_n_ptbrc")]:
                try:
                    stats[key] = c2.execute(f"SELECT COUNT(*) FROM dialogos WHERE {col} IS NOT NULL AND {col}!=''").fetchone()[0]
                except Exception:
                    stats[key] = 0
            c2.close()
        except Exception:
            pass

    return jsonify({
        "servidor":  True,
        "timestamp": datetime.now().isoformat(),
        "dbs": {
            "db1": DB1.exists(),
            "db2": DB2.exists(),
            "db2b": DB2B.exists(),
            "db3": DB3.exists(),
        },
        "config_path": str(CONFIG),
        "config_ok":   CONFIG.exists(),
        "ptbr_gerado": (HERE.parent / "PT-BR").exists(),
        "stats": stats,
        "langs": langs,
    })

# ══════════════════════════════════════════════════════════════
# ROTAS — ENTRADAS
# ══════════════════════════════════════════════════════════════

@app.route("/api/entries")
def api_entries():
    """
    Fonte base: DB2 (dialogos) — sempre presente.
    DB3 (traducoes) é LEFT JOIN — traz PT-BR, status, tags quando existir.
    Se DB2 não existe retorna vazio.
    """
    if not DB2.exists():
        return jsonify({"entries": [], "total": 0, "db2_ok": False})

    try:
        conn = sqlite3.connect(str(DB2))
        conn.row_factory = sqlite3.Row

        if DB3.exists():
            conn.execute(f"ATTACH DATABASE '{str(DB3).replace(chr(92), '/')}' AS db3")
            sql = """
                SELECT
                    d.id                           AS id,
                    d.arquivo,
                    d.full_key,
                    d.namespace,
                    d.subkey,
                    d.seq,
                    d.texto_eng                    AS eng,
                    d.texto_cht                    AS cht,
                    d.texto_kor                    AS kor,
                    d.texto_rus                    AS rus,
                    d.texto_ukt                    AS ukt,
                    d.texto_ptbrc                  AS ptbrc,
                    d.status_ptbrc                 AS status_ptbrc,
                    t.texto_ptbr                   AS pt,
                    COALESCE(t.status, 'pendente') AS status,
                    COALESCE(t.tags_ok, 0)         AS tags_ok,
                    COALESCE(t.modelo_usado, '')   AS modelo,
                    t.traduzido_em,
                    COALESCE(t.status_revisao, '') AS status_revisao,
                    COALESCE(t.nota_revisor,   '') AS nota_revisor,
                    t.tags_traduz_ok               AS tags_traduz_ok
                FROM dialogos d
                LEFT JOIN db3.traducoes t
                    ON d.arquivo = t.arquivo AND d.full_key = t.full_key
                ORDER BY d.arquivo, d.seq
            """
        else:
            # DB3 ainda não existe — lista completa do DB2 sem PT-BR
            sql = """
                SELECT
                    id,
                    arquivo,
                    full_key,
                    namespace,
                    subkey,
                    seq,
                    texto_eng   AS eng,
                    texto_cht   AS cht,
                    texto_kor   AS kor,
                    texto_rus   AS rus,
                    texto_ukt   AS ukt,
                    texto_ptbrc AS ptbrc,
                    status_ptbrc,
                    NULL        AS pt,
                    'pendente'  AS status,
                    0           AS tags_ok,
                    ''          AS modelo,
                    NULL        AS traduzido_em,
                    ''          AS status_revisao,
                    ''          AS nota_revisor
                FROM dialogos
                ORDER BY arquivo, seq
            """

        rows = conn.execute(sql).fetchall()
        entries = [dict(r) for r in rows]
        conn.close()

        return jsonify({
            "entries": entries,
            "total":   len(entries),
            "db2_ok":  True,
            "db3_ok":  DB3.exists(),
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/entries/<int:entry_id>", methods=["POST"])
def api_save_entry(entry_id):
    """
    Salva a tradução PT-BR.
    entry_id = id do DB2 (fonte da verdade).
    Resolve arquivo+full_key a partir do DB2 e salva no DB3.
    """
    data = request.get_json()
    if not data or "texto_ptbr" not in data:
        return jsonify({"error": "Campo 'texto_ptbr' obrigatório"}), 400

    # Pega arquivo+full_key do body (enviado pelo frontend) ou resolve pelo DB2
    arquivo  = data.get("arquivo")
    full_key = data.get("full_key")

    # Se não veio no body, busca no DB2 pelo id
    if not arquivo or not full_key:
        if not DB2.exists():
            return jsonify({"error": "DB2 não encontrado"}), 503
        c2 = sqlite3.connect(str(DB2))
        c2.row_factory = sqlite3.Row
        row2 = c2.execute(
            "SELECT arquivo, full_key FROM dialogos WHERE id=?", (entry_id,)
        ).fetchone()
        c2.close()
        if not row2:
            return jsonify({"error": f"ID {entry_id} não encontrado no DB2"}), 404
        arquivo  = row2["arquivo"]
        full_key = row2["full_key"]

    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503

    try:
        modelo_save = data.get("modelo_usado", "manual")
        # Salva por arquivo+full_key — funciona independente do id do DB3
        cur = conn.execute("""
            UPDATE traducoes
            SET texto_ptbr=?, status='traduzido', tags_ok=1,
                traduzido_em=datetime('now'), modelo_usado=?, status_revisao='ok'
            WHERE arquivo=? AND full_key=?
        """, (data["texto_ptbr"], modelo_save, arquivo, full_key))
        conn.commit()

        if cur.rowcount == 0:
            # Linha não existe no DB3 — insere com id do DB2
            conn.execute("""
                INSERT OR IGNORE INTO traducoes
                (id, arquivo, seq, namespace, subkey, full_key,
                 texto_eng, texto_ptbr, status, tags_ok, modelo_usado, status_revisao)
                VALUES (?,?,0,?,?,?,?,?,'traduzido',1,?,'ok')
            """, (entry_id, arquivo,
                  full_key.split("/")[0] if "/" in full_key else full_key,
                  full_key.split("/")[-1] if "/" in full_key else full_key,
                  full_key, data.get("texto_eng",""), data["texto_ptbr"], modelo_save))
            conn.commit()

        row = conn.execute(
            "SELECT id, arquivo, full_key, texto_ptbr, status, tags_ok "
            "FROM traducoes WHERE arquivo=? AND full_key=?",
            (arquivo, full_key)
        ).fetchone()
        conn.close()
        return jsonify({"ok": True, "entry": dict(row) if row else {}})

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500



@app.route("/api/save", methods=["POST"])
def api_save():
    """Salva PT-BR por arquivo+full_key (rota principal do frontend)."""
    data     = request.get_json() or {}
    arquivo  = data.get("arquivo")
    full_key = data.get("full_key")
    texto_pt = data.get("texto_ptbr")
    modelo   = data.get("modelo_usado", "manual")
    if not arquivo or not full_key or texto_pt is None:
        return jsonify({"error": "arquivo, full_key e texto_ptbr obrigatórios"}), 400
    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503
    try:
        cur = conn.execute("""
            UPDATE traducoes
            SET texto_ptbr=?, status='traduzido', tags_ok=1,
                traduzido_em=datetime('now'), modelo_usado=?
            WHERE arquivo=? AND full_key=?
        """, (texto_pt, modelo, arquivo, full_key))
        conn.commit()
        if cur.rowcount == 0:
            # Busca id do DB2 para inserir com id correto
            db2_id = 0
            if DB2.exists():
                c2 = sqlite3.connect(str(DB2))
                r2 = c2.execute("SELECT id FROM dialogos WHERE arquivo=? AND full_key=?",
                                (arquivo, full_key)).fetchone()
                c2.close()
                if r2: db2_id = r2[0]
            conn.execute("""
                INSERT OR IGNORE INTO traducoes
                (id, arquivo, seq, namespace, subkey, full_key,
                 texto_eng, texto_ptbr, status, tags_ok, modelo_usado)
                VALUES (?,?,0,?,?,?,?,?,'traduzido',1,?)
            """, (db2_id, arquivo,
                  full_key.split("/")[0] if "/" in full_key else full_key,
                  full_key.split("/")[-1] if "/" in full_key else full_key,
                  full_key, data.get("texto_eng",""), texto_pt, modelo))
            conn.commit()
        row = conn.execute(
            "SELECT id,arquivo,full_key,texto_ptbr,status,tags_ok FROM traducoes "
            "WHERE arquivo=? AND full_key=?", (arquivo, full_key)
        ).fetchone()
        conn.close()
        return jsonify({"ok": True, "entry": dict(row) if row else {}})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/revisao/save", methods=["POST"])
def api_revisao_save():
    """Salva status_revisao por lista de arquivo+full_key."""
    data     = request.get_json() or {}
    entradas = data.get("entradas", [])
    if not entradas:
        return jsonify({"error": "entradas vazio"}), 400
    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503
    try:
        for e in entradas:
            arq = e.get("arquivo"); fk = e.get("full_key")
            val = e.get("status_revisao","") or None
            modelo = e.get("modelo")
            if not arq or not fk: continue
            if modelo is not None:
                cur = conn.execute(
                    "UPDATE traducoes SET status_revisao=?, modelo_usado=COALESCE(?,modelo_usado) "
                    "WHERE arquivo=? AND full_key=?", [val, modelo or None, arq, fk])
            else:
                cur = conn.execute(
                    "UPDATE traducoes SET status_revisao=? WHERE arquivo=? AND full_key=?",
                    [val, arq, fk])
            # Se não existe no DB3, insere com id do DB2
            if cur.rowcount == 0 and val:
                db2_id = 0
                if DB2.exists():
                    c2 = sqlite3.connect(str(DB2))
                    r2 = c2.execute("SELECT id FROM dialogos WHERE arquivo=? AND full_key=?",
                                    (arq, fk)).fetchone()
                    c2.close()
                    if r2: db2_id = r2[0]
                conn.execute(
                    "INSERT OR IGNORE INTO traducoes "
                    "(id, arquivo, seq, namespace, subkey, full_key, status, status_revisao) "
                    "VALUES (?,?,0,?,?,?,'pendente',?)",
                    (db2_id, arq,
                     fk.split("/")[0] if "/" in fk else fk,
                     fk.split("/")[-1] if "/" in fk else fk,
                     fk, val))
        conn.commit(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/restore", methods=["POST"])
def api_restore():
    """Restaura entry para pendente (apaga tradução com tags ruins)."""
    data     = request.get_json() or {}
    arquivo  = data.get("arquivo")
    full_key = data.get("full_key")
    if not arquivo or not full_key:
        return jsonify({"error": "arquivo e full_key obrigatórios"}), 400
    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503
    try:
        conn.execute("""
            UPDATE traducoes SET texto_ptbr=NULL, status='pendente',
            tags_ok=1, traduzido_em=NULL, modelo_usado=NULL, status_revisao=NULL
            WHERE arquivo=? AND full_key=?
        """, (arquivo, full_key))
        conn.commit(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/entries/ids", methods=["POST"])
def api_entries_by_ids():
    """Retorna entries específicos por lista de IDs do DB2."""
    data = request.get_json() or {}
    ids  = data.get("ids", [])
    if not ids or not DB2.exists():
        return jsonify({"entries": []}), 200
    try:
        conn = sqlite3.connect(str(DB2))
        conn.row_factory = sqlite3.Row
        ph = ",".join("?" * len(ids))
        if DB3.exists():
            conn.execute(f"ATTACH DATABASE '{str(DB3).replace(chr(92),'/')}' AS db3")
            rows = conn.execute(
                "SELECT d.id, d.arquivo, d.full_key, d.namespace, d.subkey, d.seq, "
                "d.texto_eng AS eng, d.texto_cht AS cht, d.texto_kor AS kor, "
                "d.texto_rus AS rus, d.texto_ukt AS ukt, d.texto_ptbrc AS ptbrc, "
                "t.texto_ptbr AS pt, COALESCE(t.status,'pendente') AS status, "
                "COALESCE(t.tags_ok,1) AS tags_ok, COALESCE(t.modelo_usado,'') AS modelo, "
                "COALESCE(t.status_revisao,'') AS status_revisao, "
                "COALESCE(t.nota_revisor,'') AS nota_revisor "
                "FROM dialogos d LEFT JOIN db3.traducoes t "
                "ON d.arquivo=t.arquivo AND d.full_key=t.full_key "
                f"WHERE d.id IN ({ph})", ids
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT id, arquivo, full_key, namespace, subkey, seq, "
                "texto_eng AS eng, texto_cht AS cht, NULL AS pt, "
                "'pendente' AS status, 1 AS tags_ok, '' AS modelo, "
                "'' AS status_revisao, '' AS nota_revisor "
                f"FROM dialogos WHERE id IN ({ph})", ids
            ).fetchall()
        conn.close()
        return jsonify({"entries": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/entries/batch", methods=["POST"])
def api_save_batch():
    """Salva múltiplas entradas de uma vez."""
    data = request.get_json()
    if not data or "updates" not in data:
        return jsonify({"error": "Campo 'updates' obrigatório (lista de {id, texto_ptbr})"}), 400

    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503

    try:
        updates = [(u["texto_ptbr"], u["id"]) for u in data["updates"] if "id" in u and "texto_ptbr" in u]
        conn.executemany("""
            UPDATE traducoes
            SET texto_ptbr=?, status='traduzido', tags_ok=1, traduzido_em=datetime('now')
            WHERE id=?
        """, updates)
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "updated": len(updates)})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/revisao/bulk", methods=["POST"])
def api_revisao_bulk():
    """
    Marca/desmarca status_revisao em lote — 1 transação, sem concorrência.
    Body: { "ids": [1,2,3,...], "status_revisao": "pendente"|"ok"|"",
            "modelo": "manual"|""|null (opcional) }
    """
    data   = request.get_json() or {}
    ids    = data.get("ids", [])
    val    = data.get("status_revisao", "pendente") or None
    modelo = data.get("modelo", None)

    if not ids:
        return jsonify({"error": "ids vazio"}), 400

    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503
    try:
        placeholders = ",".join("?" * len(ids))
        if modelo is not None:
            conn.execute(
                f"UPDATE traducoes SET status_revisao=?, modelo_usado=COALESCE(?,modelo_usado) WHERE id IN ({placeholders})",
                [val, modelo or None] + list(ids)
            )
        else:
            conn.execute(
                f"UPDATE traducoes SET status_revisao=? WHERE id IN ({placeholders})",
                [val] + list(ids)
            )
        conn.commit()
        affected = conn.execute(
            f"SELECT COUNT(*) FROM traducoes WHERE id IN ({placeholders})", list(ids)
        ).fetchone()[0]
        conn.close()
        return jsonify({"ok": True, "affected": affected, "ids": ids})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/entries/<int:entry_id>/revisao", methods=["POST"])
def api_toggle_revisao(entry_id):
    """
    Salva status de revisão.
    entry_id = id do DB2. Resolve arquivo+full_key e opera no DB3.
    """
    data = request.get_json() or {}

    arquivo  = data.get("arquivo")
    full_key = data.get("full_key")

    # Resolve pelo DB2 se não veio no body
    if not arquivo or not full_key:
        if not DB2.exists():
            return jsonify({"error": "DB2 não encontrado"}), 503
        c2 = sqlite3.connect(str(DB2))
        c2.row_factory = sqlite3.Row
        row2 = c2.execute(
            "SELECT arquivo, full_key FROM dialogos WHERE id=?", (entry_id,)
        ).fetchone()
        c2.close()
        if not row2:
            return jsonify({"error": f"ID {entry_id} não encontrado no DB2"}), 404
        arquivo  = row2["arquivo"]
        full_key = row2["full_key"]

    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503

    try:
        row = conn.execute(
            "SELECT status_revisao FROM traducoes WHERE arquivo=? AND full_key=?",
            (arquivo, full_key)
        ).fetchone()

        if "status_revisao" in data:
            new_status = data["status_revisao"] or None
        else:
            cur = row["status_revisao"] if row else None
            if cur is None or cur == "":  new_status = "pendente"
            elif cur == "pendente":        new_status = "ok"
            else:                          new_status = None

        nota   = data.get("nota", None)
        modelo = data.get("modelo", None)

        if row:
            if modelo is not None:
                conn.execute(
                    "UPDATE traducoes SET status_revisao=?, nota_revisor=COALESCE(?,nota_revisor), modelo_usado=? "
                    "WHERE arquivo=? AND full_key=?",
                    (new_status, nota, modelo or None, arquivo, full_key))
            else:
                conn.execute(
                    "UPDATE traducoes SET status_revisao=?, nota_revisor=COALESCE(?,nota_revisor) "
                    "WHERE arquivo=? AND full_key=?",
                    (new_status, nota, arquivo, full_key))
        else:
            # Linha não existe — insere com id do DB2
            conn.execute(
                "INSERT OR IGNORE INTO traducoes "
                "(id, arquivo, seq, namespace, subkey, full_key, status, status_revisao) "
                "VALUES (?,?,0,?,?,?,'pendente',?)",
                (entry_id, arquivo,
                 full_key.split("/")[0] if "/" in full_key else full_key,
                 full_key.split("/")[-1] if "/" in full_key else full_key,
                 full_key, new_status))

        conn.commit()
        conn.close()
        return jsonify({"ok": True, "status_revisao": new_status})

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/revisao/limpar_invalidas", methods=["POST"])
def api_revisao_limpar():
    """Remove status_revisao de entries que ainda estão pendentes de tradução.
    Esses entries não deveriam ter marcação de revisão humana."""
    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503
    try:
        cur = conn.execute(
            "UPDATE traducoes SET status_revisao=NULL WHERE status_revisao='pendente' AND (status='pendente' OR status IS NULL OR status='')"
        )
        conn.commit()
        affected = cur.rowcount
        conn.close()
        return jsonify({"ok": True, "limpos": affected})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/revisao")
def api_revisao_list():
    """Retorna só as entradas marcadas para revisão."""
    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503
    try:
        rows = conn.execute("""
            SELECT id, arquivo, full_key, texto_eng AS eng, texto_ptbr AS pt,
                   status, tags_ok, status_revisao, nota_revisor
            FROM traducoes
            WHERE status_revisao IS NOT NULL AND status_revisao != ''
            ORDER BY status_revisao DESC, arquivo, seq
        """).fetchall()
        conn.close()
        return jsonify({"entries": [dict(r) for r in rows], "total": len(rows)})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/files")
def api_files():
    """Lista de arquivos com contagens por status."""
    conn, err = db3_conn()
    if err:
        return jsonify({"error": err}), 503
    try:
        rows = conn.execute("""
            SELECT
                arquivo,
                COUNT(*)                                               AS total,
                SUM(CASE WHEN status='aproveitado' THEN 1 ELSE 0 END) AS aproveitado,
                SUM(CASE WHEN status='traduzido'   THEN 1 ELSE 0 END) AS traduzido,
                SUM(CASE WHEN status='erro'        THEN 1 ELSE 0 END) AS erros,
                SUM(CASE WHEN tags_ok=0 AND status!='erro' THEN 1 ELSE 0 END) AS tags_bad
            FROM traducoes
            GROUP BY arquivo
            ORDER BY arquivo
        """).fetchall()
        conn.close()
        return jsonify({"files": [dict(r) for r in rows]})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════════
# ROTAS — CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════


@app.route("/api/db3/salvar-patch", methods=["POST"])
def api_salvar_patch():
    """
    Renomeia o DB3 atual para db3_traducao_patch.sqlite.
    Na próxima criação do DB3, o patch é automaticamente mesclado.
    """
    if not DB3.exists():
        return jsonify({"ok": False, "msg": "DB3 não encontrado"}), 404
    import shutil
    patch = DB_DIR / "db3_traducao_patch.sqlite"
    shutil.copy2(str(DB3), str(patch))
    total = sqlite3.connect(str(patch)).execute(
        "SELECT COUNT(*) FROM traducoes WHERE status IN ('traduzido','aproveitado')"
    ).fetchone()[0]
    return jsonify({
        "ok":    True,
        "patch": patch.name,
        "total": total,
        "msg":   f"Patch salvo: {total} traduções preservadas para próxima recriação"
    })


@app.route("/api/db3/importar-patch", methods=["POST"])
def api_importar_patch():
    """
    Importa um DB3 anterior como patch ao criar o novo DB3.
    
    Fluxo:
      1. Cria DB3 novo a partir do DB2b (base PT-BRC)
      2. Sobrescreve com as traduções do DB3 patch (seu trabalho anterior)
      3. O que existia no patch prevalece sobre o DB2b
      4. O que o patch não tem fica como 'pendente' para traduzir
    
    Body: { "patch_path": "caminho/para/db3_patch.sqlite" }
           Se omitido, procura db3_patch.sqlite na pasta database/
    """
    data = request.get_json() or {}
    
    # Caminho do patch — pode ser enviado ou usa padrão
    patch_path = Path(data.get("patch_path", "") or str(DB_DIR / "db3_patch.sqlite"))
    
    if not patch_path.exists():
        return jsonify({
            "ok": False,
            "msg": f"Patch não encontrado: {patch_path}. "
                   f"Coloque o DB3 anterior como 'db3_patch.sqlite' na pasta database/"
        }), 404

    if not DB2B.exists():
        return jsonify({"ok": False, "msg": "DB2b não encontrado. Execute etapa [3] primeiro."}), 404

    try:
        import shutil

        # 1. Backup do DB3 atual se existir
        if DB3.exists():
            shutil.copy2(str(DB3), str(DB3).replace(".sqlite", "_backup.sqlite"))
            DB3.unlink()

        # 2. Cria DB3 novo com schema correto
        schema = """
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
        DB3.parent.mkdir(parents=True, exist_ok=True)
        conn3 = sqlite3.connect(str(DB3))
        conn3.executescript(schema)
        conn3.execute("INSERT OR REPLACE INTO meta VALUES ('modelo','patch-import')")
        conn3.execute("INSERT OR REPLACE INTO meta VALUES ('criado_em',datetime('now'))")

        # 3. Popula base com DB2b (PT-BRC aproveitados + pendentes)
        conn2b = sqlite3.connect(str(DB2B))
        conn2b.row_factory = sqlite3.Row

        if DB2.exists():
            db2_path = str(DB2).replace(chr(92), "/")
            conn2b.execute(f"ATTACH DATABASE '{db2_path}' AS db2_src")

            def _fetch(status):
                return conn2b.execute(
                    "SELECT d.id, f.arquivo, f.seq, f.namespace, f.subkey, f.full_key, "
                    "f.texto_eng, f.texto_ptbr "
                    "FROM fila f "
                    "JOIN db2_src.dialogos d ON d.arquivo=f.arquivo AND d.full_key=f.full_key "
                    f"WHERE f.status='{status}'"
                ).fetchall()
        else:
            def _fetch(status):
                return conn2b.execute(
                    "SELECT rowid, arquivo, seq, namespace, subkey, full_key, "
                    f"texto_eng, texto_ptbr FROM fila WHERE status='{status}'"
                ).fetchall()

        # Insere aproveitados PT-BRC
        aproveitados = _fetch("aproveitado")
        conn3.executemany(
            "INSERT OR REPLACE INTO traducoes "
            "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,texto_ptbr,"
            "status,tags_ok,modelo_usado) "
            "VALUES (?,?,?,?,?,?,?,?,'aproveitado',1,'PT-BRC')",
            [(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7]) for r in aproveitados]
        )

        # Insere pendentes
        pendentes = _fetch("pendente")
        conn3.executemany(
            "INSERT OR REPLACE INTO traducoes "
            "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,status,tags_ok) "
            "VALUES (?,?,?,?,?,?,?,'pendente',1)",
            [(r[0],r[1],r[2],r[3],r[4],r[5],r[6]) for r in pendentes]
        )
        conn3.commit()

        if DB2.exists():
            try: conn2b.execute("DETACH DATABASE db2_src")
            except: pass
        conn2b.close()

        n_base = conn3.execute("SELECT COUNT(*) FROM traducoes").fetchone()[0]

        # 4. Aplica o patch por cima — prevalece sobre a base
        conn_patch = sqlite3.connect(str(patch_path))
        conn_patch.row_factory = sqlite3.Row

        patch_rows = conn_patch.execute("""
            SELECT arquivo, full_key, texto_ptbr, status, tags_ok,
                   modelo_usado, status_revisao, nota_revisor
            FROM traducoes
            WHERE status IN ('traduzido','aproveitado')
            AND texto_ptbr IS NOT NULL AND texto_ptbr != ''
        """).fetchall()
        conn_patch.close()

        aplicados     = 0
        novos_patch   = 0

        for r in patch_rows:
            # Tenta UPDATE por arquivo+full_key
            cur = conn3.execute("""
                UPDATE traducoes
                SET texto_ptbr=?, status=?, tags_ok=?,
                    modelo_usado=?, status_revisao=?,
                    nota_revisor=?, traduzido_em=datetime('now')
                WHERE arquivo=? AND full_key=?
            """, (r['texto_ptbr'], r['status'], r['tags_ok'],
                  r['modelo_usado'], r['status_revisao'],
                  r['nota_revisor'], r['arquivo'], r['full_key']))

            if cur.rowcount > 0:
                aplicados += 1
            else:
                # Entrada nova no patch que não existe no DB2b atual
                # (string nova no patch, ainda não no DB2)
                conn3.execute("""
                    INSERT OR IGNORE INTO traducoes
                    (arquivo, seq, namespace, subkey, full_key,
                     texto_eng, texto_ptbr, status, tags_ok,
                     modelo_usado, status_revisao)
                    VALUES (?,0,?,?,?,?,?,?,?,?,?)
                """, (r['arquivo'],
                      r['full_key'].split('/')[0] if '/' in r['full_key'] else r['full_key'],
                      r['full_key'].split('/')[-1] if '/' in r['full_key'] else r['full_key'],
                      r['full_key'], '', r['texto_ptbr'], r['status'],
                      r['tags_ok'], r['modelo_usado'], r['status_revisao']))
                novos_patch += 1

        conn3.commit()

        # Stats finais
        total    = conn3.execute("SELECT COUNT(*) FROM traducoes").fetchone()[0]
        trad     = conn3.execute("SELECT COUNT(*) FROM traducoes WHERE status IN ('traduzido','aproveitado')").fetchone()[0]
        pend     = conn3.execute("SELECT COUNT(*) FROM traducoes WHERE status='pendente'").fetchone()[0]
        conn3.close()

        return jsonify({
            "ok":           True,
            "total":        total,
            "base_ptbrc":   len(aproveitados),
            "base_pend":    len(pendentes),
            "patch_total":  len(patch_rows),
            "patch_aplicados": aplicados,
            "patch_novos":  novos_patch,
            "traduzidos":   trad,
            "pendentes":    pend,
            "msg": (f"DB3 criado: {total} entradas. "
                    f"PT-BRC base: {len(aproveitados)}. "
                    f"Patch aplicado: {aplicados} atualizados + {novos_patch} novos. "
                    f"Pendentes: {pend}")
        })

    except Exception as e:
        import traceback
        return jsonify({"ok": False, "msg": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/db2b/sincronizar", methods=["POST"])
def api_sincronizar_db2b():
    """
    Sincroniza o DB2b com as traduções do DB3.
    Marca como 'aproveitado' no DB2b todas as entradas que foram
    traduzidas/aprovadas no DB3 — garante que ao recriar o DB3
    essas traduções não se percam.
    
    Execute ANTES de exportar ou recriar o DB3.
    """
    if not DB3.exists():
        return jsonify({"ok": False, "msg": "DB3 não encontrado"}), 404
    if not DB2B.exists():
        return jsonify({"ok": False, "msg": "DB2b não encontrado"}), 404

    try:
        conn3  = sqlite3.connect(str(DB3))
        conn3.row_factory = sqlite3.Row
        conn2b = sqlite3.connect(str(DB2B))

        # Busca todas as traduções completas do DB3
        traduzidas = conn3.execute("""
            SELECT arquivo, full_key, texto_ptbr, status
            FROM traducoes
            WHERE status IN ('traduzido','aproveitado')
            AND texto_ptbr IS NOT NULL
            AND texto_ptbr != ''
        """).fetchall()

        atualizados = 0
        for r in traduzidas:
            cur = conn2b.execute("""
                UPDATE fila
                SET status='aproveitado', texto_ptbr=?
                WHERE arquivo=? AND full_key=? AND status='pendente'
            """, (r['texto_ptbr'], r['arquivo'], r['full_key']))
            atualizados += cur.rowcount

        conn2b.commit()
        conn3.close()
        conn2b.close()

        return jsonify({
            "ok":          True,
            "atualizados": atualizados,
            "total_db3":   len(traduzidas),
            "msg": f"{atualizados} entradas sincronizadas do DB3 para o DB2b"
        })

    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/db3/criar", methods=["POST"])
def api_criar_db3():
    """
    Cria o DB3 com merge inteligente — funciona para TODOS os casos:

    CASO 1 — Projeto do zero (sem patch):
      DB2b aproveitados → status='aproveitado' (PT-BRC)
      DB2b pendentes    → status='pendente'

    CASO 2 — Tem patch anterior (DB3 de versão anterior do jogo):
      Igual ao caso 1, depois aplica o DB3 antigo por cima.
      DB3 antigo traduzido/aproveitado → mantém a tradução (prioridade máxima)
      DB3 antigo erro → volta para pendente (vai retraduzer)

    CASO 3 — Rebuild com DB3 atual (100% traduzido):
      Igual ao caso 2 — DB3 atual tem prioridade sobre DB2b.
      Nenhuma tradução é perdida.

    Parâmetros opcionais no body JSON:
      force        : bool  — recria mesmo se DB3 já existir (default: False)
      patch_db     : str   — caminho para DB3 de patch anterior (default: DB3 atual se existir)
    """
    data     = request.get_json() or {}
    force    = data.get("force", False)
    patch_db = data.get("patch_db", None)

    if DB3.exists() and not force:
        return jsonify({"ok": False, "msg": "DB3 já existe. Use force=true para recriar."})

    if not DB2B.exists():
        return jsonify({"ok": False, "msg": "DB2b não encontrado. Execute a etapa [3] primeiro."})

    try:
        import shutil

        # Guarda o DB3 atual como patch se não foi especificado outro
        patch_path = None
        if patch_db:
            patch_path = Path(patch_db)
        elif DB3.exists():
            patch_path = DB3.parent / "db3_patch_temp.sqlite"
            shutil.copy2(str(DB3), str(patch_path))

        # Recria o DB3
        DB3.parent.mkdir(parents=True, exist_ok=True)
        if DB3.exists():
            shutil.copy2(str(DB3), str(DB3).replace(".sqlite", "_backup.sqlite"))
            DB3.unlink()

        schema = """
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
        conn3 = sqlite3.connect(str(DB3))
        conn3.executescript(schema)
        conn3.execute("INSERT OR REPLACE INTO meta VALUES ('modelo','manual')")
        conn3.execute("INSERT OR REPLACE INTO meta VALUES ('criado_em',datetime('now'))")

        # ── PASSO 1: Popula a partir do DB2b ─────────────────────────
        conn2b = sqlite3.connect(str(DB2B))
        conn2b.row_factory = sqlite3.Row

        if DB2.exists():
            db2_path = str(DB2).replace(chr(92), "/")
            conn2b.execute(f"ATTACH DATABASE '{db2_path}' AS db2_src")
            def _fetch(where):
                return conn2b.execute(
                    "SELECT d.id, f.arquivo, f.seq, f.namespace, f.subkey, f.full_key,"
                    "f.texto_eng, f.texto_ptbr "
                    "FROM fila f "
                    "JOIN db2_src.dialogos d ON d.arquivo=f.arquivo AND d.full_key=f.full_key "
                    f"WHERE f.status='{where}'"
                ).fetchall()
        else:
            def _fetch(where):
                return conn2b.execute(
                    "SELECT rowid,arquivo,seq,namespace,subkey,full_key,texto_eng,texto_ptbr "
                    f"FROM fila WHERE status='{where}'"
                ).fetchall()

        apv  = _fetch("aproveitado")
        pend = _fetch("pendente")

        # Insere aproveitados (PT-BRC)
        conn3.executemany(
            "INSERT OR IGNORE INTO traducoes "
            "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,texto_ptbr,status,tags_ok,modelo_usado) "
            "VALUES (?,?,?,?,?,?,?,?,'aproveitado',1,'PT-BRC')",
            [(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7]) for r in apv]
        )
        # Insere pendentes
        conn3.executemany(
            "INSERT OR IGNORE INTO traducoes "
            "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,status,tags_ok) "
            "VALUES (?,?,?,?,?,?,?,'pendente',1)",
            [(r[0],r[1],r[2],r[3],r[4],r[5],r[6]) for r in pend]
        )
        conn3.commit()

        if DB2.exists():
            try: conn2b.execute("DETACH DATABASE db2_src")
            except: pass
        conn2b.close()

        n_base_apv  = len(apv)
        n_base_pend = len(pend)

        # ── PASSO 2: Aplica patch do DB3 anterior (se existir) ───────
        n_patch = 0
        if patch_path and patch_path.exists():
            patch_path_str = str(patch_path).replace(chr(92), "/")
            conn3.execute(f"ATTACH DATABASE '{patch_path_str}' AS patch")

            # Aplica traduções do patch com prioridade máxima
            # (só traduzido e aproveitado — ignora erros e pendentes)
            cur = conn3.execute("""
                UPDATE traducoes
                SET texto_ptbr    = p.texto_ptbr,
                    status        = p.status,
                    tags_ok       = p.tags_ok,
                    modelo_usado  = p.modelo_usado,
                    traduzido_em  = p.traduzido_em,
                    status_revisao= p.status_revisao,
                    nota_revisor  = p.nota_revisor
                FROM patch.traducoes p
                WHERE traducoes.arquivo  = p.arquivo
                AND   traducoes.full_key = p.full_key
                AND   p.status IN ('traduzido','aproveitado')
                AND   p.texto_ptbr IS NOT NULL
                AND   p.texto_ptbr != ''
            """)
            n_patch = cur.rowcount
            conn3.commit()

            try: conn3.execute("DETACH DATABASE patch")
            except: pass

            # Remove temp se criamos nós
            if patch_path == DB3.parent / "db3_patch_temp.sqlite":
                try: patch_path.unlink()
                except: pass

        total     = conn3.execute("SELECT COUNT(*) FROM traducoes").fetchone()[0]
        traduzido = conn3.execute("SELECT COUNT(*) FROM traducoes WHERE status IN ('traduzido','aproveitado')").fetchone()[0]
        pendente  = conn3.execute("SELECT COUNT(*) FROM traducoes WHERE status='pendente'").fetchone()[0]
        conn3.close()

        msg = (f"DB3 criado: {total} entradas total · "
               f"{traduzido} traduzidas · {pendente} pendentes")
        if n_patch:
            msg += f" · {n_patch} preservadas do patch anterior"

        return jsonify({
            "ok":          True,
            "total":       total,
            "traduzido":   traduzido,
            "pendente":    pendente,
            "patch":       n_patch,
            "base_apv":    n_base_apv,
            "base_pend":   n_base_pend,
            "msg":         msg,
        })

    except Exception as e:
        import traceback
        return jsonify({"ok": False, "msg": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/config", methods=["GET"])
def api_get_config():
    cfg = load_config()
    safe = dict(cfg)

    def key_meta(k):
        k = (k or "").strip()
        return {"exists": bool(k), "len": len(k),
                "hint": ("***" + k[-4:]) if len(k) > 4 else ("*" * len(k)) if k else ""}

    ko = safe.pop("api_key_openai",   "") or ""
    kd = safe.pop("api_key_deepseek", "") or ""
    kl = safe.pop("api_key",          "") or ""   # legado

    safe["api_key"]          = ""
    safe["api_key_openai"]   = ""
    safe["api_key_deepseek"] = ""
    safe["key_openai"]       = key_meta(ko or kl)  # fallback legado
    safe["key_deepseek"]     = key_meta(kd)

    # campos legados para não quebrar nada
    active = kd if "deepseek" in safe.get("modelo","") else (ko or kl)
    safe["google_disponivel"] = _checar_google()
    safe["api_key_exists"] = bool(active)
    safe["api_key_len"]    = len(active.strip())
    safe["api_key_hint"]   = ("***" + active.strip()[-4:]) if len(active.strip()) > 4 else ""
    return jsonify(safe)

@app.route("/api/config", methods=["POST"])
def api_save_config():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Dados inválidos"}), 400
    try:
        current = load_config()

        def key_meta(k):
            k = (k or "").strip()
            return {"exists": bool(k), "len": len(k),
                    "hint": ("***" + k[-4:]) if len(k) > 4 else ""}

        # ── Salva cada chave no campo certo ────────────────────
        nko = (data.get("api_key_openai",   "") or "").strip()
        nkd = (data.get("api_key_deepseek", "") or "").strip()
        nkl = (data.get("api_key",          "") or "").strip()  # legado

        if nko:  current["api_key_openai"]   = nko
        if nkd:  current["api_key_deepseek"] = nkd
        if nkl and not nko and not nkd:
            current["api_key_openai"] = nkl   # legado → trata como OpenAI

        # ── Outros campos ──────────────────────────────────────
        skip = {"api_key", "api_key_openai", "api_key_deepseek"}
        for k, v in data.items():
            if k not in skip:
                current[k] = v

        # ── api_key legado: mantém sincronizado ────────────────
        modelo = current.get("modelo", "gpt-4o-mini")
        if "deepseek" in modelo.lower():
            current["api_key"] = current.get("api_key_deepseek") or current.get("api_key","")
        else:
            current["api_key"] = current.get("api_key_openai") or current.get("api_key","")

        CONFIG.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")

        return jsonify({
            "ok":            True,
            "modelo":        modelo,
            "key_openai":    key_meta(current.get("api_key_openai",   "")),
            "key_deepseek":  key_meta(current.get("api_key_deepseek", "")),
            "api_key_exists": bool(current.get("api_key","")),
            "api_key_len":    len((current.get("api_key","") or "").strip()),
            "api_key_hint":   ("***" + (current.get("api_key","") or "")[-4:]) if len((current.get("api_key","") or "").strip()) > 4 else "",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════════
# ROTAS — PIPELINE (executa os scripts Python existentes)
# ══════════════════════════════════════════════════════════════

PIPELINE_SCRIPTS = {
    # step_name -> comando base (tradutor é resolvido dinamicamente pelo provider)
    "extrator":      ["python", "extrator.py"],
    "atualizador":   ["python", "atualizador.py"],
    "extrator_db2":  ["python", "extrator_db2.py"],
    "traduzir":      None,   # resolvido por provider
    "traduzir_teste":None,   # resolvido por provider
    "reenviar":      None,   # resolvido por provider
    "gerador":       ["python", "gerador_arquivos.py"],
    "gerador_dry":   ["python", "gerador_arquivos.py", "--dry-run"],
    "gerador_rel":   ["python", "gerador_arquivos.py", "--relatorio"],
}

def _resolver_script_tradutor(step, data):
    """Escolhe tradutor_gpt.py ou tradutor_deepseek.py pelo provider/modelo."""
    modelo   = data.get("modelo", "gpt-4o-mini")
    provider = data.get("provider", "")
    # provider explícito tem prioridade; senão detecta pelo nome do modelo
    if provider == "google" or "google" in modelo.lower():
        script = "tradutor_google.py"
    elif provider == "deepseek" or "deepseek" in modelo.lower():
        script = "tradutor_deepseek.py"
    else:
        script = "tradutor_gpt.py"

    if step == "traduzir":
        return ["python", script]
    if step == "traduzir_teste":
        return ["python", script, "--teste", "5"]
    if step == "reenviar":
        return ["python", script, "--reenviar-erros"]
    return ["python", script]

@app.route("/api/pipeline/<step>", methods=["POST"])
def api_pipeline(step):
    """
    Executa um script Python do pipeline.
    Body opcional: { "modelo": "deepseek-chat", "arquivo": "menu.txt" }
    Retorna stdout/stderr truncados.
    """
    if step not in PIPELINE_SCRIPTS:
        return jsonify({"error": f"Step '{step}' desconhecido", "validos": list(PIPELINE_SCRIPTS.keys())}), 400

    data = request.get_json() or {}
    base = PIPELINE_SCRIPTS[step]
    cmd  = list(_resolver_script_tradutor(step, data) if base is None else base)

    _TRAD_STEPS = ("traduzir", "traduzir_teste", "reenviar")
    _prov = data.get("provider",""); _mod = data.get("modelo","")
    _is_google = _prov == "google" or "google" in _mod.lower()
    if _mod and step in _TRAD_STEPS and not _is_google:
        cmd += ["--modelo", _mod]
    if data.get("fonte_lang") and step in _TRAD_STEPS:
        cmd += ["--fonte", data["fonte_lang"]]
    if data.get("arquivo"):
        cmd += ["--arquivo", data["arquivo"]]
    if data.get("ids") and step in _TRAD_STEPS:
        ids_raw = str(data["ids"])
        _prov2 = data.get("provider",""); _mod2 = data.get("modelo","")
        _is_g2 = _prov2 == "google" or "google" in _mod2.lower()
        ids_conv2 = _converter_ids_para_core(ids_raw) if not _is_g2 else ids_raw
        cmd += ["--ids", ids_conv2]

    try:
        result = subprocess.run(
            cmd, cwd=str(HERE),
            capture_output=True, text=True,
            timeout=600, encoding="utf-8", errors="replace"
        )
        return jsonify({
            "ok":         result.returncode == 0,
            "returncode": result.returncode,
            "stdout":     result.stdout[-6000:],
            "stderr":     result.stderr[-2000:],
        })
    except subprocess.TimeoutExpired:
        return jsonify({
            "ok":    False,
            "error": "Timeout (10 min) — processo ainda pode estar rodando em background"
        }), 408
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500



# ══════════════════════════════════════════════════════════════════
# PIPELINE STREAMING — Queue + Thread (sem bloquear o servidor)
# ══════════════════════════════════════════════════════════════════

def _pipeline_worker(cmd, q, cwd):
    """
    Thread de background: executa o subprocess e coloca cada linha
    de saída numa Queue. Linha final é o sentinel (None, returncode).
    PYTHONUNBUFFERED=1 garante que o subprocess não bufferiza o stdout.
    """
    import os as _os
    env = _os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,          # line-buffered
            env=env,
        )
        for raw in iter(proc.stdout.readline, ""):
            line = raw.rstrip("\n\r ")
            if line:
                q.put(("line", line))
        proc.stdout.close()
        proc.wait()
        q.put(("done", proc.returncode))
    except Exception as exc:
        q.put(("error", str(exc)))


@app.route("/api/pipeline/<step>/stream", methods=["GET"])
def api_pipeline_stream(step):
    """
    SSE: envia cada linha do subprocess em tempo real.
    O front usa EventSource (GET) para consumir este stream.
    Parâmetros via query string: modelo, arquivo, ids.
    """
    if step not in PIPELINE_SCRIPTS:
        def _err():
            yield f"data: {json.dumps({'line': f'[ERRO] step desconhecido: {step}', 'done': True, 'returncode': 1})}\n\n"
        return Response(_err(), mimetype="text/event-stream")

    modelo     = request.args.get("modelo",     "")
    arquivo    = request.args.get("arquivo",    "")
    ids        = request.args.get("ids",        "")
    provider   = request.args.get("provider",   "")
    fonte_lang = request.args.get("fonte_lang", "")

    # Resolve script correto (GPT ou DeepSeek) para steps de tradução
    base = PIPELINE_SCRIPTS[step]
    if base is None:
        # traduzir / traduzir_teste / reenviar — dinâmico pelo provider/modelo
        cmd = list(_resolver_script_tradutor(step, {"modelo": modelo, "provider": provider}))
    else:
        cmd = list(base)

    _TRAD_STEPS = ("traduzir", "traduzir_teste", "reenviar")
    _is_google = provider == "google" or "google" in modelo.lower()
    if modelo and step in _TRAD_STEPS and not _is_google:
        cmd += ["--modelo", modelo]
    if fonte_lang and step in _TRAD_STEPS:
        cmd += ["--fonte", fonte_lang]
    if arquivo:
        cmd += ["--arquivo", arquivo]
    if ids and step in _TRAD_STEPS:
        ids_conv = _converter_ids_para_core(ids) if not _is_google else ids
        cmd += ["--ids", ids_conv]

    q = queue.Queue(maxsize=0)
    t = threading.Thread(target=_pipeline_worker, args=(cmd, q, HERE), daemon=True)
    t.start()

    def generate():
        yield f"data: {json.dumps({'line': '▶ ' + ' '.join(cmd)})}\n\n"
        while True:
            try:
                kind, data = q.get(timeout=180)  # 3 min keepalive
                if kind == "line":
                    yield f"data: {json.dumps({'line': data})}\n\n"
                elif kind == "done":
                    yield f"data: {json.dumps({'done': True, 'returncode': data})}\n\n"
                    break
                elif kind == "error":
                    yield f"data: {json.dumps({'line': '[ERRO] ' + data, 'done': True, 'returncode': 1})}\n\n"
                    break
            except queue.Empty:
                # keep-alive ping para não perder a conexão SSE
                yield f"data: {json.dumps({'ping': True})}\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering":"no",
            "Connection":       "keep-alive",
        },
    )

@app.route("/api/pipeline/status", methods=["GET"])
def api_pipeline_status():
    """Retorna o status atual dos bancos e arquivos gerados."""
    status = {
        "db1_existe":   DB1.exists(),
        "db2_existe":   DB2.exists(),
        "db2b_existe":  DB2B.exists(),
        "db3_existe":   DB3.exists(),
        "ptbr_gerado":  (HERE / "PT-BR").exists(),
        "config_existe": CONFIG.exists(),
    }
    if DB3.exists():
        try:
            c = sqlite3.connect(str(DB3))
            status["db3_total"]       = c.execute("SELECT COUNT(*) FROM traducoes").fetchone()[0]
            status["db3_aproveitado"] = c.execute("SELECT COUNT(*) FROM traducoes WHERE status='aproveitado'").fetchone()[0]
            status["db3_traduzido"]   = c.execute("SELECT COUNT(*) FROM traducoes WHERE status='traduzido'").fetchone()[0]
            status["db3_erro"]        = c.execute("SELECT COUNT(*) FROM traducoes WHERE status='erro'").fetchone()[0]
            c.close()
        except Exception:
            pass
    return jsonify(status)

# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def _auto_criar_db3():
    """
    Cria DB3 mesclando TODAS as fontes disponíveis.

    Prioridade das traduções (maior = sobrescreve menor):
      1. DB2b status='aproveitado'  — PT-BRC da comunidade
      2. DB2b status='pendente'     — sem tradução ainda
      3. DB3 patch anterior         — traduções já feitas (MAIOR PRIORIDADE)
         (se existir db3_traducao_patch.sqlite ou db3_traducao_backup.sqlite)

    Funciona em todos os casos:
      - Projeto do zero (só DB2b)
      - Com patch anterior (DB2b + DB3 antigo)
      - Múltiplos patches acumulados
    """
    import shutil

    # Detecta patch anterior — DB3 renomeado antes de recriar
    PATCH_PATHS = [
        DB_DIR / "db3_traducao_patch.sqlite",
        DB_DIR / "db3_traducao_backup.sqlite",
        DB_DIR / "db3_patch.sqlite",
    ]
    patch_db = next((p for p in PATCH_PATHS if p.exists()), None)

    try:
        schema = """
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
        DB3.parent.mkdir(parents=True, exist_ok=True)
        conn3 = sqlite3.connect(str(DB3))
        conn3.executescript(schema)
        conn3.execute("INSERT OR REPLACE INTO meta VALUES ('modelo','auto')")
        conn3.commit()

        # ── PASSO 1: Popula com DB2b (base) ──────────────────────
        if DB2B.exists():
            conn2b = sqlite3.connect(str(DB2B))
            conn2b.row_factory = sqlite3.Row
            if DB2.exists():
                conn2b.execute(
                    f"ATTACH DATABASE '{str(DB2).replace(chr(92),'/')}' AS db2_src"
                )
                apv = conn2b.execute(
                    "SELECT d.id,f.arquivo,f.seq,f.namespace,f.subkey,f.full_key,"
                    "f.texto_eng,f.texto_ptbr FROM fila f "
                    "JOIN db2_src.dialogos d ON d.arquivo=f.arquivo AND d.full_key=f.full_key "
                    "WHERE f.status='aproveitado'"
                ).fetchall()
                pend = conn2b.execute(
                    "SELECT d.id,f.arquivo,f.seq,f.namespace,f.subkey,f.full_key,f.texto_eng "
                    "FROM fila f "
                    "JOIN db2_src.dialogos d ON d.arquivo=f.arquivo AND d.full_key=f.full_key "
                    "WHERE f.status='pendente'"
                ).fetchall()
                try: conn2b.execute("DETACH DATABASE db2_src")
                except: pass
            else:
                apv  = conn2b.execute(
                    "SELECT rowid,arquivo,seq,namespace,subkey,full_key,texto_eng,texto_ptbr "
                    "FROM fila WHERE status='aproveitado'"
                ).fetchall()
                pend = conn2b.execute(
                    "SELECT rowid,arquivo,seq,namespace,subkey,full_key,texto_eng "
                    "FROM fila WHERE status='pendente'"
                ).fetchall()

            conn3.executemany(
                "INSERT OR IGNORE INTO traducoes "
                "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,texto_ptbr,status,tags_ok,modelo_usado) "
                "VALUES (?,?,?,?,?,?,?,?,'aproveitado',1,'PT-BRC')",
                [(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7]) for r in apv]
            )
            conn3.executemany(
                "INSERT OR IGNORE INTO traducoes "
                "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,status,tags_ok) "
                "VALUES (?,?,?,?,?,?,?,'pendente',1)",
                [(r[0],r[1],r[2],r[3],r[4],r[5],r[6]) for r in pend]
            )
            conn3.commit()
            conn2b.close()
            n_apv  = len(apv)
            n_pend = len(pend)
            print(f"  DB2b: {n_apv} aproveitados + {n_pend} pendentes importados")
        else:
            # Sem DB2b — popula direto do DB2 tudo como pendente
            if DB2.exists():
                conn2 = sqlite3.connect(str(DB2))
                conn2.row_factory = sqlite3.Row
                rows = conn2.execute(
                    "SELECT id,arquivo,seq,namespace,subkey,full_key,texto_eng FROM dialogos"
                ).fetchall()
                conn3.executemany(
                    "INSERT OR IGNORE INTO traducoes "
                    "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,status,tags_ok) "
                    "VALUES (?,?,?,?,?,?,?,'pendente',1)",
                    [(r[0],r[1],r[2],r[3],r[4],r[5],r[6]) for r in rows]
                )
                conn3.commit()
                conn2.close()
                print(f"  DB2: {len(rows)} entradas como pendentes")

        # ── PASSO 2: Aplica patch anterior (sobrescreve) ─────────
        if patch_db:
            print(f"  Aplicando patch: {patch_db.name}")
            conn_patch = sqlite3.connect(str(patch_db))
            conn_patch.row_factory = sqlite3.Row

            # Busca TUDO que foi traduzido/aprovado no patch
            traduzidas = conn_patch.execute("""
                SELECT arquivo, full_key, texto_ptbr, status, tags_ok,
                       modelo_usado, status_revisao, nota_revisor
                FROM traducoes
                WHERE status IN ('traduzido','aproveitado')
                AND texto_ptbr IS NOT NULL AND texto_ptbr != ''
            """).fetchall()

            # UPDATE por arquivo+full_key — sobrescreve o que veio do DB2b
            for r in traduzidas:
                conn3.execute("""
                    UPDATE traducoes
                    SET texto_ptbr=?, status=?, tags_ok=?,
                        modelo_usado=?, status_revisao=?, nota_revisor=?,
                        traduzido_em=datetime('now')
                    WHERE arquivo=? AND full_key=?
                """, (r['texto_ptbr'], r['status'], r['tags_ok'],
                      r['modelo_usado'], r['status_revisao'], r['nota_revisor'],
                      r['arquivo'], r['full_key']))

            conn3.commit()
            conn_patch.close()

            total_patch = len(traduzidas)
            print(f"  Patch aplicado: {total_patch} traduções mescladas")
        else:
            print("  Sem patch anterior — usando só DB2b")

        total = conn3.execute("SELECT COUNT(*) FROM traducoes").fetchone()[0]
        pend_final = conn3.execute(
            "SELECT COUNT(*) FROM traducoes WHERE status='pendente'"
        ).fetchone()[0]
        trad_final = total - pend_final
        conn3.close()
        print(f"  DB3 pronto: {total} total | {trad_final} traduzidos | {pend_final} pendentes")

    except Exception as e:
        print(f"  [ERRO] _auto_criar_db3: {e}")
        import traceback; traceback.print_exc()



def _converter_ids_para_core(ids_str: str) -> str:
    """
    Converte IDs do DB2 para o formato esperado pelo tradutor_core:
    - ID existe no DB3 → passa positivo (UPDATE)
    - ID só existe no DB2 → passa negativo (INSERT novo no DB3)
    """
    if not ids_str or not DB3.exists():
        return ids_str
    try:
        ids = [int(i.strip()) for i in ids_str.replace(",", " ").split() if i.strip().lstrip("-").isdigit()]
        if not ids:
            return ids_str

        conn3 = sqlite3.connect(str(DB3))
        ph = ",".join("?" * len(ids))
        # Busca quais IDs existem no DB3 (herdados do DB2)
        existentes = set(r[0] for r in conn3.execute(
            f"SELECT id FROM traducoes WHERE id IN ({ph})", ids
        ).fetchall())
        conn3.close()

        resultado = []
        for i in ids:
            if i in existentes:
                resultado.append(str(i))     # positivo = existe no DB3
            else:
                resultado.append(str(-i))    # negativo = só no DB2, core fará INSERT
        return ",".join(resultado)
    except Exception:
        return ids_str  # fallback: passa como veio


# ══════════════════════════════════════════════════════════════
# VERIFICADOR DE TAGS — PASTA PT-BR (SAÍDA)
# ══════════════════════════════════════════════════════════════

@app.route("/api/verificar-tags-saida")
def api_verificar_tags_saida():
    """
    Lê os arquivos da pasta PT-BR\\ (saída do gerador) e compara as tags
    com o ENG do DB2. Retorna relatório de entradas com tags quebradas.

    A pasta PT-BR\\ tem confiança total no extrator_db2 (sem validação).
    Este endpoint torna esse ponto cego visível.
    """
    import re as _re

    SAIDA       = HERE.parent / "PT-BR"
    _TAG_RE_V   = _re.compile(r'\\[A-Za-z_]+(?:\[[^\]]*\])?|\\\\')
    _KEY_RE_V   = _re.compile(r'^([A-Za-z0-9_]+)/([A-Za-z0-9_]+)\s*$')

    if not SAIDA.exists():
        return jsonify({
            "erro": "Pasta PT-BR\\ não encontrada. Execute a etapa [6] primeiro.",
            "total": 0, "ok": 0, "quebradas": 0, "entradas": []
        }), 404

    if not DB2.exists():
        return jsonify({
            "erro": "DB2 não encontrado. Execute a etapa [3] primeiro.",
            "total": 0, "ok": 0, "quebradas": 0, "entradas": []
        }), 404

    # ── Carrega ENG do DB2 ────────────────────────────────────
    try:
        c2 = sqlite3.connect(str(DB2))
        c2.row_factory = sqlite3.Row
        eng_map = {}   # (rel_path, full_key) → texto_eng
        for row in c2.execute(
            "SELECT arquivo, full_key, texto_eng FROM dialogos WHERE texto_eng IS NOT NULL AND texto_eng!=''"
        ):
            eng_map[(row["arquivo"], row["full_key"])] = row["texto_eng"]
        c2.close()
    except Exception as e:
        return jsonify({"erro": f"Erro ao ler DB2: {e}", "total": 0}), 500

    # ── Parser idêntico ao extrator_db2 ──────────────────────
    def _parse(caminho):
        resultado = {}
        cur_key   = None
        cur_linhas = []
        try:
            linhas = caminho.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        except Exception:
            return {}
        def _flush():
            if cur_key is not None:
                resultado[cur_key] = "".join(cur_linhas).rstrip("\n\r ")
        for linha in linhas:
            s = linha.rstrip("\n\r")
            if s.lstrip().startswith("#"):
                _flush(); cur_key = None; cur_linhas = []; continue
            if s.strip() == "" and cur_key is None:
                continue
            m = _KEY_RE_V.match(s)
            if m:
                _flush()
                cur_key    = f"{m.group(1)}/{m.group(2)}"
                cur_linhas = []
                continue
            if cur_key is not None:
                cur_linhas.append(linha)
        _flush()
        return resultado

    def _tags(texto):
        return _TAG_RE_V.findall(texto or "")

    # ── Varre PT-BR\\ e compara com ENG ──────────────────────
    txts          = sorted(SAIDA.rglob("*.txt"))
    total         = 0
    ok            = 0
    sem_ref       = 0   # chave não existe no DB2 (código, não diálogo)
    lista_quebradas = []

    for arq in txts:
        rel  = str(arq.relative_to(SAIDA)).replace("\\", "/")
        mapa = _parse(arq)
        for full_key, texto_pt in mapa.items():
            texto_eng = eng_map.get((rel, full_key))
            if texto_eng is None:
                sem_ref += 1      # entrada de código — não tem ENG no DB2, normal
                continue
            total += 1
            tags_eng = _tags(texto_eng)
            tags_pt  = _tags(texto_pt)
            if tags_eng == tags_pt:
                ok += 1
            else:
                faltando = [t for t in tags_eng if t not in tags_pt]
                sobrando = [t for t in tags_pt  if t not in tags_eng]
                lista_quebradas.append({
                    "arquivo"  : rel,
                    "full_key" : full_key,
                    "tags_eng" : tags_eng,
                    "tags_pt"  : tags_pt,
                    "faltando" : faltando,
                    "sobrando" : sobrando,
                    "texto_eng": texto_eng[:150],
                    "texto_pt" : texto_pt[:150],
                })

    return jsonify({
        "total"    : total,
        "ok"       : ok,
        "quebradas": len(lista_quebradas),
        "sem_ref"  : sem_ref,
        "entradas" : lista_quebradas,
    })


# ══════════════════════════════════════════════════════════════
# APROVAR / REJEITAR TAGS TRADUZÍVEIS
# ══════════════════════════════════════════════════════════════

@app.route("/api/tags-traduz/salvar", methods=["POST"])
def api_tags_traduz_salvar():
    """
    Salva o status de revisão do conteúdo dentro das tags traduzíveis
    (\board[...], \optB[...], \optD[...]).

    Body: { "arquivo": "...", "full_key": "...", "tags_traduz_ok": 1|0|null }
      1    = conteúdo aprovado
      0    = conteúdo rejeitado (precisa reenvio)
      null = limpa (volta a não verificado)

    Retorna: { "ok": true }
    """
    if not DB3.exists():
        return jsonify({"error": "DB3 não encontrado."}), 503

    data     = request.get_json() or {}
    arquivo  = data.get("arquivo")
    full_key = data.get("full_key")
    valor    = data.get("tags_traduz_ok")   # 1, 0 ou None

    if not arquivo or not full_key:
        return jsonify({"error": "arquivo e full_key obrigatórios"}), 400

    try:
        conn = sqlite3.connect(str(DB3))
        cur  = conn.execute(
            "UPDATE traducoes SET tags_traduz_ok=? WHERE arquivo=? AND full_key=?",
            (valor, arquivo, full_key)
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "atualizadas": cur.rowcount})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tags-traduz/stats")
def api_tags_traduz_stats():
    """Retorna contadores de tags traduzíveis para o gráfico."""
    if not DB3.exists():
        return jsonify({"total":0,"pendente":0,"aprovado":0,"rejeitado":0})
    try:
        conn = sqlite3.connect(str(DB3))
        # Entradas que têm tags traduzíveis no texto PT-BR
        tem_tag = ("status IN ('traduzido','aproveitado') AND "
                   "(texto_ptbr LIKE '%\\board[%' OR texto_ptbr LIKE '%\\optB[%' OR texto_ptbr LIKE '%\\optD[%')")
        total    = conn.execute(f"SELECT COUNT(*) FROM traducoes WHERE {tem_tag}").fetchone()[0]
        aprovado = conn.execute(f"SELECT COUNT(*) FROM traducoes WHERE {tem_tag} AND tags_traduz_ok=1").fetchone()[0]
        rejeitado= conn.execute(f"SELECT COUNT(*) FROM traducoes WHERE {tem_tag} AND tags_traduz_ok=0").fetchone()[0]
        pendente = total - aprovado - rejeitado
        conn.close()
        return jsonify({"total":total,"pendente":pendente,"aprovado":aprovado,"rejeitado":rejeitado})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def parse_args():
    p = argparse.ArgumentParser(description="LonaRPG Translator · Servidor Backend")
    p.add_argument("--porta",        type=int, default=5001, help="Porta HTTP (padrão: 5001)")
    p.add_argument("--sem-browser",  action="store_true",    help="Não abre o navegador automaticamente")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    port = args.porta
    url  = f"http://localhost:{port}"

    # Migração automática do DB3 (adiciona colunas de revisão se não existirem)
    migrate_db3()

    # Status visual
    db3_ok = DB3.exists()
    db2_ok = DB2.exists()

    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║   LonaRPG Translator PT-BR  ·  Servidor Backend     ║")
    print("╠══════════════════════════════════════════════════════╣")
    print(f"║   URL:    {url:<43}║")
    print(f"║   DB3:    {'OK ✓' if db3_ok else 'NÃO ENCONTRADO (execute etapa 4)':<43}║")
    print(f"║   DB2:    {'OK ✓' if db2_ok else 'NÃO ENCONTRADO (execute etapa 3)':<43}║")
    print(f"║   HTML:   {'OK ✓' if HTML.exists() else 'NÃO ENCONTRADO':<43}║")
    print(f"║   Config: {str(CONFIG)[-43:]:<43}║")
    print("╠══════════════════════════════════════════════════════╣")
    print("║   Ctrl+C para parar o servidor                       ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    if not db3_ok:
        if DB2B.exists():
            print("  ✓  DB3 criado automaticamente.\n")
        else:
            print("  ⚠  DB2b não encontrado. Execute [3] no MENU.bat.\n")

    # Abre o browser automaticamente
    if not args.sem_browser:
        def _open():
            import time
            time.sleep(1.5)
            webbrowser.open(url)
        threading.Thread(target=_open, daemon=True).start()

    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)


def _auto_criar_db3():
    """
    Cria DB3 no startup usando merge inteligente:
    - DB2b como base (aproveitados + pendentes)
    - DB3 existente como patch (preserva traduções anteriores)
    """
    import shutil
    try:
        schema = """
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
        # Guarda patch se DB3 já existe
        patch_path = None
        if DB3.exists():
            patch_path = DB3.parent / "db3_patch_temp.sqlite"
            shutil.copy2(str(DB3), str(patch_path))
            DB3.unlink()

        DB3.parent.mkdir(parents=True, exist_ok=True)
        conn3 = sqlite3.connect(str(DB3))
        conn3.executescript(schema)
        conn3.execute("INSERT OR REPLACE INTO meta VALUES ('modelo','auto')")
        conn3.execute("INSERT OR REPLACE INTO meta VALUES ('criado_em',datetime('now'))")

        # Passo 1: DB2b como base
        conn2b = sqlite3.connect(str(DB2B))
        conn2b.row_factory = sqlite3.Row

        if DB2.exists():
            db2_path = str(DB2).replace(chr(92), "/")
            conn2b.execute(f"ATTACH DATABASE '{db2_path}' AS db2_src")
            def _fetch(where):
                return conn2b.execute(
                    "SELECT d.id,f.arquivo,f.seq,f.namespace,f.subkey,f.full_key,"
                    "f.texto_eng,f.texto_ptbr "
                    "FROM fila f "
                    "JOIN db2_src.dialogos d ON d.arquivo=f.arquivo AND d.full_key=f.full_key "
                    f"WHERE f.status='{where}'"
                ).fetchall()
        else:
            def _fetch(where):
                return conn2b.execute(
                    "SELECT rowid,arquivo,seq,namespace,subkey,full_key,texto_eng,texto_ptbr "
                    f"FROM fila WHERE status='{where}'"
                ).fetchall()

        apv  = _fetch("aproveitado")
        pend = _fetch("pendente")

        conn3.executemany(
            "INSERT OR IGNORE INTO traducoes "
            "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,texto_ptbr,status,tags_ok,modelo_usado) "
            "VALUES (?,?,?,?,?,?,?,?,'aproveitado',1,'PT-BRC')",
            [(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7]) for r in apv]
        )
        conn3.executemany(
            "INSERT OR IGNORE INTO traducoes "
            "(id,arquivo,seq,namespace,subkey,full_key,texto_eng,status,tags_ok) "
            "VALUES (?,?,?,?,?,?,?,'pendente',1)",
            [(r[0],r[1],r[2],r[3],r[4],r[5],r[6]) for r in pend]
        )
        conn3.commit()

        if DB2.exists():
            try: conn2b.execute("DETACH DATABASE db2_src")
            except: pass
        conn2b.close()

        # Passo 2: aplica patch anterior se existia
        n_patch = 0
        if patch_path and patch_path.exists():
            patch_str = str(patch_path).replace(chr(92), "/")
            conn3.execute(f"ATTACH DATABASE '{patch_str}' AS patch")
            cur = conn3.execute("""
                UPDATE traducoes
                SET texto_ptbr=p.texto_ptbr, status=p.status,
                    tags_ok=p.tags_ok, modelo_usado=p.modelo_usado,
                    traduzido_em=p.traduzido_em,
                    status_revisao=p.status_revisao, nota_revisor=p.nota_revisor
                FROM patch.traducoes p
                WHERE traducoes.arquivo=p.arquivo AND traducoes.full_key=p.full_key
                AND p.status IN ('traduzido','aproveitado')
                AND p.texto_ptbr IS NOT NULL AND p.texto_ptbr != ''
            """)
            n_patch = cur.rowcount
            conn3.commit()
            try: conn3.execute("DETACH DATABASE patch")
            except: pass
            try: patch_path.unlink()
            except: pass

        total = conn3.execute("SELECT COUNT(*) FROM traducoes").fetchone()[0]
        trad  = conn3.execute("SELECT COUNT(*) FROM traducoes WHERE status IN ('traduzido','aproveitado')").fetchone()[0]
        pend_n= conn3.execute("SELECT COUNT(*) FROM traducoes WHERE status='pendente'").fetchone()[0]
        conn3.close()

        print(f"  DB3 criado: {total} total · {trad} traduzidas · {pend_n} pendentes"
              + (f" · {n_patch} do patch" if n_patch else ""))

    except Exception as e:
        print(f"  [ERRO] _auto_criar_db3: {e}")
        import traceback; traceback.print_exc()