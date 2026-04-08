"""
tradutor_gpt.py
===============
Tradutor LonaRPG usando a API da OpenAI (GPT).

Chave armazenada em config.json  →  "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"

Uso:
  python tradutor_gpt.py                   traduz todos os pendentes
  python tradutor_gpt.py --teste 5         traduz só 5 entradas (teste)
  python tradutor_gpt.py --modelo gpt-4o   usa modelo específico
  python tradutor_gpt.py --reenviar-erros  reenvia entradas com erro
  python tradutor_gpt.py --ids 12,34,56    reenvia IDs específicos

Modelos disponíveis:
  gpt-4o-mini  (padrão — rápido e barato)
  gpt-4o       (mais preciso)
  gpt-4-turbo
"""

import sys
import argparse
from tradutor_core import (
    log,
    carregar_config, pedir_api_key,
    traduzir, reenviar_erros, mostrar_stats,
)

# ──────────────────────────────────────────────────────────────
# CONSTANTES — OpenAI
# ──────────────────────────────────────────────────────────────

_PROVEDOR   = "OpenAI"
_CAMPO_KEY  = "api_key_openai"
_MODELO_PAD = "gpt-4o-mini"
_BASE_URL   = "https://api.openai.com/v1/chat/completions"


def _montar_cfg(args) -> dict:
    """
    Lê config.json, resolve a chave OpenAI e aplica os overrides do CLI.
    Retorna cfg pronto para passar às funções do core.
    """
    cfg = carregar_config()

    # Chave: campo dedicado, com fallback no campo legado 'api_key'
    api_key = (cfg.get("api_key_openai") or cfg.get("api_key") or "").strip()

    if not api_key:
        api_key = pedir_api_key(_PROVEDOR, _CAMPO_KEY)
        if not api_key:
            log.error("  Chave OpenAI obrigatória. Abortando.")
            sys.exit(1)

    # Modelo: CLI > config > padrão
    modelo = (args.modelo or cfg.get("modelo") or _MODELO_PAD).strip()

    cfg["api_key"]    = api_key
    cfg["modelo"]     = modelo
    cfg["base_url"]   = _BASE_URL
    cfg["fonte_lang"] = (args.fonte or "auto").strip()
    return cfg


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="LonaRPG — Tradutor GPT (OpenAI)")
    parser.add_argument("--teste",          type=int, metavar="N",
                        help="Traduz apenas N entradas (modo teste)")
    parser.add_argument("--modelo",         type=str,
                        help="Modelo OpenAI (ex: gpt-4o-mini, gpt-4o)")
    parser.add_argument("--reenviar-erros", action="store_true",
                        help="Reenvia entradas com status='erro'")
    parser.add_argument("--ids",            type=str,
                        help="IDs para reenviar, separados por vírgula (ex: 12,34)")
    parser.add_argument("--fonte",          type=str, default="auto",
                        help="Idioma fonte: auto, eng, cht (default: auto)")
    args = parser.parse_args()

    cfg = _montar_cfg(args)

    log.info("")
    log.info("=" * 60)
    log.info("  TRADUTOR GPT — OpenAI")
    log.info(f"  Modelo  : {cfg['modelo']}")
    log.info(f"  Chave   : ***{cfg['api_key'][-4:]}")
    if args.teste:
        log.info(f"  MODO TESTE: apenas {args.teste} entradas")
    if args.reenviar_erros or args.ids:
        log.info("  MODO: reenvio de erros")
    log.info("=" * 60)
    log.info("")

    if args.reenviar_erros or args.ids:
        traduzidos, erros = reenviar_erros(cfg, ids=args.ids)
    else:
        traduzidos, erros = traduzir(cfg, limite=args.teste)

    log.info("")
    log.info("=" * 60)
    log.info("  CONCLUÍDO — GPT")
    log.info(f"  Traduzidos : {traduzidos}")
    log.info(f"  Erros      : {erros}")
    log.info("=" * 60)

    mostrar_stats()


if __name__ == "__main__":
    main()
