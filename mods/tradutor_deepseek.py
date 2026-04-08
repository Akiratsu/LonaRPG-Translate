"""
tradutor_deepseek.py
====================
Tradutor LonaRPG usando a API da DeepSeek.

Ideal para conteúdo adulto que o filtro do GPT bloqueia.
Também é significativamente mais barato.

Chave armazenada em config.json  →  "sk-b15d5d95399d4519a52913448a285fbd"

Uso:
  python tradutor_deepseek.py                        traduz todos os pendentes
  python tradutor_deepseek.py --teste 5              traduz só 5 entradas (teste)
  python tradutor_deepseek.py --modelo deepseek-reasoner
  python tradutor_deepseek.py --reenviar-erros       reenvia entradas com erro
  python tradutor_deepseek.py --ids 12,34,56         reenvia IDs específicos

Modelos disponíveis:
  deepseek-chat      (padrão — rápido e barato)
  deepseek-reasoner  (mais lento, melhor qualidade)
"""

import sys
import argparse
from tradutor_core import (
    log,
    carregar_config, pedir_api_key,
    traduzir, reenviar_erros, mostrar_stats,
)

# ──────────────────────────────────────────────────────────────
# CONSTANTES — DeepSeek
# ──────────────────────────────────────────────────────────────

_PROVEDOR   = "DeepSeek"
_CAMPO_KEY  = "api_key_deepseek"
_MODELO_PAD = "deepseek-chat"
_BASE_URL   = "https://api.deepseek.com/v1/chat/completions"


def _montar_cfg(args) -> dict:
    """
    Lê config.json, resolve a chave DeepSeek e aplica os overrides do CLI.
    Retorna cfg pronto para passar às funções do core.
    """
    cfg = carregar_config()

    api_key = (cfg.get("api_key_deepseek") or "").strip()

    if not api_key:
        api_key = pedir_api_key(_PROVEDOR, _CAMPO_KEY)
        if not api_key:
            log.error("  Chave DeepSeek obrigatória. Abortando.")
            sys.exit(1)

    # Modelo: CLI > config > padrão
    modelo = (args.modelo or _MODELO_PAD).strip()

    cfg["api_key"]    = api_key
    cfg["modelo"]     = modelo
    cfg["base_url"]   = _BASE_URL
    cfg["fonte_lang"] = (args.fonte or "auto").strip()
    return cfg


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="LonaRPG — Tradutor DeepSeek")
    parser.add_argument("--teste",          type=int, metavar="N",
                        help="Traduz apenas N entradas (modo teste)")
    parser.add_argument("--modelo",         type=str,
                        help="Modelo DeepSeek (ex: deepseek-chat, deepseek-reasoner)")
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
    log.info("  TRADUTOR DEEPSEEK")
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
    log.info("  CONCLUÍDO — DeepSeek")
    log.info(f"  Traduzidos : {traduzidos}")
    log.info(f"  Erros      : {erros}")
    log.info("=" * 60)

    mostrar_stats()


if __name__ == "__main__":
    main()
