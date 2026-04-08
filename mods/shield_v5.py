"""
shield_v5.py — Novo módulo de blindagem de tags para LonaRPG
============================================================
Substitui o shield/restore do tradutor_core.py.

Abordagem: extrai textos puros como lista, preserva tags por posição.
A API recebe APENAS texto humano — tags são 100% intocáveis.

Integração:
  from shield_v5 import shield_v5 as shield, restore_v5 as restore

Compatível com Google Translate (translate_batch) e GPT/DeepSeek (JSON list).
"""

import re
import json
from typing import Tuple, List

# Regex que captura QUALQUER tag do LonaRPG
_TAG_RE = re.compile(
    r'\\board\[[^\]]*\]'         # \board[titulo]
    r'|\\optB\[[^\]]*\]'         # \optB[a,b]
    r'|\\optD\[[^\]]*\]'         # \optD[a,b]
    r'|\\[A-Za-z_]+\[[^\]]*\]'   # \CBmp[...], \C[6], \SETpl[...], etc
    r'|\\[A-Za-z_]+'             # \n, \Lshake, \prf, \PRF, etc
    r'|\\\\'                     # \\
)

_PLACEHOLDER = '\x00'


def shield_v5(texto: str) -> Tuple[List[str], list, list]:
    """
    Extrai textos puros e guarda estrutura com tags.

    Retorna:
        textos    — lista de strings puras para traduzir
        estrutura — lista de (tipo, valor):
                    ('texto', idx_em_textos)
                    ('tag',   idx_em_tags)
                    ('espaco', ' ')
        tags      — lista de tags originais na ordem de aparição
    """
    tags = []

    def sub(m):
        tags.append(m.group(0))
        return f'{_PLACEHOLDER}{len(tags)-1}{_PLACEHOLDER}'

    texto_limpo = _TAG_RE.sub(sub, texto)

    # Divide em partes: [texto, idx_tag, texto, idx_tag, ...]
    partes = re.split(r'\x00(\d+)\x00', texto_limpo)

    textos = []
    estrutura = []

    for i, parte in enumerate(partes):
        if i % 2 == 0:  # segmento de texto
            stripped = parte.strip()
            if stripped:
                estrutura.append(('texto', len(textos)))
                textos.append(stripped)
            elif parte:   # espaço/newline entre tags
                estrutura.append(('espaco', parte))
        else:            # índice de tag
            estrutura.append(('tag', int(parte)))

    return textos, estrutura, tags


def restore_v5(traduzidos: List[str], estrutura: list, tags: list) -> str:
    """
    Reconstrói o texto com traduções e tags originais.
    Se a lista de traduzidos tiver tamanho errado, usa original.
    """
    resultado = []
    for tipo, valor in estrutura:
        if tipo == 'texto':
            t = traduzidos[valor] if valor < len(traduzidos) else ''
            resultado.append(t)
        elif tipo == 'tag':
            resultado.append(tags[valor] if valor < len(tags) else '')
        else:  # espaco
            resultado.append(valor)
    return ''.join(resultado)


def validar_v5(textos_orig: List[str], traduzidos: List[str]) -> bool:
    """Verifica se a lista traduzida tem o mesmo tamanho da original."""
    return len(textos_orig) == len(traduzidos)


# ── Compat: drop-in replacement para shield/restore do tradutor_core ──────────

def shield(texto: str) -> Tuple[str, dict]:
    """
    Compatível com a assinatura do shield original:
    retorna (texto_para_api, mapa_para_restore)

    O texto_para_api é JSON dos textos puros.
    O mapa contém estrutura e tags para restaurar.
    """
    textos, estrutura, tags = shield_v5(texto)
    # Serializa como JSON compacto para passar como string única
    payload = json.dumps(textos, ensure_ascii=False, separators=(',', ':'))
    mapa = {'__v5__': True, 'estrutura': estrutura, 'tags': tags,
            'textos_orig': textos}
    return payload, mapa


def restore(traduzido: str, mapa: dict) -> str:
    """
    Compatível com a assinatura do restore original.
    traduzido deve ser JSON list de strings traduzidas.
    """
    if not mapa.get('__v5__'):
        # Fallback para mapa v4 (tokens ❰N❱)
        for token in sorted(mapa.keys(),
                            key=lambda t: -int(t[1:-1]) if t.startswith('❰') else 0):
            if token != '__v5__':
                traduzido = traduzido.replace(token, mapa[token])
        return traduzido

    try:
        textos_trad = json.loads(traduzido)
        if not isinstance(textos_trad, list):
            raise ValueError("resposta não é lista JSON")
    except Exception:
        # API não retornou JSON — tenta usar o texto como está
        return traduzido

    if not validar_v5(mapa['textos_orig'], textos_trad):
        # Tamanho diferente — usa originais (falha segura)
        textos_trad = mapa['textos_orig']

    return restore_v5(textos_trad, mapa['estrutura'], mapa['tags'])


# ── Prompt helper para GPT/DeepSeek ──────────────────────────────────────────

def prompt_sistema_v5(idioma_destino: str = "português brasileiro") -> str:
    return (
        f"Você é um tradutor especializado em RPG. "
        f"Receberá uma lista JSON de strings em inglês ou chinês. "
        f"Traduza cada string para {idioma_destino} e retorne APENAS "
        f"uma lista JSON com o mesmo número de elementos, na mesma ordem. "
        f"Não adicione explicações, não combine elementos, não omita nenhum. "
        f"Preserve nomes próprios de personagens e itens do jogo."
    )


def prompt_usuario_v5(textos: List[str]) -> str:
    return json.dumps(textos, ensure_ascii=False)


# ── Teste rápido ──────────────────────────────────────────────────────────────
if __name__ == '__main__':
    teste = (
        r'\board[G8Demon King Challenge] Hello, my warriors. Welcome to the '
        r'\C[2]G8Demon King Challenge\C[0]. Within \C[6]10\C[0] minutes, '
        r'you must equip yourself and take down the kings. \n Good luck.'
    )

    textos, estrutura, tags = shield_v5(teste)

    print("=== TEXTOS PARA TRADUZIR ===")
    for i, t in enumerate(textos):
        print(f"  [{i}] {t!r}")

    print(f"\n=== TAGS PRESERVADAS ({len(tags)}) ===")
    for i, tag in enumerate(tags):
        print(f"  [{i}] {tag!r}")

    # Simula tradução
    traduzidos = [
        "Olá, meus guerreiros. Bem-vindos ao",
        "Desafio do Rei G8",
        ". Dentro de",
        "10",
        "minutos, vocês devem se equipar e derrubar os reis.",
        "Boa sorte."
    ]

    resultado = restore_v5(traduzidos, estrutura, tags)
    print(f"\n=== RESULTADO ===")
    print(resultado)

    print("\n=== PROMPT SISTEMA ===")
    print(prompt_sistema_v5())

    print("\n=== PROMPT USUÁRIO ===")
    print(prompt_usuario_v5(textos))
