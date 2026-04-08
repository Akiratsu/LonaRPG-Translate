# LonaRPG Translator PT-BR

Uma ferramenta para traduzir textos de jogos RPG do inglês para português brasileiro, utilizando modelos de IA como GPT, DeepSeek e Google Translate.

## Funcionalidades

- Extração de textos de arquivos de jogo
- Tradução automática com preservação de tags de formatação
- Interface web para gerenciamento de traduções
- Suporte a múltiplos provedores de IA
- Banco de dados SQLite para armazenamento de textos e traduções

## Estrutura do Projeto

- `servidor.py`: Servidor Flask que conecta o frontend aos bancos de dados
- `tradutor_core.py`: Lógica principal de tradução com sistema de proteção (shield)
- `tradutor_*.py`: Módulos específicos para cada provedor de IA
- `extrator.py` / `extrator_db2.py`: Extração de textos dos arquivos de jogo
- `gerador_arquivos.py`: Geração dos arquivos traduzidos
- `atualizador.py`: Atualização do banco de dados
- `LonaTranslator.html`: Interface web React
- `config.json`: Configurações (chaves de API, modelo, etc.)
- `database/`: Bancos de dados SQLite
- `PT-BRC/`: Arquivos traduzidos

## Instalação

1. Instale o Python 3.7 ou superior
2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

## Configuração

1. Edite o arquivo `config.json`:
   ```json
   {
     "api_key": "sua-chave-openai",
     "api_key_openai": "sua-chave-openai",
     "api_key_deepseek": "sua-chave-deepseek",
     "modelo": "gpt-4o-mini",
     "base_url": "https://api.openai.com/v1/chat/completions",
     "lote_size": 20,
     "temperatura": 0.3,
     "timeout": 60,
     "max_retries": 3
   }
   ```

2. Configure as chaves de API dos provedores desejados

## Uso

1. Execute o servidor:
   ```bash
   python servidor.py
   ```
   Isso abrirá automaticamente o navegador na interface web.

2. Alternativamente, especifique porta ou desabilite abertura automática:
   ```bash
   python servidor.py --porta 5001
   python servidor.py --sem-browser
   ```

3. Na interface web, você pode:
   - Visualizar textos extraídos
   - Gerenciar traduções
   - Exportar arquivos traduzidos

## Sistema de Proteção (Shield)

O projeto utiliza um sistema avançado de proteção de tags durante a tradução:

- Tags de formatação são separadas do texto
- Apenas o conteúdo textual é enviado para tradução
- Tags são reinseridas nas posições corretas após tradução
- Preserva 100% a integridade da formatação

## Dependências

- Flask: Servidor web
- SQLite3: Banco de dados (incluído no Python)
- Outras bibliotecas padrão do Python

## Logs

Os logs são salvos em arquivos como:
- `db1_extrator.log`
- `db2_extrator.log`
- `db3_tradutor.log`
- etc.

## Troubleshooting

- **Erro de Flask não instalado**: Execute `pip install flask`
- **Problemas com API**: Verifique as chaves no `config.json`
- **Timeout**: Ajuste o `timeout` no config
- **Erros de tradução**: Verifique logs para detalhes

## Desenvolvimento

O projeto é desenvolvido em Python com frontend em React (via Babel standalone).

Para contribuir:
1. Faça fork do repositório
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Abra um pull request

## Licença

[Adicione licença se aplicável]</content>
<parameter name="filePath">f:\LonaRPG\Text\mods\README.md