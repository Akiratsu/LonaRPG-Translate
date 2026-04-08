@echo off
setlocal
title LonaRPG Translator PT-BR
color 0B
cd /d "%~dp0"

:menu
cls
echo.
echo  =====================================================
echo   LonaRPG Translator PT-BR
echo  =====================================================
echo.

set DB1=[pendente]
set DB2=[pendente]
set DB2B=[pendente]
set DB3=[pendente]
set PTBR=[pendente]
if exist "mods\database\db1_estrutura.sqlite"  set DB1=[OK]
if exist "mods\database\db2_dialogos.sqlite"   set DB2=[OK]
if exist "mods\database\db2b_fila.sqlite"      set DB2B=[OK]
if exist "mods\database\db3_traducao.sqlite"   set DB3=[OK]
if exist "PT-BR\"                         set PTBR=[OK]

echo   Status:
echo     DB1   Estrutura CHT      %DB1%
echo     DB2   Dialogos todos     %DB2%
echo     DB2b  Fila API           %DB2B%
echo     DB3   Traducao PT-BR     %DB3%
echo     PT-BR Arquivos gerados   %PTBR%
echo.
echo   Coloque as pastas aqui ao lado:
echo     CHT\  ENG\  KOR\  RUS\  UKR\  PT-BRC\
echo.
echo   -----------------------------------------------
echo   [1]  Extrair estrutura CHT          (DB1)
echo   [2]  Atualizar DB1 apos patch
echo   [3]  Extrair dialogos todos idiomas (DB2 + DB2b)
echo   [4]  Traduzir via API               (DB3)
echo   [5]  Revisar traducoes              (Interface HTML)
echo   [6]  Gerar arquivos finais          (PT-BR\)
echo        [6S] Simular  [6R] Relatorio  [6A] Arquivo especifico
echo   -----------------------------------------------
echo   [0]  Sair
echo.
set /p OPCAO=   Escolha: 

if "%OPCAO%"=="1"  goto op1
if "%OPCAO%"=="2"  goto op2
if "%OPCAO%"=="3"  goto op3
if "%OPCAO%"=="4"  goto op4
if "%OPCAO%"=="5"  goto op5
if "%OPCAO%"=="6"  goto op6
if "%OPCAO%"=="6S" goto op6s
if "%OPCAO%"=="6R" goto op6r
if "%OPCAO%"=="6A" goto op6a
if "%OPCAO%"=="0"  goto sair
echo   Opcao invalida.
timeout /t 1 >nul
goto menu


:: ─────────────────────────────────────────────────────────────
:op1
cls & echo.
python mods\extrator.py
echo. & pause & goto menu


:: ─────────────────────────────────────────────────────────────
:op2
cls & echo.
echo  [2A] Simular (ver o que mudaria sem aplicar)
echo  [2B] Aplicar mudancas
echo.
set /p SUB=   Escolha 2A ou 2B: 
if /i "%SUB%"=="2A" python mods\atualizador.py --dry-run
if /i "%SUB%"=="2B" python mods\atualizador.py
echo. & pause & goto menu


:: ─────────────────────────────────────────────────────────────
:op3
cls & echo.
python mods\extrator_db2.py
echo. & pause & goto menu


:: ─────────────────────────────────────────────────────────────
:: [4] TRADUZIR — escolha o provedor primeiro
:: ─────────────────────────────────────────────────────────────
:op4
cls
echo.
echo  =====================================================
echo   [4] Traduzir via API — Escolha o Provedor
echo  =====================================================
echo.
echo   [G]  GPT  (OpenAI)
echo          Melhor qualidade geral
echo          Chave: api_key_openai  em config.json
echo.
echo   [D]  DeepSeek
echo          Sem filtro de conteudo adulto, mais barato
echo          Chave: api_key_deepseek  em config.json
echo.
echo   [O] Google Translate
echo          Gratuito, sem chave necessaria
echo          Requer:  pip install deep-translator
echo.
echo   [0]  Voltar ao menu principal
echo.
set /p PROV=   Provedor (G/D/O): 

if /i "%PROV%"=="G"  goto op4_gpt_menu
if /i "%PROV%"=="D"  goto op4_ds_menu
if /i "%PROV%"=="O" goto op4_gl_menu
if /i "%PROV%"=="0"  goto menu
goto op4


:: ── GPT — submenu ────────────────────────────────────────────
:op4_gpt_menu
cls
echo.
echo  ── GPT (OpenAI) ─────────────────────────────────────
echo.
echo   [T]  Teste        (5 entradas)
echo   [A]  Traduzir tudo
echo   [R]  Reenviar erros
echo.
echo   [0]  Voltar
echo.
set /p ACT=   Acao: 
if /i "%ACT%"=="T" goto op4_gpt_teste
if /i "%ACT%"=="A" goto op4_gpt_tudo
if /i "%ACT%"=="R" goto op4_reenviar
if /i "%ACT%"=="0" goto op4
goto op4_gpt_menu

:op4_gpt_teste
cls & echo.
echo  [GPT] Modo teste — 5 entradas...
echo.
python mods\tradutor_gpt.py --teste 5
echo. & pause & goto menu

:op4_gpt_tudo
cls & echo.
echo  [GPT] Traduzindo todos os pendentes...
echo.
python mods\tradutor_gpt.py
echo. & pause & goto menu


:: ── DeepSeek — submenu ───────────────────────────────────────
:op4_ds_menu
cls
echo.
echo  ── DeepSeek ─────────────────────────────────────────
echo.
echo   [T]  Teste        (5 entradas)  deepseek-chat
echo   [A]  Traduzir tudo              deepseek-chat
echo   [Q]  Traduzir tudo              deepseek-reasoner (melhor qualidade)
echo   [R]  Reenviar erros
echo.
echo   [0]  Voltar
echo.
set /p ACT=   Acao: 
if /i "%ACT%"=="T" goto op4_ds_teste
if /i "%ACT%"=="A" goto op4_ds_tudo
if /i "%ACT%"=="Q" goto op4_ds_reasoner
if /i "%ACT%"=="R" goto op4_reenviar
if /i "%ACT%"=="0" goto op4
goto op4_ds_menu

:op4_ds_teste
cls & echo.
echo  [DeepSeek] Modo teste — 5 entradas...
echo.
python mods\tradutor_deepseek.py --teste 5
echo. & pause & goto menu

:op4_ds_tudo
cls & echo.
echo  [DeepSeek] Traduzindo todos os pendentes...
echo.
python mods\tradutor_deepseek.py
echo. & pause & goto menu

:op4_ds_reasoner
cls & echo.
echo  [DeepSeek] Traduzindo com deepseek-reasoner (melhor qualidade)...
echo.
python mods\tradutor_deepseek.py --modelo deepseek-reasoner
echo. & pause & goto menu


:: ── Google Translate — submenu ───────────────────────────────
:op4_gl_menu
cls
echo.
echo  ── Google Translate ─────────────────────────────────
echo.
echo   [T]  Teste        (5 entradas)
echo   [A]  Traduzir tudo
echo   [R]  Reenviar erros
echo.
echo   [0]  Voltar
echo.
set /p ACT=   Acao: 
if /i "%ACT%"=="T" goto op4_gl_teste
if /i "%ACT%"=="A" goto op4_gl_tudo
if /i "%ACT%"=="R" goto op4_reenviar
if /i "%ACT%"=="0" goto op4
goto op4_gl_menu

:op4_gl_teste
cls & echo.
echo  [Google] Modo teste — 5 entradas...
echo.
python mods\tradutor_google.py --teste 5
echo. & pause & goto menu

:op4_gl_tudo
cls & echo.
echo  [Google] Traduzindo todos os pendentes...
echo.
python mods\tradutor_google.py
echo. & pause & goto menu


:: ── Reenviar erros — escolha o provedor ─────────────────────
:op4_reenviar
cls
echo.
echo  ── Reenviar Erros — Qual provedor usar? ─────────────
echo.
echo   [G]  GPT             (bom para timeouts e erros gerais)
echo   [D]  DeepSeek        (bom para conteudo adulto bloqueado)
echo   [O] Google Translate (gratuito, sem chave)
echo.
echo   [0]  Voltar
echo.
set /p RPROV=   Provedor (G/D/O): 
if /i "%RPROV%"=="G"  goto op4_rev_gpt
if /i "%RPROV%"=="D"  goto op4_rev_ds
if /i "%RPROV%"=="O" goto op4_rev_gl
if /i "%RPROV%"=="0"  goto op4
goto op4_reenviar

:op4_rev_gpt
cls & echo.
echo  [GPT] Reenviando entradas com erro...
echo.
python mods\tradutor_gpt.py --reenviar-erros
echo. & pause & goto menu

:op4_rev_ds
cls & echo.
echo  [DeepSeek] Reenviando entradas com erro...
echo.
python mods\tradutor_deepseek.py --reenviar-erros
echo. & pause & goto menu

:op4_rev_gl
cls & echo.
echo  [Google] Reenviando entradas com erro...
echo.
python mods\tradutor_google.py --reenviar-erros
echo. & pause & goto menu


:: ─────────────────────────────────────────────────────────────
:op5
cls & echo.
echo  =====================================================
echo   Revisar Traducoes - Interface com Backend
echo  =====================================================
echo.

if not exist "mods\servidor.py" (
    echo  [ERRO] servidor.py nao encontrado nesta pasta.
    echo  Coloque o arquivo servidor.py em: %CD%\mods\
    echo.
    pause & goto menu
)
if not exist "mods\LonaTranslator.html" (
    echo  [ERRO] LonaTranslator.html nao encontrado nesta pasta.
    echo.
    pause & goto menu
)

python -c "import flask" 2>nul
if errorlevel 1 (
    echo  [AVISO] Flask nao instalado. Instalando agora...
    echo.
    pip install flask
    if errorlevel 1 (
        echo.
        echo  [ERRO] Nao foi possivel instalar Flask.
        echo  Execute manualmente:  pip install flask
        echo.
        pause & goto menu
    )
    echo  Flask instalado com sucesso!
    echo.
)

echo  Iniciando servidor backend...
echo  URL: http://localhost:5001
echo.
echo  O navegador abrira automaticamente em instantes.
echo  Feche esta janela ou pressione Ctrl+C para parar o servidor.
echo.
python mods\servidor.py
echo.
echo  Servidor encerrado. Pressione qualquer tecla para voltar ao menu.
pause >nul
goto menu


:: ─────────────────────────────────────────────────────────────
:op6
cls & echo.
echo  =====================================================
echo   Gerar Arquivos Finais PT-BR
echo  =====================================================
echo.

if not exist "mods\database\db1_estrutura.sqlite" (
    echo  [ERRO] DB1 nao encontrado. Execute a opcao [1] primeiro.
    echo.
    pause & goto menu
)
if not exist "mods\database\db3_traducao.sqlite" (
    echo  [ERRO] DB3 nao encontrado. Execute a opcao [4] primeiro.
    echo.
    pause & goto menu
)

echo  O que deseja fazer?
echo.
echo  [6A] Gerar TODOS os arquivos           (grava em PT-BR\)
echo  [6S] Simular sem gravar               (dry-run)
echo  [6R] Ver relatorio de cobertura
echo  [6F] Gerar arquivo especifico
echo.
set /p SUB6=   Escolha: 
if /i "%SUB6%"=="6A" goto op6a_exec
if /i "%SUB6%"=="6S" goto op6s
if /i "%SUB6%"=="6R" goto op6r
if /i "%SUB6%"=="6F" goto op6a
goto op6

:op6a_exec
cls & echo.
echo  Gerando todos os arquivos em PT-BR\ ...
echo.
python mods\gerador_arquivos.py
echo. & pause & goto menu

:op6s
cls & echo.
echo  Simulando geracao (dry-run) - nenhum arquivo sera gravado...
echo.
python mods\gerador_arquivos.py --dry-run
echo. & pause & goto menu

:op6r
cls & echo.
echo  Relatorio de cobertura de traducao...
echo.
python mods\gerador_arquivos.py --relatorio
echo. & pause & goto menu

:op6a
cls & echo.
echo  Exemplos: menu.txt  common.txt  CompElise.txt
echo.
set /p ARQNOME=   Nome do arquivo: 
if "%ARQNOME%"=="" goto menu
cls & echo.
echo  Gerando arquivo: %ARQNOME%
echo.
python mods\gerador_arquivos.py --arquivo "%ARQNOME%"
echo. & pause & goto menu


:: ─────────────────────────────────────────────────────────────
:sair
exit /b 0