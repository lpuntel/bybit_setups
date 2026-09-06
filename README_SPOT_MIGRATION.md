# Bybit Spot Grid — migração do projeto Futures

## O que foi preservado
- Setups Larry 9.1–9.4 e PC do `bybit_setups_script_hr.py`
- MME9, MMA21, ATR, slope, swing
- Universo dinâmico
- Turnover, spread e depth
- Excel de saída
- Monitor de gatilho e Telegram
- Otimizador ATR/SL/TP como base

## O que mudou
- `category=spot`
- VENDA não vira short: vira `AGUARDAR` ou `SAIR/NAO_ABRIR`
- Funding, funding z-score, leverage, margem isolada e liquidação foram removidos
- Grid mínimo considera fee de compra + fee de venda
- `TS_RETRACAO_PCT` é retração da equity do Spot Grid
- `TRAILING_UP` é sugerido em regime de alta
- SL sempre abaixo do limite inferior e TP acima do limite superior

## Execução
1. `python bybit_setups_script_hr_context_spot.py --timeframe 240`
2. Ajuste `ativos_spot.xlsx`
3. `python bybit_setups_script_hr_spot.py`
4. Copie as oportunidades desejadas para `monitor_gatilho_grid_spot.xlsx`
5. `python monitor_gatilho_grid_spot.py --once`

Para diário use `D`. Por compatibilidade, 1440 e 1444 também são convertidos para `D`.

## Importante
Os scripts novos preservam os arquivos Futures antigos; não os sobrescrevem.
