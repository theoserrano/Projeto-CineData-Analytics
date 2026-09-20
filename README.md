# Projeto CineData Analytics
Este documento detalha a arquitetura, as decisões técnicas e os diferenciais implementados no pipeline ETL do projeto CineData Analytics. Desenvolvido em Databricks (PySpark, Spark SQL e Delta Lake), este projeto segue a Arquitetura Medallion

O pipeline foi desenhado com foco em Resiliência a Anomalias, Governança de Dados (Data Lineage e Quality Checks) e Otimização para Business Intelligence (BI) e Inteligência Artificial (GenAI/RAG).

# Resumo dos Diferenciais Arquiteturais
Governança e Rastreabilidade (O respeito à Data Lineage): O carimbo de tempo ingestion_datetime criado na Bronze é tratado como um registo histórico imutável. Ao contrário de práticas comuns que sobrescrevem esta data na Silver, o nosso pipeline utiliza-a exclusivamente para desduplicação (via Window Functions), preservando a linhagem exata de quando o dado bruto aterrou no Data Lake.

Tratamento Defensivo contra Column Shift (O Problema do Ano/Popularidade): A base de origem continha falhas graves de deslocamento de colunas (textos numéricos a invadir colunas vizinhas). Implementamos funções nativas (F.expr("try_cast(...)")) e lógicas de Regex específicas para anular (NULL) valores matematicamente válidos mas contextualmente errados (ex.: o ano "1969" lido incorretamente como índice de "Popularidade"), garantindo um ranking de BI limpo e real, um erro comum que quebra análises de mercado.

Framework de Data Quality Auditável: Em vez de omitir dados inconsistentes silenciosamente, o pipeline possui funções de auditoria (dq_check_unique, dq_check_condition). Os resultados (PASS/FAIL) não ficam apenas no ecrã, podem ser persistidos na Silver, criando métricas de observabilidade para a equipa de Engenharia de Dados.

Otimização Física Delta (Performance e Custos): A finalização da camada Silver inclui rotinas de manutenção OPTIMIZE e ZORDER BY (id_filme). Isto agrupa e indexa fisicamente os ficheiros Parquet, ativando o Data Skipping e reduzindo drasticamente o consumo computacional nos Joins massivos da camada Gold.

Geração Imutável de Surrogate Keys (SK): Na modelagem Star Schema, não dependemos de funções frágeis como monotonically_increasing_id (que gera gaps em processamento distribuído). As SKs são geradas via Hash Criptográfico (SHA-256), garantindo idempotência absoluta.

Tratamento "Anti-Nulos" para IA (O desafio GenAI): Para o prompt longo da base vetorial, utilizámos concat_ws aliado a coalesce para garantir que campos vitais nulos (ex.: "sem diretor") geram fallbacks gramaticais perfeitos ("diretor não especificado") em vez de corromperem e anularem todo o contexto do filme no LLM.

01. Camada Bronze

Decisões Técnicas Célula a Célula:
Setup e Validação Inicial: Criação dos schemas no Unity Catalog (workspace.bronze, etc.). A leitura inclui escape='"' e multiLine=True (essencial, pois sinopses e reviews contêm quebras de linha internas que destruiriam a tabela se lidas com a configuração padrão).

Imutabilidade do Ingestion Datetime: A coluna ingestion_datetime é inserida em Lazy Evaluation no momento exato do write, carimbando o histórico real. Decisão: Este timestamp reflete a extração; jamais é atualizado nas camadas seguintes, protegendo a auditoria (Data Lineage).

Resiliência na API do BACEN: Parametrização via dbutils.widgets para consumo dinâmico de datas, com lógica de fallback nativa (calcula os últimos 7 dias via datetime) para que o Job automatizado nunca quebre se a data de entrada estiver em branco.

02. Camada Silver
   
Decisões Técnicas Célula a Célula:
O Framework de Data Quality: Definição das funções de Quality Gates antes do processamento.

Tabela tb_info_filmes (Datas Defensivas): Utilização de F.try_to_date num F.coalesce. O PySpark testa múltiplas máscaras de data na mesma coluna, transformando os casos impossíveis em NULL silenciosamente sem abortar a execução.

A Regra de Desduplicação Inteligente: Utilização de Window Functions ordenadas por ingestion_datetime (descendente) para identificar e manter apenas a linha mais recente de um filme duplicado.

Tabela tb_cotacao_dolar (Continuidade de Série): Como a API não funciona aos fins de semana, gerámos um calendário contínuo (Join) e utilizámos uma Window Function com F.last(ignorenulls=True) (Forward Fill), assegurando que não existam "buracos" financeiros ao cruzar as moedas.

Tabela tb_metricas_engajamento (O Tratamento do Column Shift - Ponto Crítico):

O Problema: Dados textuais (anos, strings) vazaram para colunas numéricas devido a separadores extras na origem. Um .cast() comum resultaria num erro fatal (CAST_INVALID_INPUT).

A Solução Híbrida: Utilização de F.expr("try_cast(coluna AS INT)") para forçar strings a nulo.

A "Falsa Popularidade" Resolvida: Identificámos que o ano de lançamento ("2020", "1969") empurrou-se para a coluna popularity. Sendo um número matematicamente válido, o try_cast não detetava o erro, o que colocaria blockbusters fora do top 5. Implementámos uma regra de Regex (F.col("popularity").rlike(r"^(18|19|20)\d{2}$")) para intercetar especificamente os anos perdidos e convertê-los em NULL. O Top de popularidade tornou-se real e confiável.

Normalização de Gêneros e Pessoas (tb_generos e tb_pessoas_empresas): O uso rigoroso de split, explode, e a normalização via F.initcap() com F.trim(). Decisão: Não foram removidos apóstrofos (ex.: O'Connor) nem hífens, preservando a exatidão das identidades.

Manutenção de Perfomance (OPTIMIZE e ZORDER): Célula final dedicada a aplicar Vacuum/Optimize e agrupar ficheiros por id_filme, otimizando e barateando a leitura das tabelas para o próximo nível (Gold).

03. Camada Gold

Decisões Técnicas Célula a Célula:
Idempotência e Performance de Chaves (SHA-256): Em vez das chaves nativas do Spark, que criam inconsistências em atualizações incrementais, todas as SKs (Surrogate Keys) das dimensões (dim_movies, dim_genres, dim_people, dim_companies) foram geradas via hex(sha2(id, 256)), garantindo que a mesma string gere sempre o mesmo ID imutável.

Métricas Agregadas na Dimensão (dim_reviews): Aplicação das regras de negócio (contagem e média) já na dimensão, evitando que o painel de BI tenha de processar milhões de reviews em tempo real.

O Isolamento Aditivo (Tabelas Bridge): O BI exige que a tabela Fato seja "aditiva". Para evitar que a bilheteira do "Spider-Man" fosse somada 50 vezes (uma por cada ator do filme), isolámos os relacionamentos "1 para N" nas três Bridge Tables (bridge_movie_person, bridge_movie_genre, bridge_movie_company). O Grão da fato (fact_movies_performance) mantém-se estritamente 1 linha = 1 filme.

Tipagem Estrita Financeira na Fato: Regra de negócio obrigatória cumprida: Todas as 6 métricas monetárias (BRL e USD) receberam .cast("decimal(18,2)") na tabela Fato, blindando os cálculos contra problemas de precisão de flutuação (float precision loss).

A Entrega para Inteligência Artificial (gold_genai_movies_context):

Para o LLM não ler campos vazios e "quebrar" todo o contexto do RAG, não utilizámos a simples concatenação.

Empregámos agregadores como F.concat_ws com F.collect_list para agrupar múltiplos atores, juntamente com F.coalesce para textos descritivos. Isto garante que, se um filme não tiver realizador registado, a IA recebe a frase gramaticalmente correta "diretor não especificado", sem perder a faturação ou a sinopse.

Resultado: O pipeline ETL não é apenas um script de movimentação de dados. É um produto de dados tolerante a falhas, otimizado para a cloud e perfeitamente ajustado para responder com exatidão tanto a métricas de gestão como a inferências de Inteligência Artificial.
