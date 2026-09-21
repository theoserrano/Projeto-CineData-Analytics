# Projeto CineData Analytics

Este projeto apresenta um pipeline ETL desenvolvido em Databricks com PySpark, Spark SQL e Delta Lake, seguindo a Arquitetura Medallion e organizando os dados nas camadas Bronze, Silver e Gold.

Durante o desenvolvimento, meu foco foi construir um pipeline capaz de lidar com problemas reais de qualidade dos dados, mantendo a rastreabilidade e o desempenho necessários para aplicações de Business Intelligence e Inteligência Artificial.

# Resolução de casos de borda e anomalias

Durante o desenvolvimento, encontrei algumas inconsistências na base de origem do TMDB que poderiam gerar resultados analíticos incorretos. Para cada caso, precisei investigar a origem do problema e criar tratamentos específicos sem comprometer a informação original.

## Kevin Hart com 66 participações em 2 anos

Durante a análise de engajamento dos atores, Kevin Hart aparecia com cerca de 66 filmes em apenas dois anos. A investigação mostrou que o mesmo filme podia aparecer na origem com IDs diferentes, fazendo com que os registros fossem multiplicados nas tabelas Bridge e Fato.

Inicialmente, tentei remover dos títulos tudo o que aparecesse depois de dois pontos. Porém, isso poderia transformar continuações legítimas em um único registro. Por isso, optei por uma deduplicação baseada em `match_key` e `ano_lancamento`, utilizando uma Window Function ordenada por `ingestion_datetime`.

Assim, consigo eliminar clones gerados por múltiplos IDs e, ao mesmo tempo, preservar continuações, subtítulos e filmes diferentes da mesma franquia.

## Problema de ano de lançamento em popularidade

Durante a criação do ranking de popularidade, alguns filmes apareciam com valores como `2020`, `2019` e `2018`. A investigação mostrou que esses valores eram anos de lançamento que haviam sido deslocados para a coluna `popularity` devido a um problema de Column Shift na origem.

O problema é que esses valores continuavam sendo números válidos. Um simples `.cast("DOUBLE")` não identificaria o erro. Para tratar isso, utilizei `try_cast` junto com uma validação por expressão regular que identifica valores no formato de anos.

Dessa forma, consigo diferenciar valores realmente inválidos de valores que são válidos numericamente, mas estão incorretos no contexto da coluna.

# Resumo dos diferenciais arquiteturais

## Governança e rastreabilidade

Preservei o `ingestion_datetime` criado na Bronze como um registro histórico da chegada dos dados ao Data Lake. Esse valor não é sobrescrito nas camadas seguintes e é utilizado principalmente durante a deduplicação.

Isso permite identificar qual versão de um registro chegou mais recentemente sem perder a informação original de ingestão.

## Tratamento defensivo contra Column Shift

Para lidar com os problemas de deslocamento encontrados na origem, utilizei `try_cast` para valores que não podem ser convertidos e expressões regulares para identificar valores que são numericamente válidos, mas contextualmente incorretos.

Um exemplo é um ano como `1969` aparecendo na coluna `popularity`. Embora possa ser convertido normalmente para número, esse valor não representa uma métrica válida de popularidade.

## Framework de Data Quality

Criei funções como `dq_check_unique` e `dq_check_condition` para acompanhar a qualidade dos dados durante o processamento.

Os resultados são registrados como `PASS` ou `FAIL` e podem ser persistidos, permitindo acompanhar as condições de qualidade do pipeline e criar uma camada de observabilidade.

## Otimização física Delta

Ao final da Silver, utilizo `OPTIMIZE` e `ZORDER BY` para melhorar a organização física das tabelas Delta.

O `OPTIMIZE` reduz a fragmentação causada por arquivos pequenos, enquanto o `ZORDER` melhora o Data Skipping em consultas que utilizam `id_filme`, reduzindo a quantidade de dados lidos nos joins posteriores.

## Surrogate Keys com SHA-256

Na modelagem Star Schema, optei por gerar as Surrogate Keys utilizando SHA-256 em vez de funções como `monotonically_increasing_id`.

Com `hex(sha2(id, 256))`, a mesma chave de origem sempre produz o mesmo identificador. Isso garante estabilidade durante reprocessamentos e atualizações incrementais.

## Tratamento de nulos para GenAI

Na preparação dos dados para RAG, utilizei `coalesce`, `concat_ws` e `collect_list` para evitar que valores nulos comprometam o contexto enviado ao modelo.

Quando uma informação não está disponível, utilizo fallbacks como “diretor não especificado”, mantendo o restante das informações do filme disponível para o LLM.

# 01. Camada Bronze

Na Bronze, mantenho os dados o mais próximo possível da origem e realizo a ingestão inicial no Unity Catalog.

A leitura utiliza configurações como `escape='"'` e `multiLine=True`, necessárias para arquivos que possuem sinopses e avaliações com quebras de linha internas.

Também adiciono o `ingestion_datetime`, preservando o momento da chegada dos dados ao Data Lake. Esse valor permanece imutável nas etapas seguintes.

Para a API do BACEN, utilizo `dbutils.widgets` para parametrizar as datas. Caso nenhuma data seja informada, o pipeline utiliza automaticamente um período de sete dias, evitando falhas durante execuções automatizadas.

# 02. Camada Silver

Na Silver, concentro as principais etapas de limpeza, padronização, validação e deduplicação dos dados.

## `tb_info_filmes`

Utilizo `try_to_date` em conjunto com `coalesce` para testar diferentes formatos de data. Valores que não podem ser interpretados são transformados em `NULL` sem interromper o processamento.

## Deduplicação

Utilizo Window Functions ordenadas por `ingestion_datetime` para identificar diferentes versões do mesmo registro e manter a versão mais recente.

## `tb_cotacao_dolar`

Como a API do BACEN não fornece cotações em fins de semana e outros dias sem funcionamento, criei um calendário contínuo para evitar buracos na série.

Depois, utilizo `F.last(ignorenulls=True)` para realizar o Forward Fill e carregar a última cotação disponível para os dias sem novos registros.

## `tb_metricas_engajamento`

Para lidar com valores incompatíveis com os tipos esperados, utilizo `try_cast`, transformando entradas inválidas em `NULL`.

Além disso, aplico a validação por expressão regular na coluna `popularity` para identificar anos que foram deslocados para essa coluna. Assim, trato tanto erros de formato quanto erros de contexto.

## Normalização de gêneros e pessoas

Nas tabelas `tb_generos` e `tb_pessoas_empresas`, utilizo `split`, `explode`, `trim` e `initcap` para normalizar os dados.

Também preservo caracteres que fazem parte dos nomes, como apóstrofos e hífens, evitando alterar identidades como O'Connor e Jean-Pierre.

## Manutenção e otimização

Ao final da Silver, aplico `OPTIMIZE` e `ZORDER` para melhorar a organização física das tabelas e reduzir o volume de dados lido nas consultas posteriores.

# 03. Camada Gold

Na Gold, organizo os dados tratados em um Star Schema preparado para consumo analítico e ferramentas de BI.

## Surrogate Keys

As dimensões `dim_movies`, `dim_genres`, `dim_people` e `dim_companies` utilizam SHA-256 para gerar suas Surrogate Keys.

A mesma chave de origem sempre produz o mesmo identificador, garantindo estabilidade durante reprocessamentos.

## `dim_reviews`

As principais métricas das avaliações são pré-agregadas nessa dimensão, como quantidade de reviews e média das notas.

Assim, o BI não precisa processar milhões de registros de avaliações sempre que essas informações forem consultadas.

## Tabelas Bridge

Para evitar a multiplicação de registros da Fato, isolei os relacionamentos entre filmes, pessoas, gêneros e empresas nas tabelas `bridge_movie_person`, `bridge_movie_genre` e `bridge_movie_company`.

A `fact_movies_performance` mantém o grão de uma linha por filme. Dessa forma, a bilheteira de um filme com 50 atores continua sendo contabilizada apenas uma vez.

## Tipagem financeira

As métricas financeiras da Fato recebem `decimal(18,2)` para os valores em BRL e USD.

Isso evita problemas de precisão relacionados ao uso de ponto flutuante e torna os cálculos financeiros mais seguros no BI.

# Entrega para Inteligência Artificial

Uma das entregas finais é a tabela `gold_genai_movies_context`, criada para aplicações de GenAI e RAG.

Nessa etapa, transformo os dados estruturados dos filmes em um contexto textual utilizando `concat_ws`, `collect_list` e `coalesce`.

Os atores podem ser agrupados em uma única informação e os campos ausentes recebem fallbacks adequados. Assim, um filme sem diretor registrado pode receber “diretor não especificado” sem comprometer o restante do contexto.

# Resultado final

O CineData Analytics foi desenvolvido para ser mais do que um pipeline de transformação de dados. A estrutura foi pensada para lidar com problemas reais da origem, como duplicidades, Column Shift, valores inconsistentes e campos ausentes.

Com a Arquitetura Medallion, as regras de Data Quality, a preservação da linhagem e as otimizações do Delta Lake, consegui estruturar uma base preparada tanto para análises de BI quanto para aplicações de Inteligência Artificial e RAG.

