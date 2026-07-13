# DB2Connector

Conector **Trino → IBM Db2**, escrito à mão sobre a **SPI crua** do Trino. Ele expõe tabelas de um Db2 como um catálogo Trino e, quando
possível, **empurra** parte do trabalho (filtro, limit, top-N, agregação, join) para o Db2
executar — traduzindo o plano do Trino em SQL Db2 e lendo o resultado via JDBC.

O código-fonte fica em `demo/` (Maven). O artefato de deploy é um **shade** que vai
para a pasta `plugin/` de uma instalação Trino. **O conector é amarrado à versão do Trino**
(atualmente **482**).

---

## O que o conector faz

```
[SELECT do usuário]
      │
[Trino: parser + planner/otimizador]
      │
      ▼
┌───────────────────   CONECTOR    ───────────────────┐
│  Db2Metadata      Db2SplitManager   Db2RecordCursor │
│  (catálogo +      (divide o          (lê linha-a-   │
│   pushdown)   ──▶  trabalho)     ──▶  linha via JDBC)│──▶ [Db2]
└─────────────────────────────────────────────────────┘
```

**Pushdown é uma otimização opcional.** Se o conector não souber empurrar algo, o Trino
simplesmente puxa as linhas cruas pelo `Db2RecordCursor` e faz a operação **ele mesmo**. Os
métodos `applyX` do `Db2Metadata` existem só para deixar o Db2 fazer os filtros, assim tornando a consulta mais eficiente.

---

## Fluxo de uma query (ex.: `SELECT count(*) FROM "db2-dbhmlg".pppe.tbpessoa`)

```
getTableHandle ──▶ Db2TableHandle(tabela, sem filtro/limit/sql)
      │
applyAggregation  ← Trino oferece o count(*)
      │  monta:  SELECT COUNT(*) AS "AGG_0" FROM (SELECT * FROM "PPPE"."TBPESSOA") AS T_AGG
      │  e guarda essa string no campo sqlQuery de um Db2TableHandle NOVO
      ▼
getSplits  ──▶ 1 Db2Split carregando o handle (hoje: split único, sem paralelismo)
      │
Db2RecordCursor.buildSql  ──▶  SELECT "AGG_0" FROM ( <sqlQuery> ) AS T
      │
      ▼
   Db2 executa ──▶ cursor lê as linhas ──▶ Trino rotula a saída (_col0) ──▶ resultado
```

### O handle é um acumulador imutável

`Db2TableHandle` é **imutável** que carrega o estado da consulta. Cada pushdown
lê o handle que chegou, adiciona sua parte e devolve um handle **novo**:

| Campo do `Db2TableHandle` | Preenchido por | Vira, no SQL Db2 |
|---|---|---|
| `constraint` (TupleDomain) | `applyFilter` | `WHERE ...` |
| `limit` | `applyLimit` | `FETCH FIRST n ROWS ONLY` |
| `sortOrder` | `applyTopN` | `ORDER BY ...` |
| `isDistinct` | (leitura) | `SELECT DISTINCT` |
| `sqlQuery` (String) | `applyAggregation` / `applyJoin` | subquery inteira, aninhada |

Os aliases `AGG_0`, `AGG_1`… que aparecem no SQL empurrado são **Nomeações artificais** que o
`applyAggregation` coloca: o `COUNT(*)` não tem nome natural, e o Trino precisa de um nome
para se referir a cada agregado (via `projections` + `assignments`).

---

## Estrutura (`demo/src/main/java/com/connector/plugin/db2/`)

```
Db2Plugin.java            ponto de entrada; getConnectorFactories() → registra a fábrica
Db2ConnectorFactory.java  getName() == "db2" (é o connector.name do catálogo); create()
Db2Connector.java         amarra Metadata + SplitManager + RecordSetProvider
Db2ConnectionPool.java    pool HikariCP; lê connection-url/user/password do catálogo

Db2Metadata.java          O CÉREBRO:
   ├─ listSchemaNames / listTables / getTableMetadata / getColumnHandles   (catálogo, via SYSCAT)
   ├─ getTableHandle                                                       (resolve a tabela)
   ├─ applyFilter / applyLimit / applyTopN                                 (pushdowns simples)
   ├─ applyAggregation                                                     (pushdown de agregação)
   ├─ applyJoin                                                            (pushdown de join)
   └─ toTrinoType                                                          (mapeia tipos Db2→Trino)

Db2TableHandle.java       estado imutável da consulta (tabela + acúmulos acima); serializável (@JsonProperty)
Db2ColumnHandle.java      nome + tipo de uma coluna
Db2SplitManager.java      getSplits(...) → divide o trabalho (hoje: 1 split via FixedSplitSource)
Db2Split.java             uma fatia de trabalho (carrega o handle)
Db2RecordSetProvider.java entrega o RecordSet para um split
Db2RecordSet.java         cria o cursor
Db2RecordCursor.java      buildSql() + leitura linha-a-linha (getLong/getSlice/…), tipo a tipo
Db2TransactionHandle.java handle de transação (stub)

src/main/resources/META-INF/services/io.trino.spi.Plugin  → aponta para Db2Plugin (ServiceLoader)
```

---

## Configuração (o catálogo)

Arquivo `etc/catalog/<nome>.properties` na instalação Trino. **O nome do arquivo vira o nome do
catálogo no SQL** (ex.: `db2-dbhmlg.properties` → `"db2-dbhmlg".schema.tabela`).

| Chave | Obrigatória | Exemplo / default |
|---|---|---|
| `connector.name` | sim | `db2` (tem que bater com `Db2ConnectorFactory.getName()`) |
| `connection-url` | sim | `jdbc:db2://host:50000/NOMEDOBANCO` |
| `connection-user` | sim | usuário do Db2 |
| `connection-password` | sim | senha do Db2 |
| `db2.max-pool-size` | não | tamanho do pool Hikari (default `10`) |

---

## Build & Deploy

**Requisito:** o Trino 482 roda em **Java ≥ 24**, e o `trino-spi:482` só compila com **JDK ≥ 24**
(os `.class` dele são versão 68). Use um JDK 24/25.

```bash
cd demo
mvn clean package                 # gera em target/demo-<versão>.jar

# deploy numa instalação Trino:
rm  <trino>/plugin/db2/*.jar
cp  target/demo-*.jar  <trino>/plugin/db2/
```

- **Não** builde pelo Eclipse: ele gera `.jar` mesmo com erro de compilação (vira
  `java.lang.Error: Unresolved compilation problems` em runtime). Use `mvn package`.
---