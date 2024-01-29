# Datum

Olá Datum. Tudo bem com vocês? Por aqui, tudo bem! :blush:

Neste repositório vocês encontrarão informações a respeito do meu teste técnico para a vaga de Desenvolvedor Python (job description compatível com Engenheiro de Dados).

## 1. DataSet

Para realizar o teste técnico, escolhi o [Brazilian E-Commerce Public Dataset by Olist]([https://www.exemplo.com](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). O diagrama de entidade e relacionamento foi fornecido:

![Descrição da imagem](auxiliares/er_kaggle.png)


## 2. Camada Bronze

[Arquivos da camada bronze](./bronze)

Nesta camada, um arquivo .ipynb que:
1. Faz a autenticação na api do kaggle, com minhas informações de usuário.
2. Faz o check da watermark que registra a data de ultima atualização do dataset no kaggle.
3. Faz o check dos arquivos na camada bronze no dbfs.

Basicamente, 
* se o **dataset não consta atualização** e já **estamos com os arquivos .csv no dbfs (camada bronze)**, o job falha e as camadas silver e gold não são processada.

* já se o **dataset sofreu atualização no kaggle** ou se "existem arquivos faltantes na camada bronze do dbfs**, o job é executado por completo.

Uma notificação é enviada por email caso o job rode por completo. O normal, aqui, é não executar essa pipeline completa, já que o dataset sofre poucas atualizações. Trigger, notificação e as tasks do workflow:

![Schedule e Job Notiication](auxiliares/databricks_schedule_jobnotifications.png)

![Workflow tasks](auxiliares/databricks_workflow_tasks.png)

## 3. Camada Silver
[Arquivos da camada silver](./silver)

Na camada silver eu crio dataframes a partir dos dados .csv da camada bronze. 

Então, defino schema, faço transformações básicas (como a que vocês pediram -> adição de colunas calculadas) e exporto os dados em delta para a camada silver. Todos os notebooks da camada silver seguem esta lógica.

## 4. Camada Gold
[Arquivos da camada gold](./gold)

Antes de implementar a camada gold, defini 5 informações que eu gostaria de obter do dataset. São elas:

1. Uma lista dos produtos comprados aos pares e a quantidade de vezes que isto acontece.
2. Total de vendas (R$) para cada forma de pagamento.
3. Total de vendas (R$) por Estado.
4. Total de vendas (R$) por mês e por dia.
5. Um estudo sobre a diferença na data de entrega real e a data de entrega prevista pelo algoritmo da Olist.

`Observação: Tantas outras informações importantes para o negócio poderiam ser obtidas deste dataset. ` 

Então, com os objetivos definidos, os notebooks da camada gold seguem a lógica:

importamos os dados da camada silver -> fazemos os tratamentos necessários (valores nulos ou vazios, adicioarmos colunas calculadas, agregações, somas, counts, joins e etc) -> exportamos os dados pertinentes no formato delta para a camada gold -> criamos uma DELTA TABLE com os dados pertinentes

Um objetivo aqui na camada gold é o de entregar o dado mais pronto possível para análise, para que não tenhamos tratamentos de dados em ferramentas de BI ou mesmo aqui no databricks dentro das sql queries do sql warehouse.

Por fim, vocês encontrarão nos .ipynb da camada gold, um pouco mais de tratamento de dados e manipulação dos dados. Porém, ainda são tratamentos de um dataset do kaggle. "A realidade lá fora é bastante mais dura do que no kaggle." :grin:































