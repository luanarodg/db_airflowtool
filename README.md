# Airflow Tooltorial
Desafio de orquestração de dados utilizando Airflow.


## :package:	Arquivos
Este programa contém os seguintes arquivos:

```
README.md
requirements.txt
.gitignore
install.sh
data/
├──── Northwind_small.sqlite
├──── count.txt
├──── final_output.txt
├──── output_orders.csv
airflow-data/
dags/
├──── dag_db.py
```

## :books:	 Dicionário

### O que é Airflow?
- Airflow é uma ferramenta que orquestra pipeline utilizando tarefas agendadas.

### DAGs
- É uma coleção de tarefas organizadas que resultam em objetivo, são executadas por tempo e ordem de qual tarefa é realizada primeiro.

### Tasks
- Task é a tarefa, é uma parte da dag onde se encontra a implementação de uma lógica.

## :white_check_mark:	 Desafio
Utilizar o banco de dados Northwind em formato Sqlite3 para extrair a tabela 'Order' e gerar um novo arquivo output_orders.csv;

Fazer o join desse csv gerado com a tabela 'OrderDetail do mesmo banco;

Calcular a soma da quantidade vendida (Quantity) com destino (ShipCity) para o Rio de Janeiro;

Exportar a quantidade para um arquivo "count.txt" que contenha somente esse valor em formato texto.


## :computer:	 Ambiente

Para esse desafio é recomendado o ambiente Linux, e em caso de Windows, o uso do WSL2.

### Airflow

Esse programa foi orquestrado em Airflow, para ativá-lo é preciso primeiro criar um ambiente virtual para isolar a aplicação e ativar esse ambiente para assim seguir os passos de instalação.

```
virtualenv venv -p python3
source venv/bin/activate
```

- Agora dentro do ambiente virtual, é preciso instalar os pacotes usados que estão dentro do arquivo `requirements.txt`.
```
pip install -r requirements.txt
```

- Assim, instalar o Airflow.
```
bash install.sh
```

- Se tudo ocorrer bem é só ir até seu navegador para acessar a porta 8080 onde se encontra o ambiente digitando o seguinte endereço para ver a orquestração acontecendo.
```
localhost:8080
```



