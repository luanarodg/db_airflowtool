# Airflow Tooltorial
Desafio de orquestração de dados utilizando Airflow.


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

Este programa foi orquestrado com Airflow, para ativá-lo é preciso primeiro criar um ambiente virtual para isolar a aplicação e ativar esse ambiente para assim seguir os passos de instalação.

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

Após rodar o código irá surgir uma nova pasta no diretório chamada `airflow-data` contendo arquivos sobre a configuração do ambiente.

### Ajustes
Num primeiro momento, o Airflow vem com algumas dags de exemplos dentro do ambiente que não são necessárias para esse desafio. Assim é possível alterar o código para que o ambiente considere apenas as dags criadas. Para isso

- Vá até a página `airflow-data` no arquivo airflow.cfg e procure a variável `load_examples` que vai está True e substitua para False.
```
load_examples = True  >> load_examples = False
```

- No mesmo arquivo airflow.cfg, certifique que o caminho da variável `dags_folder` é o mesmo de onde se encontra o arquivo do código da dag. Por exemplo:
```
dags_folder = /.../airflow_tooltorial/dags
```

- Após salvar a alteração, vá ao terminal e digite.
```
airflow db reset
```
- Vai pedir uma confirmação do usuário e após terminar o carregamento, é só digitar novamente.
```
airflow standalone
```

## :rocket:	 Resultado
Após isso, é só abrir o Airflow no navegador novamente que estará um ambiente mais limpo contendo apenas as dags necessárias.

Dessa forma o Airflow executará a DAG de forma que a lógica do programa aconteça e traga o resultado esperado, de utilizar o banco de dados para trazer as informações pedidas. Resultando no Graph abaixo que nos diz que tudo ocorreu de forma esperada. 

<img src= "https://github.com/lurodig/db_airflowtool/blob/main/dag_image.png?raw=true">
