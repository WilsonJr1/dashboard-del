# Dashboard de Métricas - Delbank

Dashboard interativo para análise de métricas de desenvolvimento, built with Streamlit.

## Funcionalidades

- Visualização de métricas por sprint
- Analise de performance por desenvolvedor
- Comparativo Previsto vs Realizado
- Analise por categorias (Feature, Bug, etc.)
- Metricas de tempo (Lead Time, Cycle Time)

## Como Executar

### Pre-requisitos
- Python 3.8+
- PostgreSQL Database

### Instalação

1. Clone o repositorio:
```bash
git clone <url-do-repositorio>
cd Dash
```

2. Instale as dependencias:
```bash
pip install -r requirements.txt
```

3. Configure via Streamlit Secrets (Recomendado):
```bash
# Crie a pasta de secrets
mkdir -p .streamlit

# Copie o arquivo de exemplo
cp secrets.toml.example .streamlit/secrets.toml
```

Edite `.streamlit/secrets.toml`:
```toml
[db]
host = "seu-host-postgres"
port = 5432
dbname = "nome-do-banco"
user = "seu-usuario"
password = "sua-senha"
```

4. Ou configure via variaveis de ambiente do sistema operacional:
```bash
# Linux/Mac
export DB_HOST=seu-host-postgres
export DB_PORT=5432
export DB_NAME=nome-do-banco
export DB_USER=seu-usuario
export DB_PASSWORD=sua-senha

# Windows
set DB_HOST=seu-host-postgres
set DB_PORT=5432
set DB_NAME=nome-do-banco
set DB_USER=seu-usuario
set DB_PASSWORD=sua-senha
```

5. Execute a aplicacao:
```bash
streamlit run app.py
```

## Configuracao do Banco de Dados

### Prioridade de Configuracao:
1. Streamlit Secrets (`.streamlit/secrets.toml`) - Recomendado
2. Variaveis de Ambiente do Sistema - Alternativo
3. db_local.py - Apenas desenvolvimento local (NAO commit)

### Estrutura do Banco Esperada:
O dashboard espera as seguintes tabelas no PostgreSQL:
- `public.cycles` - Informações das sprints
- `public.issues` - Issues/tickets
- `public.cycle_issues` - Relação issues-sprints
- `public.issue_assignees` - Assignees das issues
- `public.users` - Usuários/desenvolvedores

## Deploy

### Rede Interna:
```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

Acesse: `http://<seu-ip>:8501`

### Configurações de Rede:
- Porta padrão: 8501
- Aceita conexões de todas as interfaces (0.0.0.0)

## Segurança

- Credenciais de banco NAO são commitadas
- Arquivos sensíveis estão no `.gitignore`
- Configure firewall para restringir acesso

## Metricas Disponíveis

### Por Sprint:
- Total Previsto vs Realizado
- Distribuição por categoria
- Linha temporal de entregas

### Por Desenvolvedor:
- Issues realizadas
- Pontos entregues
- Lead Time médio
- Cycle Time médio

## Desenvolvimento

### Estrutura do Projeto:
```
Dash/
├── app.py              # Aplicação principal
├── db.py               # Conexão com banco
├── requirements.txt    # Dependências
├── .streamlit/
│   └── config.toml     # Configuração Streamlit
├── secrets.toml.example # Exemplo configuração
└── README.md           # Esta documentação
```

### Para Contribuir:
1. Siga o padrão de nomenclatura "Previsto/Realizado"
2. Mantenha o `.gitignore` atualizado
3. Teste as conexões com banco localmente

## Suporte

Para dúvidas ou problemas:
1. Verifique as configurações de banco
2. Confirme que as tabelas existem
3. Consulte os logs do Streamlit

---

Desenvolvido para Delbank