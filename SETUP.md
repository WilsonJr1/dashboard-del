# Guia Completo de Configuração do Projeto

Este guia detalha todos os passos necessários para configurar e executar o projeto Dashboard de Métricas Delbank.

## Pre-requisitos

### Software Necessário:
- Python 3.8+
- PostgreSQL 12+
- Git
- Pip (gerenciador de pacotes Python)

### Credenciais de Banco:
- Host do PostgreSQL
- Porta (padrão: 5432)
- Nome do banco de dados
- Usuário
- Senha

## Estrutura do Projeto

```
Dash/
├── .streamlit/                 # Configurações do Streamlit
│   ├── config.toml            # Configuração do servidor
│   └── secrets.toml           # CREDENCIAIS (não commit)
├── app.py                     # Aplicação principal
├── db.py                      # Conexão com banco de dados
├── requirements.txt            # Dependências do Python
├── secrets.toml.example       # Exemplo de configuração
├── README.md                   # Documentação geral
└── SETUP.md                   # Este guia
```

## Passo a Passo de Configuração

### 1. Clone o Repositório
```bash
git clone <url-do-repositorio>
cd Dash
```

### 2. Instale as Dependências
```bash
pip install -r requirements.txt
```

### 3. Configure as Credenciais do Banco

#### Opção A: Streamlit Secrets (RECOMENDADO)
```bash
# Crie a pasta de configurações
mkdir -p .streamlit

# Copie o arquivo de exemplo
cp secrets.toml.example .streamlit/secrets.toml

# Edite o arquivo com suas credenciais
nano .streamlit/secrets.toml
```

Conteúdo do arquivo .streamlit/secrets.toml:
```toml
[db]
host = "seu-host-postgres"
port = 5432
dbname = "nome-do-banco"
user = "seu-usuario"
password = "sua-senha"
```

#### Opção B: Variáveis de Ambiente
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

### 4. Configure o Servidor (Opcional)

Arquivo: .streamlit/config.toml
```toml
[server]
address = "0.0.0.0"    # Aceita conexões de qualquer IP
port = 8501            # Porta do servidor
enableCORS = false
enableXsrfProtection = false
```

### 5. Execute a Aplicação
```bash
streamlit run app.py
```

Acesse: http://localhost:8501

## Estrutura do Banco de Dados

### Tabelas Necessárias:
```sql
-- Tabela de ciclos/sprints
CREATE TABLE public.cycles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    start_date DATE,
    end_date DATE
);

-- Tabela de issues/tickets
CREATE TABLE public.issues (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500),
    points INTEGER,
    category VARCHAR(50)
);

-- Relação issues-ciclos
CREATE TABLE public.cycle_issues (
    cycle_id INTEGER REFERENCES public.cycles(id),
    issue_id INTEGER REFERENCES public.issues(id)
);

-- Assignees das issues
CREATE TABLE public.issue_assignees (
    issue_id INTEGER REFERENCES public.issues(id),
    assignee_id INTEGER
);

-- Tabela de usuários/desenvolvedores
CREATE TABLE public.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255)
);
```

## Segurança e Boas Práticas

### Arquivos Protegidos (.gitignore):
- .streamlit/secrets.toml - Credenciais do banco
- db_local.py - Configurações locais
- __pycache__/ - Cache do Python
- *.pyc - Arquivos compilados

### NUNCA Commitar:
- Credenciais de banco de dados
- Chaves de API
- Tokens de acesso
- Informações sensíveis

## Deploy em Rede Interna

### Para acesso na rede interna:
```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

### Configurações de Rede:
- Porta: 8501 (pode ser alterada)
- IP: 0.0.0.0 (aceita conexões de qualquer IP)
- Acesso: http://<ip-do-servidor>:8501

## Solução de Problemas

### Erro: "Missing DB configuration"
Causa: Credenciais não configuradas
Solução: Siga o passo 3 de configuração

### Erro: "Connection refused"
Causa: Banco não está acessível
Solução: Verifique host, porta e credenciais

### Erro: "Table does not exist"
Causa: Estrutura do banco incompleta
Solução: Execute os scripts SQL acima

### Erro: "Module not found"
Causa: Dependências não instaladas
Solução: Execute pip install -r requirements.txt

## Funcionalidades do Dashboard

### Métricas Disponíveis:
- Por Sprint: Previsto vs Realizado
- Por Desenvolvedor: Performance individual
- Por Categoria: Feature vs Bug vs Não planejada
- Temporal: Evolução ao longo do tempo
- Lead Time/Cycle Time: Métricas de tempo

### Filtros e Visualizações:
- Filtro por período
- Filtro por desenvolvedor
- Filtro por categoria
- Gráficos interativos
- Tabelas detalhadas

## Fluxo de Desenvolvimento

### 1. Desenvolvimento Local:
```bash
# Configure ambiente local
cp secrets.toml.example .streamlit/secrets.toml
# Edite com credenciais de desenvolvimento
```

### 2. Testes:
```bash
# Execute a aplicação
streamlit run app.py
# Teste todas as funcionalidades
```

### 3. Deploy:
```bash
# Use credenciais de produção
# Verifique configurações de rede
# Execute em modo produção
```

## Suporte

### Em caso de problemas:
1. Verifique as credenciais do banco
2. Confirme que as tabelas existem
3. Consulte os logs do Streamlit
4. Verifique as dependências

### Logs do Streamlit:
```bash
# Logs detalhados
streamlit run app.py --logger.level=debug
```

---

## Checklist de Configuração

- [ ] Python 3.8+ instalado
- [ ] PostgreSQL instalado e rodando
- [ ] Banco de dados criado
- [ ] Tabelas criadas (estrutura acima)
- [ ] Dependências instaladas (requirements.txt)
- [ ] Arquivo secrets.toml configurado
- [ ] Servidor configurado (opcional)
- [ ] Aplicação executando (streamlit run app.py)
- [ ] Acesso via http://localhost:8501
- [ ] Todas funcionalidades testadas

Ultima atualização: 2025-11-13