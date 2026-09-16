# Dataset de clientes
Criado por: Prof. Marcelo Barbosa
Github: https://github.com/infobarbosa
Linkedin: https://www.linkedin.com/in/infobarbosa/

Uma base de clientes de um e-commerce fictício.

# Leiaute dos arquivos

- Formato: JSON
- Compressão: GZIP

### `clientes.json.gz`
| Atributo        | Tipo      | Obs                                               |
| ---             | ---       | ---                                               |
| ID              | long      | O identificador da pessoa                         |
| NOME            | string    | O nome da pessoa                                  |
| DATA_NASC       | date      | A data de nascimento da pessoa                    |
| CPF             | string    | O CPF da pessoa                                   |
| EMAIL           | string    | O email da pessoa                                 |
| INTERESSES      | object    | Um array com a lista de interesses da pessoa      |

### Sample
```json
{ "id": 1, "nome": "Isabelly Barbosa", "data_nasc": "1963-08-15", "cpf": "137.064.289-03", "email": "isabelly.barbosa@gmail.com", "interesses": ["Política", "Economia"] }
{ "id": 2, "nome": "Larissa Fogaça", "data_nasc": "1933-09-29", "cpf": "703.685.294-10", "email": "larissa.fogaca@example.com", "interesses": ["Música", "Viagens", "Gastronomia"] }
{ "id": 3, "nome": "João Gabriel Silveira", "data_nasc": "1958-05-27", "cpf": "520.179.643-52", "email": "joao.gabriel.silveira@example.com", "interesses": ["Ciências", "Inteligência Artificial"] }

```
