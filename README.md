ifelse(
    {State} = 'SP', 'Sao Paulo',
    {State} = 'RJ', 'Rio de Janeiro',
    {State} = 'MG', 'Minas Gerais',
    {State} = 'ES', 'Espirito Santo',
    {State} = 'RS', 'Rio Grande do Sul',
    {State} = 'PR', 'Parana',
    {State} = 'SC', 'Santa Catarina',
    {State} = 'MS', 'Mato Grosso do Sul',
    {State} = 'MT', 'Mato Grosso',
    {State} = 'GO', 'Goias',
    {State} = 'DF', 'Distrito Federal',
    {State} = 'AC', 'Acre',
    {State} = 'AL', 'Alagoas',
    {State} = 'AP', 'Amapa',
    {State} = 'AM', 'Amazonas',
    {State} = 'BA', 'Bahia',
    {State} = 'CE', 'Ceara',
    {State} = 'MA', 'Maranhao',
    {State} = 'PA', 'Para',
    {State} = 'PB', 'Paraíba',
    {State} = 'PE', 'Pernambuco',
    {State} = 'PI', 'Piaui',
    {State} = 'RN', 'Rio Grande do Norte',
    {State} = 'RO', 'Rondonia',
    {State} = 'RR', 'Roraima',
    {State} = 'SE', 'Sergipe',
    {State} = 'TO', 'Tocantins',
    'Vazio/Desconhecido' -- Default case for any state code not matched above
)
