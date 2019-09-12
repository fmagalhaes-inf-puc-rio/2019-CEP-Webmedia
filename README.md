# Resumo
A cada dia é mais importante extrair informação rapidamente de fluxo de dados que são gerados constantemente e em grande quantidade. Exemplos práticos são a processamento de dados de sensores na indústria, a detecção de engarrafamento de veículos, de situações de alerta em redes sociais, e a análise do mercado de ações.
Para tais tarefas existem várias tecnologias focadas na análise de fluxos de dados, as quais vão desde bancos de dados ativos até o processamento de eventos complexos.
O processamento de eventos complexos, também conhecido como CEP (Complex Event Processing), consiste em um modelo de programação que fornece primitivas para processar e derivar eventos (informações) mais complexas a partir de fluxos de dados.

CEP tem capacidade de examinar *dados provenientes de múltiplas fontes*.
Esses dados são encapsulados em *eventos simples*, também chamados de eventos atômicos ou eventos brutos.
Os fluxos de eventos são examinados por um *processador de eventos*, o qual utiliza regras CEP previamente especificadas.
Regras CEP são primitivas que permite especificar como os eventos devem ser processados.
Cada processador utiliza uma linguagem, ou EPL (Event Processing Language) para definir as regras.
As regras são o método disponibilizado pelas ferramentas CEP para especificar como deve ser feita a análise dos eventos.
Regras CEP são semelhantes a consultas a um banco de dados, porém enquanto no banco as consultas são feita a informações já armazenadas, uma regra CEP é uma *consulta contínua*, ou seja, todo novo evento recebido é testado contra as regras que já haviam sido instanciadas antes da chegada do evento.

Em uma indústria, por exemplo, o evento de incêndio pode ser gerado a partir de eventos de aumento de temperatura e detecção de Fumaça.
Em Smart Cities, por exemplo, o evento de engarrafamento pode ser detectado a partir de eventos mais simples que indicam que vários  veículos não estão se movendo em um determinado período de tempo.

Neste contexto, este capítulo tem o objetivo apresentar o modelo de programação de processamento de eventos complexos (CEP) como meio de lidar com as especificidades de fluxos de dados.
Portanto, o restantes deste trabalho esta organizado como segue.

* A Seção 1 apresenta conceitos e problemas fundamentais de processamento de fluxo de dados e o modelo CEP.
* A Seção 2 apresenta a tecnologia Esper e a sua linguagem de regras EPL como meio de tratar os fluxo de dados.
* A Seção 3 exemplifica um caso de uso da aplicação de CEP para tratamento de dados de aplicações IoT;
* A Seção 4 apresenta um outro caso de uso de aplicação de CEP como meio de processar dados de aplicações para \textit{Smart Cities.
* A Seção 5 apresenta nossas considerações finais.


## Material
* Capitulo: TODO
* Slides. TODO

## Autores
* Marcos Roriz Junior
* Fernando B. V. Magalhães
* Álan L. Vasconcelos
* Sérgio Colcher
* Markus Endler

