const amqp = require('amqplib/callback_api');
const csv = require('csv-parser');
const fs = require('fs');
const amqp = require('amqplib');

const csvFilePath = 'dados.csv'; // caminho para o arquivo CSV
const rabbitMQUrl = 'amqp://localhost'; // URL do servidor RabbitMQ
const exchangeName = 'dados'; // nome da exchange RabbitMQ
const routingKey = 'dados'; // routing key para as mensagens

const delay = 1000; // tempo de espera entre envio de mensagens (em milissegundos)
const timestampIndex = 0; // índice da coluna com informações de data e hora

// Conecta ao servidor RabbitMQ
amqp.connect(rabbitMQUrl)
  .then((conn) => {
    return conn.createChannel();
  })
  .then((ch) => {
    // Declara a exchange
    ch.assertExchange(exchangeName, 'direct', { durable: false });
    
    // Lê o arquivo CSV linha por linha
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        // Modifica a informação de data e hora para o momento de envio
        row[timestampIndex] = new Date().toISOString();
        
        // Converte a linha em uma mensagem JSON
        const message = JSON.stringify(row);
        
        // Envia a mensagem para o RabbitMQ
        ch.publish(exchangeName, routingKey, Buffer.from(message));
        
        // Aguarda o tempo de delay//esse tempo deve ser multiplicado pelo valor de data e hora normalizado
        setTimeout(() => {}, delay);
      })
      .on('end', () => {
        console.log('Envio concluído');
      });
  })
  .catch((err) => {
    console.error(err);
  });




// Conexão com o RabbitMQ
amqp.connect('amqp://localhost', function(error0, connection) {
  if (error0) {
    throw error0;
  }

  // Criação do canal de comunicação
  connection.createChannel(function(error1, channel) {
    if (error1) {
      throw error1;
    }

    // Função que envia a solicitação para o broker
    function enviarSolicitacao(solicitacao) {
      channel.assertExchange('solicitacoes', 'fanout', {
        durable: false
      });
      channel.publish('solicitacoes', '', Buffer.from(JSON.stringify(solicitacao)));
      console.log("Solicitação enviada:", solicitacao);
    }

    // Cliente envia a solicitação para o broker
    const solicitacao = { tipo: "solicitar_pedido", dados: { cliente: "João", produto: "Camisa", quantidade: 2 } };
    enviarSolicitacao(solicitacao);

    console.log("Aguardando resposta...");
  });
});
