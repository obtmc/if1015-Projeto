const amqp = require('amqplib/callback_api');
const csv = require('csv-parser');
const fs = require('fs');

const csvFilePath = 'dataset_tratado.csv'; // caminho para o arquivo CSV
const rabbitMQUrl = 'amqp://brckerRabbitMQ'; // URL do servidor RabbitMQ
const exchangeName = 'dados'; // nome da exchange RabbitMQ
const routingKey = 'dados'; // routing key para as mensagens

const delay = 10000; // tempo de espera entre envio de mensagens (em milissegundos)
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
        row[solicitacao_data_hora] = new Date().toISOString();
        const delayM = row['delay'];

        row = row.drop(['delay'], axis=1)

        // Converte a linha em uma mensagem JSON
        const message = JSON.stringify(row);
        
        // Envia a mensagem para o RabbitMQ
        ch.publish(exchangeName, routingKey, Buffer.from(message));
        
        // Aguarda o tempo de delay//esse tempo deve ser multiplicado pelo valor de data e hora normalizado
        setTimeout(() => {}, delay*delayM);
      })
      .on('end', () => {
        console.log('Envio concluído');
      });
  })
  .catch((err) => {
    console.error(err);
  });