const amqp = require('amqplib/callback_api');
const csv = require('csv-parser');
const fs = require('fs');

const csvFilePath = 'dataset_tratado.csv'; // caminho para o arquivo CSV
const rabbitMQUrl = 'amqp://localhost'; // brockerRabbitMQ - URL do servidor RabbitMQ
const exchangeName = 'dados'; // nome da exchange RabbitMQ
const routingKey = 'dados'; // routing key para as mensagens
const delay = 10; // tempo de espera entre envio de mensagens (em milissegundos)
let delayM =1;

// Conexão com o RabbitMQ
amqp.connect(rabbitMQUrl, function(error0, connection) {
  if (error0) {
    throw error0;
  }

  // Criação do canal de comunicação
  connection.createChannel(function(error1, channel) {
    if (error1) {
      throw error1;
    }

    // Função que envia a mensagem para o broker
    function enviarMensagem(mensagem) {
      
      // Converte a mensagem em uma string JSON
      const message = JSON.stringify(mensagem);

      // Envia a mensagem para o RabbitMQ
      channel.publish(exchangeName, routingKey, Buffer.from(message));
      console.log("Mensagem enviada:", message);

    }
    // Lê o arquivo CSV linha por linha
    fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on('data', (row) => {

        // Modifica a informação de data e hora para o momento de envio
        row['solicitacao_data_hora'] = new Date().toISOString();
        delayM = delay*row['delay'];
        delete row['delay'];

        // Converte a linha em uma mensagem JSON
        const solicitacao = JSON.stringify(row);      

        // Envia a mensagem para o broker
        setTimeout(() => enviarMensagem(solicitacao), delayM);// Aguarda o tempo de delay antes de enviar a próxima mensagem  
    })

    .on('end', () => {
      console.log('Envio concluído');
      // Fecha a conexão com o RabbitMQ após o envio de todas as mensagens
      setTimeout(() => {
        connection.close();
      }, 3000000);
    });
  });
});
