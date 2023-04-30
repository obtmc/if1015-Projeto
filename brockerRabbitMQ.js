const amqp = require('amqplib/callback_api');
const exchange = 'minhaExchange';
const routingKey = 'minhaChaveDeRoteamento';
const queue = 'minhaFila';


// Conexão com o RabbitMQ
amqp.connect('amqp://localhost', function(errorConnect, connection) {
  if (errorConnect) {
    throw errorConnect;
  }

  // Criação do canal de comunicação
  connection.createChannel(function(errorCreateChannel, channel) {
    if (errorCreateChannel) {
      throw errorCreateChannel;
    }

    // Criação do exchange de solicitações
    channel.assertExchange(exchange, 'direct', {
      durable: true
    });

    // Criação da fila para receber as solicitações
    channel.assertQueue(queue, {
      exclusive: true
    }, function(errorAssetQueue, q) {
      if (errorAssetQueue) {
        throw errorAssetQueue;
      }

      console.log("Aguardando solicitações...");

      // Faz o binding da fila com o exchange
      channel.bindQueue(q.queue, exchange, routingKey);

      // Callback que recebe as solicitações do exchange
      channel.consume(exchange, function(msg) {
        //let solicitacao = JSON.parse(msg.content.toString());
        processarSolicitacao(msg);
      }, { noAck: true });


      // Encaminha a solicitação para o gerenciador de solicitações
      function processarPedido(solicitacao) {

        const gerenciador = 'gerenciador_de_solicitacoes';

        console.log(`Processando pedido ${solicitacao.id}...`);

        channel.assertQueue(gerenciador, {
          durable: true
        });
      }

    });

  });
});
