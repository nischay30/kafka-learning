const fs = require('fs');
const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const client = new kafka.Client();
const consumer = new Consumer(client, [{topic: 'messaging'}, {topic: 'xmlTOjson'}, {topic: 'jsonTOxml'}]);

consumer.on('message', (message) => {
	console.log(message);
   // console.log('Message:', message.value);
   console.log('Topic:',message.topic);
   console.log('Partition:', message.partition);
   writefile(message.topic, message.value);
});

function writefile(topic, message) {
	fs.writeFile('./' +topic, message, (err) => {
		if(err) { console.log('Err:', err); return; }
	});
}
