const fs = require('fs');
const { WebClient } = require('@slack/web-api');
const { download, sleep, extractZip, readCsv, objectsToCsv } = require('./util');
const token = '<slack-token>';
const conversationId = '<channel-id>';

const web = new WebClient(token);

const readMessages = () => {
  return web.conversations.history({
    channel: conversationId,
  })
    .then((res) => res.messages)
    .catch(() => []);
}

const hasReaction = (message, reaction) => {
  return message.reactions && message.reactions.find((r) => r.name === reaction);
}

const hasFile = (message) => {
  return message.files && message.files.length > 0;
}

const sendAwnser = async (fileName, tsStartMessage) => {
  await web.files.upload({
    channels: conversationId,
    file: fs.createReadStream(`./files/unzipped/${fileName}`),
    thread_ts: tsStartMessage,
  });

  await web.reactions.remove({
    channel: conversationId,
    name: 'repeat',
    timestamp: tsStartMessage,
  });

  await web.reactions.add({
    channel: conversationId,
    name: 'white_check_mark',
    timestamp: tsStartMessage,
  });
}

const startFileProcessing = async (file, message, transformer = undefined) => {
  console.log('Starting file processing');
  await download(token, file.url_private, file.name);

  if (file.name.indexOf('zip') !== -1) {
    await extractZip(file.name);
  } else {
    fs.copyFileSync(`./files/${file.name}`, `./files/unzipped/${file.name}`);
  }

  fs.unlinkSync(`./files/${file.name}`);
  await web.reactions.add({
    channel: conversationId,
    name: 'repeat',
    timestamp: message.ts,
  });

  if (transformer === undefined) {
    transformer = (data) => data;
  }

  const fileName = file.name.indexOf('csv') !== -1 ?
    file.name.replace('.zip', '') :
    file.name.replace('.zip', '.csv');

  const data = readCsv(fileName);
  const result = transformer(data);
  const csv = objectsToCsv(result);

  fs.writeFileSync(`./files/unzipped/${fileName}`, csv);

  await sendAwnser(fileName, message.ts);
  console.log('End file processing');
}

const isBotMessage = (message) => {
  return message.bot_id !== undefined;
}

(async () => {
  await web.auth.test();

  let qtyMessages = 0;
  while (true) {
    const messages = await readMessages();
    if (messages.length > qtyMessages) {
      const lastMessage = messages[0];

      if (
        hasFile(lastMessage) &&
        !hasReaction(lastMessage, 'repeat') &&
        !isBotMessage(lastMessage)
      ) {
        const file = lastMessage.files[0];
        startFileProcessing(file, lastMessage, (data) => {
          data.push({
            nome: "João",
            email: "tts@gmail.com",
            cpf: "12345678910",
          });

          return data;
        });
      }
    }

    qtyMessages = messages.length;
    await sleep(2000);
    console.log('Checking new messages...');
  }

})();