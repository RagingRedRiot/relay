const socket = new WebSocket('ws://localhost:3000/ws');

socket.onopen = () => {
    console.log("Connected")
}

socket.onmessage = (event) => {
    data = JSON.parse(event.data);
    console.log('Received action ', data.action)
    console.log('Received content ', data.content)

    socket.send(JSON.stringify(data))
}

socket.onclose = () => {
    console.log("Disconnected")
}