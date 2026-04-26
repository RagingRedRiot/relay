const username = "user";
const password = "pass";

const socket = new WebSocket('ws://localhost:3000/ws');

socket.onopen = () => {
    socket.send(JSON.stringify({
        Auth: {
            username: "user",
            password: "pass"
        }
    }));
}

socket.onmessage = (event) => {
    data = JSON.parse(event.data);
    if (data == "AuthOk") {
        console.log("Connected")
    } else {
        console.log(data)
    }
}

socket.onclose = () => {
    console.log("Disconnected")
}